# Section 11: Multi-Process Scaling

## 11.1 Overview

The broker scales horizontally via multiple processes sharing a listen socket with `SO_REUSEPORT`. Each process is fully independent with its own io_uring instance, buffer pool, and subscription state.

## 11.2 Architecture

```
                    ┌─────────────────────────────────────┐
                    │           KERNEL                     │
                    │                                     │
                    │  ┌─────────────────────────────┐   │
                    │  │    Listen Socket (:1883)    │   │
                    │  │       SO_REUSEPORT          │   │
                    │  └──────────┬──────────────────┘   │
                    │             │                       │
                    │    Connection distribution          │
                    │    (kernel load balancing)          │
                    │             │                       │
                    │      ┌──────┼──────┐               │
                    │      │      │      │               │
                    └──────┼──────┼──────┼───────────────┘
                           │      │      │
              ┌────────────┘      │      └────────────────┐
              │                   │                       │
              ▼                   ▼                       ▼
    ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
    │   Process 0     │ │   Process 1     │ │   Process N     │
    │   CPU 0         │ │   CPU 1         │ │   CPU N         │
    │                 │ │                 │ │                 │
    │ ┌─────────────┐ │ │ ┌─────────────┐ │ │ ┌─────────────┐ │
    │ │  io_uring   │ │ │ │  io_uring   │ │ │ │  io_uring   │ │
    │ └─────────────┘ │ │ └─────────────┘ │ │ └─────────────┘ │
    │ ┌─────────────┐ │ │ ┌─────────────┐ │ │ ┌─────────────┐ │
    │ │ Buffer Pool │ │ │ │ Buffer Pool │ │ │ │ Buffer Pool │ │
    │ └─────────────┘ │ │ └─────────────┘ │ │ └─────────────┘ │
    │ ┌─────────────┐ │ │ ┌─────────────┐ │ │ ┌─────────────┐ │
    │ │ Topic Trie  │ │ │ │ Topic Trie  │ │ │ │ Topic Trie  │ │
    │ └─────────────┘ │ │ └─────────────┘ │ │ └─────────────┘ │
    └─────────────────┘ └─────────────────┘ └─────────────────┘
```

## 11.3 Process Spawning

### 11.3.1 Fork Model

```c
void spawn_workers(int num_workers) {
    // Create shared listen socket
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
    
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(1883),
        .sin_addr.s_addr = INADDR_ANY,
    };
    bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr));
    listen(listen_fd, SOMAXCONN);
    
    // Fork workers
    for (int i = 0; i < num_workers; i++) {
        pid_t pid = fork();
        
        if (pid == 0) {
            // Child process
            worker_init(i, listen_fd);
            worker_run();
            exit(0);
        }
        
        // Parent continues spawning
    }
    
    // Parent waits for children
    while (wait(NULL) > 0);
}
```

### 11.3.2 CPU Affinity

Each worker pins to a specific CPU:

```c
void worker_init(int worker_id, int listen_fd) {
    // Pin to CPU
    cpu_set_t cpus;
    CPU_ZERO(&cpus);
    CPU_SET(worker_id, &cpus);
    sched_setaffinity(0, sizeof(cpus), &cpus);
    
    // Initialize broker state (own memory, own io_uring)
    broker_init(&broker, listen_fd);
}
```

### 11.3.3 NUMA Awareness

For NUMA systems, workers SHOULD be distributed across NUMA nodes:

```c
void worker_init_numa(int worker_id, int listen_fd) {
    int num_nodes = numa_num_configured_nodes();
    int node = worker_id % num_nodes;
    
    // Bind to NUMA node
    struct bitmask *mask = numa_allocate_nodemask();
    numa_bitmask_setbit(mask, node);
    numa_bind(mask);
    numa_free_nodemask(mask);
    
    // Pin to CPU within that node
    int cpus_per_node = numa_num_configured_cpus() / num_nodes;
    int cpu = node * cpus_per_node + (worker_id / num_nodes);
    
    cpu_set_t cpus;
    CPU_ZERO(&cpus);
    CPU_SET(cpu, &cpus);
    sched_setaffinity(0, sizeof(cpus), &cpus);
    
    broker_init(&broker, listen_fd);
}
```

## 11.4 Subscription Isolation

### 11.4.1 Default: No Cross-Process Routing

By default, each process maintains its own subscription state. A client connected to Process 0 only receives messages published to Process 0.

```
Client A ──► Process 0 ──► PUBLISH "topic/x"
                              │
                              ▼
                         Process 0 subscribers only

Client B ──► Process 1 ──► SUBSCRIBE "topic/x"
                              │
                              ▼
                         Won't receive messages from Process 0
```

This is suitable for:
- Stateless workloads where any response is acceptable
- Load balancing scenarios with external routing
- High-throughput scenarios where cross-process overhead is unacceptable

### 11.4.2 Shared Subscriptions via IPC

For full MQTT semantics, processes can share subscription state:

```
┌─────────────────────────────────────────────────────────────────┐
│                    SHARED MEMORY REGION                          │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Global Topic Trie (read-mostly)             │   │
│  │                                                         │   │
│  │  Modifications synchronized via futex or atomic flags   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Process Subscription Bitmaps                │   │
│  │                                                         │   │
│  │  topic_node.process_bitmap[process_id] = fd_bitmap      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              │               │               │
              ▼               ▼               ▼
        Process 0       Process 1       Process N
```

### 11.4.3 Message Forwarding

When a publish matches subscribers in other processes:

```c
struct forward_msg {
    u32 topic_hash;
    u16 topic_len;
    u32 payload_len;
    u8  data[];  // topic + payload
};

void fanout_with_forwarding(struct broker *b, struct mqtt_publish *pub) {
    // Local subscribers
    u32 local_count = 0;
    trie_match_local(&b->trie, pub->topic, pub->topic_len, 
                     count_callback, &local_count);
    
    // Check for remote subscribers
    u64 remote_procs = trie_get_remote_processes(&b->shared_trie, 
                                                   pub->topic, pub->topic_len);
    
    if (remote_procs != 0) {
        // Forward to other processes via Unix socket or shared memory queue
        forward_to_processes(b, remote_procs, pub);
    }
    
    // Normal local fan-out
    if (local_count > 0) {
        fanout_publish_local(b, pub);
    }
}
```

## 11.5 Shared State Options

### 11.5.1 Option A: Shared Memory Trie

```c
struct shared_trie {
    // mmap'd MAP_SHARED
    struct topic_node nodes[MAX_TOPICS];
    
    // Per-process subscription state
    // Indexed as: process_subs[topic_idx][process_id]
    u64 process_subs[MAX_TOPICS][MAX_PROCESSES];
    
    // Synchronization
    _Atomic u64 version;             // Incremented on any modification
    pthread_rwlock_t lock;           // For modifications
};
```

### 11.5.2 Option B: Message Queues

Each process has an inbox:

```c
struct process_inbox {
    _Atomic u32 head;
    _Atomic u32 tail;
    struct forward_msg msgs[INBOX_SIZE];
};

// Shared between all processes
struct ipc_state {
    struct process_inbox inboxes[MAX_PROCESSES];
};
```

Publishers enqueue to target process inboxes:

```c
void forward_to_process(struct ipc_state *ipc, int target_proc, 
                        struct forward_msg *msg) {
    struct process_inbox *inbox = &ipc->inboxes[target_proc];
    
    u32 tail = atomic_load(&inbox->tail);
    u32 next = (tail + 1) % INBOX_SIZE;
    
    if (next == atomic_load(&inbox->head)) {
        // Inbox full, drop message
        return;
    }
    
    memcpy(&inbox->msgs[tail], msg, sizeof(*msg) + msg->topic_len + msg->payload_len);
    atomic_store(&inbox->tail, next);
}
```

### 11.5.3 Option C: Unix Domain Sockets

Each process pair has a socketpair:

```c
struct ipc_mesh {
    int sockets[MAX_PROCESSES][MAX_PROCESSES];
};

void forward_to_process(struct ipc_mesh *mesh, int from, int to,
                        struct forward_msg *msg) {
    int fd = mesh->sockets[from][to];
    write(fd, msg, sizeof(*msg) + msg->topic_len + msg->payload_len);
}
```

The receiving process monitors these sockets via io_uring.

## 11.6 Supervisor Process

An optional supervisor manages worker lifecycle:

```c
struct supervisor {
    pid_t workers[MAX_PROCESSES];
    int worker_count;
    int listen_fd;
};

void supervisor_run(struct supervisor *sup) {
    // Spawn initial workers
    for (int i = 0; i < sup->worker_count; i++) {
        sup->workers[i] = spawn_worker(i, sup->listen_fd);
    }
    
    // Monitor and restart
    while (1) {
        int status;
        pid_t pid = waitpid(-1, &status, 0);
        
        if (pid > 0) {
            // Worker died, find and restart
            for (int i = 0; i < sup->worker_count; i++) {
                if (sup->workers[i] == pid) {
                    log_warn("Worker %d (pid %d) died, restarting", i, pid);
                    sup->workers[i] = spawn_worker(i, sup->listen_fd);
                    break;
                }
            }
        }
    }
}
```

## 11.7 Configuration

### 11.7.1 Worker Count

```c
int determine_worker_count() {
    // Default: one per CPU
    int cpus = sysconf(_SC_NPROCESSORS_ONLN);
    
    // Cap at reasonable maximum
    if (cpus > MAX_PROCESSES) {
        cpus = MAX_PROCESSES;
    }
    
    return cpus;
}
```

### 11.7.2 Memory Sizing

Each worker gets independent memory:

```
Total memory = workers × (buffer_pool + conn_slots + trie + refs)
             = workers × ~330 MiB (default)
             = 8 workers × 330 MiB = 2.6 GiB
```

Plus shared memory for IPC if cross-process routing enabled.

## 11.8 Limitations

| Aspect | Single-Process | Multi-Process |
|--------|---------------|---------------|
| Throughput | 1M msg/sec | N × 1M msg/sec |
| Latency | Lowest | Slightly higher (IPC) |
| Session state | Complete | Per-process or shared |
| Complexity | Simple | More complex |
| Failure isolation | None | Per-process |
