# Section 3: System Architecture

## 3.1 Overview

The broker consists of a single-threaded event loop that processes io_uring completions and submits new I/O operations. All state is maintained in pre-allocated data structures indexed by connection slot or buffer index.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              BROKER PROCESS                              │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                         EVENT LOOP                               │   │
│  │                                                                  │   │
│  │   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐ │   │
│  │   │  Wait    │───►│  Drain   │───►│ Process  │───►│  Submit  │ │   │
│  │   │  CQEs    │    │   CQ     │    │  Events  │    │   SQEs   │ │   │
│  │   └──────────┘    └──────────┘    └──────────┘    └──────────┘ │   │
│  │        ▲                                               │        │   │
│  │        └───────────────────────────────────────────────┘        │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │
│  │   Buffer    │  │ Connection  │  │   Topic     │  │   Message   │   │
│  │    Pool     │  │   Slots     │  │    Trie     │  │    Refs     │   │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              KERNEL                                      │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                         io_uring                                 │   │
│  │   ┌──────────┐              ┌──────────┐                        │   │
│  │   │    SQ    │──────────────│    CQ    │                        │   │
│  │   │  (submit)│              │(complete)│                        │   │
│  │   └──────────┘              └──────────┘                        │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     TCP/IP Stack + NIC                           │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

## 3.2 Component Responsibilities

### 3.2.1 Event Loop

The event loop is the broker's core. It MUST:

1. Call `io_uring_enter()` to wait for completions and submit pending operations
2. Process all available CQEs before submitting new SQEs
3. Never block except in `io_uring_enter()`
4. Maintain single-threaded execution (no spawned threads)

```c
void run(struct broker *b) {
    while (b->running) {
        // Single syscall: wait for completions and submit pending
        io_uring_enter(b->ring_fd, pending, 1, IORING_ENTER_GETEVENTS);
        
        // Process all completions
        while (cqe = next_cqe(b)) {
            dispatch(b, cqe);
            advance_cq(b);
        }
    }
}
```

### 3.2.2 Buffer Pool

The buffer pool provides fixed-size buffers for all I/O operations. It MUST:

1. Allocate all buffers at startup via `mmap()`
2. Register buffers with io_uring via `IORING_REGISTER_BUFFERS`
3. Provide O(1) allocation and deallocation via a freelist
4. Never grow or shrink after initialization

See [Section 4: Memory Model](./04-memory.md) for details.

### 3.2.3 Connection Slots

Connection slots track per-connection state. The slot array MUST:

1. Be sized to `MAX_CONNS` at startup
2. Be indexed by direct descriptor number for O(1) lookup
3. Track connection state machine (connecting, connected, draining)
4. Store MQTT session state (client ID, subscriptions, pending ACKs)

See [Section 6: Connection Management](./06-connections.md) for details.

### 3.2.4 Topic Trie

The topic trie indexes subscriptions for efficient matching. It MUST:

1. Support exact topic matching in O(topic_length) time
2. Support wildcard matching (`+`, `#`) 
3. Use a bitmap per topic node for O(1) subscriber lookup
4. Maintain atomic subscriber counts for safe concurrent access

See [Section 8: Topic Trie](./08-trie.md) for details.

### 3.2.5 Message References

Message references track in-flight publishes during fan-out. They MUST:

1. Be allocated from a fixed pool
2. Store buffer index, offset, length for zero-copy sends
3. Maintain an atomic refcount decremented on send completion
4. Return the buffer to the pool when refcount reaches zero

See [Section 10: Buffer Lifecycle](./10-buffers.md) for details.

## 3.3 Data Flow

### 3.3.1 Publish Flow (QoS 0)

```
1. RECV CQE arrives with PUBLISH packet in buffer[N]
         │
         ▼
2. Parse PUBLISH in-place (no copy)
   Extract: topic, payload offset, payload length
         │
         ▼
3. Match topic in trie
   Count total subscribers: M
         │
         ▼
4. Allocate msg_ref from pool
   Set: buf_idx=N, offset, len, refcount=M
         │
         ▼
5. For each subscriber fd in topic bitmap:
   Submit SEND_ZC SQE referencing buffer[N]
         │
         ▼
6. io_uring_enter() submits all SQEs
         │
         ▼
7. Kernel sends data (zero-copy from buffer[N])
         │
         ▼
8. SEND CQEs arrive (possibly out of order)
   Each: atomic_sub(&msg_ref.refcount, 1)
         │
         ▼
9. When refcount hits 0:
   Return buffer[N] to free pool
   Return msg_ref to free pool
```

### 3.3.2 Connect Flow

```
1. ACCEPT CQE arrives with new fd
         │
         ▼
2. Register fd as direct descriptor (or use multishot direct accept)
         │
         ▼
3. Allocate connection slot[fd]
   Set state = CONNECTING
         │
         ▼
4. Submit RECV SQE for CONNECT packet
         │
         ▼
5. RECV CQE arrives with CONNECT packet
         │
         ▼
6. Parse and validate CONNECT
         │
         ▼
7. Initialize session state
   Set state = CONNECTED
         │
         ▼
8. Submit SEND SQE for CONNACK
         │
         ▼
9. Submit multishot RECV SQE for subsequent packets
```

### 3.3.3 Subscribe Flow

```
1. RECV CQE with SUBSCRIBE packet
         │
         ▼
2. Parse topic filters
         │
         ▼
3. For each filter:
   Insert/update trie node
   Set bit in fd_bitmap
   atomic_add(&node.sub_count, 1)
         │
         ▼
4. Submit SEND SQE for SUBACK
```

## 3.4 Invariants

The following invariants MUST be maintained:

1. **Single writer**: Only the event loop thread modifies broker state
2. **Atomic refcounts**: Buffer refcounts are the only shared mutable state
3. **No allocation on hot path**: PUBLISH handling never calls mmap or equivalent
4. **No copies on hot path**: PUBLISH payload is never memcpy'd during fan-out
5. **Bounded memory**: Total memory usage is fixed at startup
6. **Bounded latency**: No operation holds a lock or spins indefinitely

## 3.5 Error Handling

Errors are categorized by severity:

| Category | Examples | Response |
|----------|----------|----------|
| Fatal | Ring setup failure, mmap failure | Exit with error code |
| Connection | Parse error, protocol violation | Close connection |
| Transient | Send buffer full | Retry or apply backpressure |
| Ignorable | Client disconnect during send | Continue (refcount still decremented) |

On any error, the broker MUST:

1. Never corrupt shared state
2. Never leak buffers or slots
3. Log sufficient detail for debugging (if logging enabled)
4. Continue processing other connections
