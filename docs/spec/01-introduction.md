# Section 1: Introduction

## 1.1 Background

Traditional MQTT broker implementations face inherent performance limitations stemming from their architecture:

1. **Syscall overhead**: Each send/receive operation requires a context switch to kernel mode
2. **Memory copying**: Messages are typically copied multiple times during fan-out (receive buffer → application → per-subscriber send buffer)
3. **Allocation pressure**: Dynamic memory allocation on the hot path introduces latency variance and fragmentation
4. **Threading complexity**: Multi-threaded designs require locks or complex lock-free structures for shared subscription state

Modern Linux kernels (5.1+) introduced io_uring, a fundamentally different I/O model that addresses these limitations through:

- Shared memory rings between userspace and kernel, eliminating most syscalls
- Batch submission of multiple operations in a single syscall
- Completion-driven programming model with optional polling
- Zero-copy transmission directly from userspace buffers
- Registered resources (buffers, file descriptors) for fast-path lookup

This specification defines a broker architecture that fully exploits these capabilities.

## 1.2 Motivation

Existing high-performance brokers (EMQX, VerneMQ, Mosquitto) achieve good throughput but leave performance on the table:

| Broker | Language | I/O Model | Zero-Copy | Allocations/msg |
|--------|----------|-----------|-----------|-----------------|
| Mosquitto | C | epoll + read/write | No | 2-3 |
| EMQX | Erlang | epoll via BEAM | No | Many (GC'd) |
| VerneMQ | Erlang | epoll via BEAM | No | Many (GC'd) |
| **Ringbroker** | C | io_uring | Yes | 0 |

The performance difference becomes significant at scale:

- 10,000 subscribers to a topic means 10,000 copies in traditional brokers
- With zero-copy send, the kernel DMAs from a single buffer to all NICs
- At 1M msg/sec, eliminating copies saves ~1GB/sec of memory bandwidth

## 1.3 Scope

This specification covers:

- Broker architecture and component design
- Memory layout and allocation strategy
- io_uring setup and operation
- MQTT 3.1.1 and 5.0 protocol handling
- Topic matching and subscription management
- Message fan-out and buffer lifecycle
- Multi-process scaling via SO_REUSEPORT

This specification does NOT cover:

- Client library implementation
- Clustering or distributed operation
- Persistence or durable sessions
- Authentication backends (LDAP, OAuth, etc.)
- Management APIs or metrics export

## 1.4 Design Principles

### 1.4.1 Push Work to the Kernel

The kernel is already optimized for moving bytes between file descriptors. Rather than reimplementing networking in userspace, we provide minimal routing logic and let io_uring handle:

- TCP connection management
- Buffer management for transmission
- Scatter-gather I/O
- Zero-copy to NIC

### 1.4.2 Predictable Memory

All memory is allocated at startup via mmap. The broker's memory footprint is:

```
total = sizeof(broker)
      + (MAX_CONNS × sizeof(conn_slot))
      + (MAX_BUFFERS × BUFFER_SIZE)
      + (MAX_TOPICS × sizeof(topic_node))
      + (MAX_INFLIGHT × sizeof(message_ref))
```

No malloc, no free, no realloc. Ever.

### 1.4.3 Single-Threaded Core

The event loop runs on a single thread with no locks. Concurrency is achieved through:

- Asynchronous I/O (io_uring handles parallelism in kernel)
- Multi-process scaling (fork with SO_REUSEPORT)
- Atomic refcounts only where unavoidable (buffer lifecycle)

### 1.4.4 Explicit Over Implicit

No hidden allocations, no garbage collection, no runtime overhead. Every operation's cost is visible in the code.

## 1.5 Document Organization

This specification is organized as follows:

- **Sections 2**: Terminology and conventions
- **Sections 3-6**: Core infrastructure (architecture, memory, io_uring, connections)
- **Sections 7-10**: Protocol and messaging (MQTT, trie, fan-out, buffers)
- **Sections 11-12**: Operations (scaling, security)
- **Appendices**: Reference material (syscalls, structures, build)

Cross-references use the format `[Section N.M](./NN-filename.md#anchor)`.
