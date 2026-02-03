# Section 4: Memory Model

## 4.1 Design Principles

The broker's memory model is designed around three principles:

1. **Static allocation**: All memory allocated at startup, never during operation
2. **Flat structures**: Arrays indexed by integer, no pointer chasing
3. **Cache-friendly layout**: Hot data in contiguous memory, cold data separate

## 4.2 Memory Regions

All memory is obtained via `mmap()` with `MAP_PRIVATE | MAP_ANONYMOUS`. The broker MUST NOT use malloc, calloc, realloc, or free.

```
┌─────────────────────────────────────────────────────────────────┐
│                        MEMORY LAYOUT                             │
├─────────────────────────────────────────────────────────────────┤
│  Region              │ Size                │ Purpose             │
├─────────────────────────────────────────────────────────────────┤
│  broker struct       │ ~4 KiB              │ Core state          │
│  io_uring rings      │ ~256 KiB            │ SQ + CQ + SQEs      │
│  connection slots    │ MAX_CONNS × 128 B   │ Per-conn state      │
│  buffer pool         │ MAX_BUFS × 64 KiB   │ I/O buffers         │
│  topic trie nodes    │ MAX_TOPICS × 1 KiB  │ Subscription index  │
│  message refs        │ MAX_INFLIGHT × 32 B │ Fan-out tracking    │
│  recv buffer ring    │ ~64 KiB             │ io_uring buf_ring   │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2.1 Default Sizes

| Constant | Default | Memory |
|----------|---------|--------|
| MAX_CONNS | 65,536 | 8 MiB |
| MAX_BUFS | 4,096 | 256 MiB |
| BUFFER_SIZE | 65,536 (64 KiB) | - |
| MAX_TOPICS | 65,536 | 64 MiB |
| MAX_INFLIGHT | 65,536 | 2 MiB |

Total default footprint: ~330 MiB

These SHOULD be configurable at startup but MUST NOT change during operation.

## 4.3 Buffer Pool

### 4.3.1 Structure

```c
struct buffer_pool {
    u8 *base;                    // mmap'd region: MAX_BUFS × BUFFER_SIZE
    u32 capacity;                // MAX_BUFS
    u32 free_head;               // Head of freelist (index, not pointer)
    u32 free_count;              // Available buffers
    u32 next[MAX_BUFS];          // Freelist links (index of next free)
};
```

The pool MUST:

1. Allocate `capacity × BUFFER_SIZE` bytes via single mmap call
2. Initialize freelist linking all buffers
3. Register entire region with io_uring via `IORING_REGISTER_BUFFERS`

### 4.3.2 Allocation

Buffer allocation is O(1) via freelist pop:

```c
static inline i32 buf_alloc(struct buffer_pool *pool) {
    if (pool->free_count == 0) {
        return -1;  // Pool exhausted
    }
    u32 idx = pool->free_head;
    pool->free_head = pool->next[idx];
    pool->free_count--;
    return (i32)idx;
}
```

### 4.3.3 Deallocation

Buffer deallocation is O(1) via freelist push:

```c
static inline void buf_free(struct buffer_pool *pool, u32 idx) {
    pool->next[idx] = pool->free_head;
    pool->free_head = idx;
    pool->free_count++;
}
```

### 4.3.4 Addressing

To get a pointer to buffer contents:

```c
static inline u8 *buf_ptr(struct buffer_pool *pool, u32 idx) {
    return pool->base + ((u64)idx * BUFFER_SIZE);
}
```

For io_uring registered buffer operations, use the buffer index directly in the SQE.

## 4.4 Connection Slots

### 4.4.1 Structure

```c
struct conn_slot {
    // Hot data (accessed every packet) - first cache line
    i32 fd;                      // Direct descriptor index
    u8  state;                   // Connection state
    u8  protocol_version;        // MQTT 4 or 5
    u16 keepalive;               // Keepalive interval (seconds)
    u32 recv_buf_idx;            // Assigned receive buffer
    u16 recv_offset;             // Current parse position
    u16 recv_len;                // Bytes received in buffer
    u32 last_active;             // Timestamp for keepalive
    u8  _pad1[32];               // Pad to 64 bytes
    
    // Cold data (accessed on connect/subscribe) - second cache line
    u32 client_id_hash;          // For fast lookup
    u16 client_id_len;
    u8  client_id[64];           // Inline storage for common case
    u8  _pad2[58];               // Pad to 128 bytes total
};

_Static_assert(sizeof(struct conn_slot) == 128, "conn_slot must be 128 bytes");
```

### 4.4.2 Indexing

Slots are indexed by direct descriptor number:

```c
struct conn_slot slots[MAX_CONNS];

// Access slot for connection
struct conn_slot *slot = &slots[direct_fd];
```

When using direct descriptors, the kernel assigns indices sequentially from the registered file table, making fd-to-slot mapping trivial.

### 4.4.3 State Machine

```
         ACCEPT
            │
            ▼
    ┌───────────────┐
    │   CONNECTING  │  Waiting for CONNECT packet
    └───────────────┘
            │
            │ Valid CONNECT received
            ▼
    ┌───────────────┐
    │   CONNECTED   │  Normal operation
    └───────────────┘
            │
            │ Disconnect or error
            ▼
    ┌───────────────┐
    │   DRAINING    │  Flushing pending sends
    └───────────────┘
            │
            │ All sends complete
            ▼
    ┌───────────────┐
    │     FREE      │  Slot available for reuse
    └───────────────┘
```

## 4.5 Message References

### 4.5.1 Structure

```c
struct msg_ref {
    u32 buf_idx;                 // Buffer containing message
    u32 offset;                  // Offset within buffer
    u32 len;                     // Message length
    _Atomic u32 refcount;        // Outstanding sends
};
```

### 4.5.2 Pool Management

Message refs use the same freelist pattern as buffers:

```c
struct msg_ref_pool {
    struct msg_ref refs[MAX_INFLIGHT];
    u32 free_head;
    u32 free_count;
    u32 next[MAX_INFLIGHT];
};
```

### 4.5.3 Lifecycle

1. **Allocate**: Pop from freelist when PUBLISH received
2. **Initialize**: Set buf_idx, offset, len; store refcount = subscriber_count
3. **Use**: Submit SEND_ZC operations, each carries msg_ref index in user_data
4. **Decrement**: On each SEND completion, `atomic_sub(&refcount, 1)`
5. **Release**: When `atomic_sub` returns 1 (was 1, now 0), push to freelist and free buffer

## 4.6 Topic Trie Nodes

See [Section 8: Topic Trie](./08-trie.md) for detailed structure.

Basic layout:

```c
struct topic_node {
    _Atomic u32 sub_count;       // Number of subscribers
    u64 fd_bitmap[16];           // 1024 bits = 1024 possible subscribers
    u32 children[64];            // Child node indices (trie structure)
    u16 child_count;
    u16 name_len;
    u8  name[128];               // Topic segment name
    u8  _pad[...];               // Pad to 1 KiB
};
```

## 4.7 Memory Safety

### 4.7.1 Bounds Checking

All array accesses MUST be bounds-checked:

```c
static inline struct conn_slot *get_slot(struct broker *b, u32 fd) {
    if (fd >= MAX_CONNS) return NULL;
    return &b->slots[fd];
}
```

### 4.7.2 Use-After-Free Prevention

Freed structures are immediately placed on freelists but not zeroed. Code MUST NOT access structures after freeing. The single-threaded model prevents races, but discipline is required.

### 4.7.3 Buffer Overrun Prevention

MQTT parsing MUST validate lengths before accessing data:

```c
// BAD: trusts packet length
u16 topic_len = read_u16(buf + offset);
memcpy(topic, buf + offset + 2, topic_len);  // Potential overrun

// GOOD: validates against buffer bounds
u16 topic_len = read_u16(buf + offset);
if (offset + 2 + topic_len > buf_len) {
    return PARSE_ERROR;
}
memcpy(topic, buf + offset + 2, topic_len);
```

## 4.8 Alignment

All structures SHOULD be aligned to cache line boundaries (64 bytes) for hot data. The broker SHOULD use `__attribute__((aligned(64)))` on performance-critical structures.

mmap allocations are page-aligned (4096 bytes) by default, exceeding cache line requirements.

## 4.9 NUMA Considerations

For NUMA systems, the broker SHOULD:

1. Bind process to a single NUMA node via `numactl` or `set_mempolicy()`
2. Ensure all mmap allocations come from local memory
3. Run one broker process per NUMA node with SO_REUSEPORT

The specification does not require NUMA-aware allocation within the broker, as the single-threaded model naturally localizes memory access.
