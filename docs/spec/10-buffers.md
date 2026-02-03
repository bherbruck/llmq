# Section 10: Buffer Lifecycle

## 10.1 Overview

Buffer lifecycle management ensures buffers are safely reused without use-after-free or double-free bugs. The only synchronization mechanism is an atomic refcount per message.

## 10.2 Surgical Zero-Copy Principle

The buffer holds the **entire** received packet, but we only reference the parts we need:

```
┌─────────────────────────────────────────────────────────────────┐
│                    RECEIVE BUFFER (64KB)                         │
├────────────────────────────────────────────────────────────────┤
│ [MQTT Fixed Header] [Topic] [Packet ID] [Payload...         ]   │
├────────────────────────────────────────────────────────────────┤
│        ↑               ↑         ↑          ↑                   │
│     parsed          topic_ptr  (ignored   payload_ptr           │
│     & discarded                 for QoS0)                       │
└─────────────────────────────────────────────────────────────────┘
```

**What lives vs. what dies:**

| Component | Lifetime | Notes |
|-----------|----------|-------|
| Buffer memory | Until refcount=0 | The actual 64KB allocation |
| Fixed header bytes | Effectively dead after parse | Values extracted to stack vars |
| Topic bytes | Lives until all sends complete | Referenced by outbound iovecs |
| Packet ID bytes | Dead for QoS 0, used briefly for QoS 1 | New packet ID generated per-subscriber |
| Payload bytes | Lives until all sends complete | Referenced by outbound iovecs |
| `mqtt_publish` struct | Stack-only, dies after fan-out | Contains pointers into buffer |
| `mqtt_publish_parts` | Lives with inflight entry | Contains offsets, not pointers |

The buffer stays alive as long as ANY part of it is needed. Once refcount hits zero, the entire buffer is returned to the pool - we don't track individual regions.

## 10.3 Buffer States

```
    ┌─────────────────────────────────────────────────────────────┐
    │                       BUFFER STATES                          │
    └─────────────────────────────────────────────────────────────┘
    
         ┌──────────┐
         │          │
         │   FREE   │◄─────────────────────────────────────────┐
         │          │                                          │
         └────┬─────┘                                          │
              │                                                │
              │ buf_alloc()                                    │
              ▼                                                │
         ┌──────────┐                                          │
         │          │                                          │
         │  OWNED   │  Assigned to connection for recv         │
         │          │                                          │
         └────┬─────┘                                          │
              │                                                │
              │ PUBLISH received, refcount = N                 │
              ▼                                                │
         ┌──────────┐                                          │
         │          │                                          │
         │ INFLIGHT │  Being sent to subscribers               │
         │          │                                          │
         └────┬─────┘                                          │
              │                                                │
              │ All CQEs received, refcount = 0                │
              ▼                                                │
         ┌──────────┐                                          │
         │          │                                          │
         │ RELEASED │──────────────────────────────────────────┘
         │          │  buf_free()
         └──────────┘
```

## 10.4 Message Reference Structure

```c
struct msg_ref {
    u32 buf_idx;                     // Index into buffer pool
    u32 offset;                      // Offset within buffer
    u32 len;                         // Message length
    _Atomic u32 refcount;            // Outstanding sends
};
```

The refcount is the ONLY atomic field. It tracks how many send operations still reference this buffer.

## 10.5 Lifecycle Operations

### 10.9.1 Allocation

When a PUBLISH is received and has subscribers:

```c
// 1. Allocate msg_ref from pool
u32 ref_idx = ref_alloc(&b->refs);
struct msg_ref *ref = &b->refs.refs[ref_idx];

// 2. Initialize with buffer info
ref->buf_idx = recv_buf_idx;
ref->offset = publish_offset;
ref->len = publish_len;

// 3. Set refcount to subscriber count
// Use release ordering - ensures buffer writes visible before sends read
atomic_store(&ref->refcount, subscriber_count);
```

### 10.9.2 Decrement on Completion

When a send CQE arrives:

```c
void handle_send_cqe(struct broker *b, struct io_uring_cqe *cqe) {
    u32 ref_idx = USER_DATA_CTX(cqe->user_data);
    struct msg_ref *ref = &b->refs.refs[ref_idx];
    
    // Wait for final CQE if using zero-copy
    if (cqe->flags & IORING_CQE_F_MORE) {
        return;  // More CQEs coming
    }
    
    // Decrement refcount
    // fetch_sub with acq_rel ordering:
    // - acquire: see all writes to buffer before we potentially free
    // - release: our decrement visible to others
    u32 old = atomic_sub(&ref->refcount, 1);
    
    if (old == 1) {
        // We decremented from 1 to 0, we're responsible for cleanup
        buf_free(&b->buffers, ref->buf_idx);
        ref_free(&b->refs, ref_idx);
    }
}
```

### 10.5.3 Edge Case: No Subscribers

If a PUBLISH has no matching subscribers:

```c
void handle_publish(struct broker *b, ...) {
    u32 sub_count = trie_count_subscribers(&b->trie, topic, topic_len);
    
    if (sub_count == 0) {
        // No subscribers - buffer stays with connection
        // Do NOT allocate msg_ref
        // Do NOT change buffer ownership
        return;
    }
    
    // Normal fan-out path...
}
```

### 10.5.4 Edge Case: Subscriber Count Changes

Between counting and sending, subscribers may disconnect:

```c
void fanout_publish(struct broker *b, ...) {
    // Count phase
    u32 expected_count = 0;
    trie_match(&b->trie, topic, len, count_callback, &expected_count);
    
    // Allocate ref with expected count
    atomic_store(&ref->refcount, expected_count);
    
    // Send phase
    u32 actual_sent = 0;
    trie_match(&b->trie, topic, len, send_callback, &actual_sent);
    
    // Reconcile
    if (actual_sent < expected_count) {
        u32 diff = expected_count - actual_sent;
        u32 old = atomic_sub(&ref->refcount, diff);
        
        if (old == diff) {
            // Refcount hit zero during adjustment
            buf_free(&b->buffers, ref->buf_idx);
            ref_free(&b->refs, ref_idx);
        }
    }
}
```

## 10.6 Memory Ordering

### 10.9.1 Required Orderings

| Operation | Ordering | Reason |
|-----------|----------|--------|
| Initial store | release | Buffer writes must be visible before sends |
| Decrement | acq_rel | See all writes, make decrement visible |
| Read (debug) | acquire | See latest value |

### 10.9.2 Why This Works

The pattern is a classic reference counting scheme:

1. **Publisher** writes buffer contents
2. **Publisher** stores refcount with release ordering
3. **Release** creates happens-before with any acquire
4. **Subscribers** (via kernel) read buffer contents
5. **On CQE**, handler decrements with acquire ordering
6. **Last decrementer** (whoever sees old=1) frees

The acquire-release pair ensures:
- All buffer writes happen-before any send reads them
- All decrements are totally ordered (no lost updates)
- The "last" decrementer sees all prior decrements

## 10.7 Freelist Management

### 10.9.1 Buffer Pool Freelist

```c
struct buffer_pool {
    u8 *base;                        // mmap'd buffer space
    u32 capacity;
    u32 free_head;                   // Index of first free buffer
    u32 free_count;
    u32 next[MAX_BUFS];              // Per-buffer next free index
};

u32 buf_alloc(struct buffer_pool *pool) {
    if (pool->free_count == 0) return -1;
    
    u32 idx = pool->free_head;
    pool->free_head = pool->next[idx];
    pool->free_count--;
    return idx;
}

void buf_free(struct buffer_pool *pool, u32 idx) {
    pool->next[idx] = pool->free_head;
    pool->free_head = idx;
    pool->free_count++;
}
```

### 10.9.2 Message Ref Freelist

Identical pattern:

```c
struct msg_ref_pool {
    struct msg_ref refs[MAX_INFLIGHT];
    u32 free_head;
    u32 free_count;
    u32 next[MAX_INFLIGHT];
};
```

### 10.7.3 Thread Safety

Because the broker is single-threaded, freelist operations need no synchronization. The only atomic operation is the msg_ref refcount, which is accessed from:

1. **Main thread**: Initial store, adjustment on send count mismatch
2. **Main thread**: Decrement on CQE (still single-threaded!)

Even CQE processing is in the main thread, so there's no actual concurrent access. The atomics are used for:

1. Correctness guarantees with kernel (memory barriers)
2. Future-proofing if multi-threaded CQE processing added

## 10.8 Debugging Support

### 10.9.1 Buffer State Tracking (Debug Mode)

```c
#ifdef DEBUG
enum buf_state {
    BUF_FREE,
    BUF_OWNED,
    BUF_INFLIGHT,
};

struct buffer_debug {
    enum buf_state state;
    u32 owner_fd;                    // If OWNED, which connection
    u32 ref_idx;                     // If INFLIGHT, which msg_ref
    u64 alloc_time;
    u64 free_time;
};

struct buffer_debug buf_debug[MAX_BUFS];
#endif
```

### 10.9.2 Leak Detection

On shutdown, verify all buffers returned:

```c
void check_buffer_leaks(struct buffer_pool *pool) {
    if (pool->free_count != pool->capacity) {
        log_error("Buffer leak: %u buffers not freed",
                  pool->capacity - pool->free_count);
        
        #ifdef DEBUG
        for (u32 i = 0; i < pool->capacity; i++) {
            if (buf_debug[i].state != BUF_FREE) {
                log_error("  Buffer %u: state=%d owner=%u",
                          i, buf_debug[i].state, buf_debug[i].owner_fd);
            }
        }
        #endif
    }
}
```

## 10.9 Failure Scenarios

### 10.9.1 Send Failure

If a send fails (connection reset, etc.), the CQE still arrives with an error code. The refcount is decremented normally:

```c
void handle_send_cqe(struct broker *b, struct io_uring_cqe *cqe) {
    // Error handling for connection
    if (cqe->res < 0) {
        handle_send_error(b, cqe);
    }
    
    // Refcount decrement happens regardless of success/failure
    // The send is "done" from the buffer's perspective
    decrement_refcount(b, cqe);
}
```

### 10.9.2 Connection Close During Fan-out

If a connection closes while sends are in flight:

1. Outstanding sends may fail with EBADF or similar
2. CQEs still arrive for failed sends
3. Refcount decrements still happen
4. Buffer eventually freed when refcount hits zero

No special handling needed - the system self-heals.

### 10.9.3 Broker Shutdown

On shutdown:

1. Stop accepting new connections
2. Stop processing new publishes
3. Wait for all pending sends to complete (or timeout)
4. Verify all buffers freed (debug mode)
5. munmap all memory

```c
void broker_shutdown(struct broker *b) {
    b->running = false;
    
    // Drain pending operations
    while (b->refs.free_count < MAX_INFLIGHT) {
        io_uring_enter(b->ring.fd, 0, 1, IORING_ENTER_GETEVENTS);
        process_cqes(b);
    }
    
    check_buffer_leaks(&b->buffers);
    check_ref_leaks(&b->refs);
}
```
