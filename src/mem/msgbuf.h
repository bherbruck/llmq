// mem/msgbuf.h - Zero-copy message metadata for fan-out
// Canonical messages store metadata only - offsets/lengths pointing into stolen recv buffers
// One stolen buffer, N references via atomic refcount

#ifndef MEM_MSGBUF_H
#define MEM_MSGBUF_H

#include "sys/types.h"
#include "sys/syscall.h"
#include "config.h"

// =============================================================================
// Atomic Operations (for refcount)
// =============================================================================

typedef volatile u32 atomic_u32;

INLINE u32 atomic_load(atomic_u32 *a) {
    return *a;
}

// Single-threaded broker: use plain ops instead of atomics for ref counting.
INLINE void atomic_store(atomic_u32 *a, u32 val) {
    *a = val;
}

INLINE u32 atomic_inc(atomic_u32 *a) {
    return ++(*a);
}

INLINE u32 atomic_dec(atomic_u32 *a) {
    return --(*a);
}

// =============================================================================
// Canonical Message (metadata only, ~32 bytes)
// =============================================================================

// Canonical message: metadata pointing into stolen recv buffer
// The actual data lives in the stolen buffer, not copied here
struct canonical_msg {
    u32 buf_idx;          // Index into recv_pool (stolen buffer)
    u16 topic_off;        // Offset to topic string within buffer
    u16 topic_len;        // Topic length
    u32 payload_off;      // Offset to payload within buffer
    u32 payload_len;      // Payload length
    u8 qos;               // Publisher's QoS level
    u8 retain;            // Retain flag
    u8 dup;               // DUP flag
    u8 _pad;
    atomic_u32 ref_count; // Decremented per-subscriber completion
};

STATIC_ASSERT(sizeof(struct canonical_msg) == 24, "canonical_msg should be 24 bytes");

// =============================================================================
// Message Pool (metadata pool, not buffer pool)
// =============================================================================

// Pool configuration
#ifndef LLMQ_MSG_POOL_SIZE
#define LLMQ_MSG_POOL_SIZE 1024 // Max concurrent fan-out messages
#endif

struct msg_pool {
    struct canonical_msg *msgs; // mmap'd array of canonical messages
    u32 *free_stack;            // Stack of free message indices
    u32 capacity;               // Total number of slots
    u32 free_count;             // Number of free slots
    u32 high_water;             // Peak usage
};

#define MSG_POOL_INVALID ((u32)-1)

// Initialize message pool
INLINE i32 msg_pool_init(struct msg_pool *p, u32 capacity) {
    p->capacity   = capacity;
    p->free_count = capacity;
    p->high_water = 0;

    // mmap the message array
    usize msgs_size = (usize)capacity * sizeof(struct canonical_msg);
    p->msgs         = (struct canonical_msg *)sys_mmap(NULL, msgs_size, PROT_READ | PROT_WRITE,
                                                       MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(p->msgs)) {
        return -1;
    }

    // mmap the free stack
    usize stack_size = (usize)capacity * sizeof(u32);
    p->free_stack =
        (u32 *)sys_mmap(NULL, stack_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(p->free_stack)) {
        sys_munmap(p->msgs, msgs_size);
        return -1;
    }

    // Initialize free stack (reverse order for cache locality)
    for (u32 i = 0; i < capacity; i++) {
        p->free_stack[i] = capacity - 1 - i;
    }

    return 0;
}

// Cleanup message pool
INLINE void msg_pool_cleanup(struct msg_pool *p) {
    if (p->msgs) {
        sys_munmap(p->msgs, (usize)p->capacity * sizeof(struct canonical_msg));
        p->msgs = NULL;
    }
    if (p->free_stack) {
        sys_munmap(p->free_stack, (usize)p->capacity * sizeof(u32));
        p->free_stack = NULL;
    }
}

// Get message by index
INLINE struct canonical_msg *msg_pool_get(struct msg_pool *p, u32 idx) {
    if (unlikely(idx >= p->capacity)) {
        return NULL;
    }
    return &p->msgs[idx];
}

// Allocate a message slot, returns index or MSG_POOL_INVALID
INLINE u32 msg_pool_alloc(struct msg_pool *p) {
    if (unlikely(p->free_count == 0)) {
        return MSG_POOL_INVALID;
    }
    p->free_count--;

    // Track high water mark
    u32 used = p->capacity - p->free_count;
    if (unlikely(used > p->high_water)) {
        p->high_water = used;
    }

    u32 idx                  = p->free_stack[p->free_count];
    struct canonical_msg *m  = &p->msgs[idx];
    m->buf_idx               = MSG_POOL_INVALID;
    atomic_store(&m->ref_count, 0);
    return idx;
}

// Free a message slot back to pool
INLINE void msg_pool_free(struct msg_pool *p, u32 idx) {
    if (unlikely(idx >= p->capacity)) {
        return;
    }
    p->free_stack[p->free_count] = idx;
    p->free_count++;
}

// Increment reference count (call before each subscriber send)
INLINE void msg_pool_ref(struct msg_pool *p, u32 idx) {
    if (unlikely(idx >= p->capacity)) {
        return;
    }
    atomic_inc(&p->msgs[idx].ref_count);
}

// Decrement reference count
// Returns: new ref count (0 means caller should free the message and stolen buffer)
INLINE u32 msg_pool_unref(struct msg_pool *p, u32 idx) {
    if (unlikely(idx >= p->capacity)) {
        return 0;
    }
    return atomic_dec(&p->msgs[idx].ref_count);
}

// Get current ref count
INLINE u32 msg_pool_refcount(struct msg_pool *p, u32 idx) {
    if (unlikely(idx >= p->capacity)) {
        return 0;
    }
    return atomic_load(&p->msgs[idx].ref_count);
}

#endif // MEM_MSGBUF_H
