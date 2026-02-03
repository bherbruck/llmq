// mem/msgbuf.h - Reference-counted message buffers for zero-copy fan-out
// Allows a single encoded message to be sent to multiple subscribers

#ifndef MEM_MSGBUF_H
#define MEM_MSGBUF_H

#include "sys/types.h"
#include "sys/syscall.h"
#include "config.h"

// =============================================================================
// Message Buffer Pool
// =============================================================================

// Maximum message size (topic + payload + header overhead)
#ifndef LLMQ_MSG_BUF_SIZE
#define LLMQ_MSG_BUF_SIZE 65536
#endif

// Number of message buffers (concurrent fan-out messages)
#ifndef LLMQ_MSG_BUF_COUNT
#define LLMQ_MSG_BUF_COUNT 256
#endif

// Message buffer header (stored at start of each buffer slot)
struct msg_header {
    u32 ref_count; // Number of pending sends (atomic decrement on CQE)
    u32 data_len;  // Length of encoded message data
};

// Message buffer pool
struct msg_pool {
    u8 *buffers;     // mmap'd array of buffer slots
    u32 *free_stack; // Stack of free buffer indices
    u32 slot_size;   // Size of each slot (header + data)
    u32 capacity;    // Total number of slots
    u32 free_count;  // Number of free slots
    u32 high_water;  // Peak usage
};

#define MSG_POOL_INVALID ((u32) - 1)

// Header size aligned to 8 bytes for data alignment
#define MSG_HEADER_SIZE ((sizeof(struct msg_header) + 7) & ~7ULL)

// Initialize message buffer pool
INLINE i32 msg_pool_init(struct msg_pool *p, u32 capacity, u32 max_msg_size) {
    p->slot_size  = (u32)(MSG_HEADER_SIZE + max_msg_size);
    p->capacity   = capacity;
    p->free_count = capacity;
    p->high_water = 0;

    // mmap the buffer array
    usize buffers_size = (usize)capacity * p->slot_size;
    p->buffers =
        (u8 *)sys_mmap(NULL, buffers_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(p->buffers)) {
        return -1;
    }

    // mmap the free stack
    usize stack_size = (usize)capacity * sizeof(u32);
    p->free_stack =
        (u32 *)sys_mmap(NULL, stack_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(p->free_stack)) {
        sys_munmap(p->buffers, buffers_size);
        return -1;
    }

    // Initialize free stack
    for (u32 i = 0; i < capacity; i++) {
        p->free_stack[i] = capacity - 1 - i;
    }

    return 0;
}

// Cleanup message buffer pool
INLINE void msg_pool_cleanup(struct msg_pool *p) {
    if (p->buffers) {
        sys_munmap(p->buffers, (usize)p->capacity * p->slot_size);
        p->buffers = NULL;
    }
    if (p->free_stack) {
        sys_munmap(p->free_stack, (usize)p->capacity * sizeof(u32));
        p->free_stack = NULL;
    }
}

// Get pointer to slot by index
INLINE u8 *msg_pool_slot(struct msg_pool *p, u32 idx) {
    if (idx >= p->capacity) {
        return NULL;
    }
    return p->buffers + ((usize)idx * p->slot_size);
}

// Get header from slot
INLINE struct msg_header *msg_pool_header(struct msg_pool *p, u32 idx) {
    return (struct msg_header *)msg_pool_slot(p, idx);
}

// Get data pointer from slot (after header)
INLINE u8 *msg_pool_data(struct msg_pool *p, u32 idx) {
    u8 *slot = msg_pool_slot(p, idx);
    if (!slot) {
        return NULL;
    }
    return slot + MSG_HEADER_SIZE;
}

// Allocate a message buffer, returns index or MSG_POOL_INVALID
INLINE u32 msg_pool_alloc(struct msg_pool *p) {
    if (p->free_count == 0) {
        return MSG_POOL_INVALID;
    }
    p->free_count--;

    // Track high water mark
    u32 used = p->capacity - p->free_count;
    if (used > p->high_water) {
        p->high_water = used;
    }

    u32 idx              = p->free_stack[p->free_count];
    struct msg_header *h = msg_pool_header(p, idx);
    h->ref_count         = 0;
    h->data_len          = 0;
    return idx;
}

// Free a message buffer back to pool
INLINE void msg_pool_free(struct msg_pool *p, u32 idx) {
    if (idx >= p->capacity) {
        return;
    }
    p->free_stack[p->free_count] = idx;
    p->free_count++;
}

// Increment reference count (call before each send)
INLINE void msg_pool_ref(struct msg_pool *p, u32 idx) {
    struct msg_header *h = msg_pool_header(p, idx);
    if (h) {
        h->ref_count++;
    }
}

// Decrement reference count, free if zero (call on send CQE)
// Returns true if buffer was freed
INLINE bool msg_pool_unref(struct msg_pool *p, u32 idx) {
    struct msg_header *h = msg_pool_header(p, idx);
    if (!h || h->ref_count == 0) {
        return false;
    }
    h->ref_count--;
    if (h->ref_count == 0) {
        msg_pool_free(p, idx);
        return true;
    }
    return false;
}

#endif // MEM_MSGBUF_H
