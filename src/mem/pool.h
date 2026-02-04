// mem/pool.h - Fixed-size buffer pool allocator
// Uses mmap for allocation, free list for O(1) alloc/free

#ifndef MEM_POOL_H
#define MEM_POOL_H

#include "sys/types.h"
#include "sys/syscall.h"

// =============================================================================
// Buffer Pool
// =============================================================================

struct buf_pool {
    u8 *buffers;     // mmap'd array of fixed-size buffers
    u32 *free_stack; // Stack of free buffer indices
    u32 buf_size;    // Size of each buffer
    u32 capacity;    // Total number of buffers
    u32 free_count;  // Number of free buffers
    u32 high_water;  // Peak usage (capacity - min(free_count))
};

#define BUF_POOL_INVALID ((u32) - 1)

// Initialize a buffer pool with given capacity and buffer size
// Returns 0 on success, -1 on failure
INLINE i32 buf_pool_init(struct buf_pool *p, u32 capacity, u32 buf_size) {
    p->buf_size   = buf_size;
    p->capacity   = capacity;
    p->free_count = capacity;
    p->high_water = 0;

    // mmap the buffer array
    usize buffers_size = (usize)capacity * buf_size;
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

    // Initialize free stack (all indices free, in reverse order for cache locality)
    for (u32 i = 0; i < capacity; i++) {
        p->free_stack[i] = capacity - 1 - i;
    }

    return 0;
}

// Cleanup a buffer pool
INLINE void buf_pool_cleanup(struct buf_pool *p) {
    if (p->buffers) {
        sys_munmap(p->buffers, (usize)p->capacity * p->buf_size);
        p->buffers = NULL;
    }
    if (p->free_stack) {
        sys_munmap(p->free_stack, (usize)p->capacity * sizeof(u32));
        p->free_stack = NULL;
    }
}

// Allocate a buffer from the pool
// Returns buffer index, or BUF_POOL_INVALID if pool exhausted
INLINE u32 buf_pool_alloc(struct buf_pool *p) {
    if (unlikely(p->free_count == 0)) {
        return BUF_POOL_INVALID;
    }
    p->free_count--;
    // Track peak usage
    u32 used = p->capacity - p->free_count;
    if (unlikely(used > p->high_water)) {
        p->high_water = used;
    }
    return p->free_stack[p->free_count];
}

// Free a buffer back to the pool
INLINE void buf_pool_free(struct buf_pool *p, u32 idx) {
    if (unlikely(idx >= p->capacity)) {
        return; // Invalid index
    }
    p->free_stack[p->free_count] = idx;
    p->free_count++;
}

// Get pointer to buffer by index
INLINE u8 *buf_pool_get(struct buf_pool *p, u32 idx) {
    if (unlikely(idx >= p->capacity)) {
        return NULL;
    }
    return p->buffers + ((usize)idx * p->buf_size);
}

// Get number of allocated buffers
INLINE u32 buf_pool_used(struct buf_pool *p) {
    return p->capacity - p->free_count;
}

// =============================================================================
// Slot Pool (for variable-size structs with free list)
// =============================================================================

struct slot_pool {
    void *slots;     // mmap'd array of fixed-size slots
    u32 *free_stack; // Stack of free slot indices
    u32 slot_size;   // Size of each slot
    u32 capacity;    // Total number of slots
    u32 free_count;  // Number of free slots
};

#define SLOT_POOL_INVALID ((u32) - 1)

// Initialize a slot pool
INLINE i32 slot_pool_init(struct slot_pool *p, u32 capacity, u32 slot_size) {
    p->slot_size  = slot_size;
    p->capacity   = capacity;
    p->free_count = capacity;

    usize slots_size = (usize)capacity * slot_size;
    p->slots = sys_mmap(NULL, slots_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(p->slots)) {
        return -1;
    }

    usize stack_size = (usize)capacity * sizeof(u32);
    p->free_stack =
        (u32 *)sys_mmap(NULL, stack_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(p->free_stack)) {
        sys_munmap(p->slots, slots_size);
        return -1;
    }

    for (u32 i = 0; i < capacity; i++) {
        p->free_stack[i] = capacity - 1 - i;
    }

    return 0;
}

INLINE void slot_pool_cleanup(struct slot_pool *p) {
    if (p->slots) {
        sys_munmap(p->slots, (usize)p->capacity * p->slot_size);
        p->slots = NULL;
    }
    if (p->free_stack) {
        sys_munmap(p->free_stack, (usize)p->capacity * sizeof(u32));
        p->free_stack = NULL;
    }
}

INLINE u32 slot_pool_alloc(struct slot_pool *p) {
    if (unlikely(p->free_count == 0)) {
        return SLOT_POOL_INVALID;
    }
    p->free_count--;
    return p->free_stack[p->free_count];
}

INLINE void slot_pool_free(struct slot_pool *p, u32 idx) {
    if (unlikely(idx >= p->capacity)) {
        return;
    }
    p->free_stack[p->free_count] = idx;
    p->free_count++;
}

INLINE void *slot_pool_get(struct slot_pool *p, u32 idx) {
    if (unlikely(idx >= p->capacity)) {
        return NULL;
    }
    return (u8 *)p->slots + ((usize)idx * p->slot_size);
}

#endif // MEM_POOL_H
