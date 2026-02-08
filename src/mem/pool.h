// mem/pool.h - Fixed-size buffer pool allocator
// Uses mmap for allocation, free list for O(1) alloc/free
//
// buf_pool uses chunked layout for io_uring safety: each chunk is independently
// mmap'd so existing buffer pointers (held by in-flight SQEs) remain valid when
// the pool grows. Growth adds a new chunk; buf_pool_get uses bit-shift addressing
// for O(1) chunk lookup.

#ifndef MEM_POOL_H
#define MEM_POOL_H

#include "sys/types.h"
#include "sys/syscall.h"

// =============================================================================
// Buffer Pool (chunked, growable, io_uring safe)
// =============================================================================

#define BUF_POOL_MAX_CHUNKS 32

struct buf_pool {
    u8 *chunks[BUF_POOL_MAX_CHUNKS]; // Independently mmap'd buffer chunks
    u32 *free_stack;  // Stack of free buffer indices (grows via mremap)
    u32 buf_size;     // Size of each buffer
    u32 capacity;     // Total number of buffers across all chunks
    u32 free_count;   // Number of free buffers
    u32 high_water;   // Peak usage (capacity - min(free_count))
    u32 chunk_cap;    // Buffers per chunk (power of 2)
    u32 chunk_mask;   // chunk_cap - 1
    u8 chunk_shift;   // log2(chunk_cap) for fast indexing
    u8 num_chunks;    // Number of allocated chunks
};

#define BUF_POOL_INVALID ((u32) - 1)

// Compute log2 of a power-of-2 value
INLINE u8 buf_pool_log2(u32 v) {
    u8 r = 0;
    while (v > 1) { v >>= 1; r++; }
    return r;
}

// Round up to next power of 2
INLINE u32 buf_pool_next_pow2(u32 v) {
    v--;
    v |= v >> 1; v |= v >> 2; v |= v >> 4;
    v |= v >> 8; v |= v >> 16;
    return v + 1;
}

// Grow buffer pool by adding a new chunk
// Returns 0 on success, -1 on failure
INLINE i32 buf_pool_grow(struct buf_pool *p) {
    if (p->num_chunks >= BUF_POOL_MAX_CHUNKS) {
        return -1;
    }

    // Allocate new buffer chunk
    usize chunk_bytes = (usize)p->chunk_cap * p->buf_size;
    u8 *new_chunk = (u8 *)sys_mmap(NULL, chunk_bytes, PROT_READ | PROT_WRITE,
                                    MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(new_chunk)) {
        return -1;
    }

    // Grow free stack via mremap (just u32 indices, not held by io_uring)
    u32 old_cap    = p->capacity;
    u32 new_cap    = old_cap + p->chunk_cap;
    usize old_sz   = (usize)old_cap * sizeof(u32);
    usize new_sz   = (usize)new_cap * sizeof(u32);
    void *new_stack = sys_mremap(p->free_stack, old_sz, new_sz, MREMAP_MAYMOVE);
    if (IS_ERR(new_stack)) {
        sys_munmap(new_chunk, chunk_bytes);
        return -1;
    }
    p->free_stack = (u32 *)new_stack;

    // Register chunk
    p->chunks[p->num_chunks] = new_chunk;
    p->num_chunks++;

    // Push new indices onto free stack (reverse order for cache locality)
    for (u32 i = 0; i < p->chunk_cap; i++) {
        p->free_stack[p->free_count++] = new_cap - 1 - i;
    }

    p->capacity = new_cap;
    return 0;
}

// Initialize a buffer pool with given capacity and buffer size
// Capacity is rounded up to the next power of 2 for chunk addressing
// Returns 0 on success, -1 on failure
INLINE i32 buf_pool_init(struct buf_pool *p, u32 capacity, u32 buf_size) {
    // Round up to power of 2 for bit-shift addressing
    u32 chunk_cap = buf_pool_next_pow2(capacity);

    p->buf_size    = buf_size;
    p->chunk_cap   = chunk_cap;
    p->chunk_mask  = chunk_cap - 1;
    p->chunk_shift = buf_pool_log2(chunk_cap);
    p->num_chunks  = 0;
    p->capacity    = 0;
    p->free_count  = 0;
    p->high_water  = 0;

    // Zero out chunk array
    for (u32 i = 0; i < BUF_POOL_MAX_CHUNKS; i++) {
        p->chunks[i] = NULL;
    }

    // Allocate first chunk's free stack
    usize stack_size = (usize)chunk_cap * sizeof(u32);
    p->free_stack = (u32 *)sys_mmap(NULL, stack_size, PROT_READ | PROT_WRITE,
                                     MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(p->free_stack)) {
        return -1;
    }

    // Allocate first chunk of buffers
    usize chunk_bytes = (usize)chunk_cap * buf_size;
    p->chunks[0] = (u8 *)sys_mmap(NULL, chunk_bytes, PROT_READ | PROT_WRITE,
                                    MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(p->chunks[0])) {
        sys_munmap(p->free_stack, stack_size);
        return -1;
    }
    p->num_chunks = 1;
    p->capacity   = chunk_cap;
    p->free_count = chunk_cap;

    // Initialize free stack (reverse order for cache locality)
    for (u32 i = 0; i < chunk_cap; i++) {
        p->free_stack[i] = chunk_cap - 1 - i;
    }

    return 0;
}

// Cleanup a buffer pool
INLINE void buf_pool_cleanup(struct buf_pool *p) {
    usize chunk_bytes = (usize)p->chunk_cap * p->buf_size;
    for (u8 i = 0; i < p->num_chunks; i++) {
        if (p->chunks[i]) {
            sys_munmap(p->chunks[i], chunk_bytes);
            p->chunks[i] = NULL;
        }
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
        if (buf_pool_grow(p) < 0) {
            return BUF_POOL_INVALID;
        }
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
// O(1) via chunk_shift/chunk_mask addressing
INLINE u8 *buf_pool_get(struct buf_pool *p, u32 idx) {
    u32 chunk  = idx >> p->chunk_shift;
    u32 offset = idx & p->chunk_mask;
    if (unlikely(chunk >= p->num_chunks)) {
        return NULL;
    }
    return p->chunks[chunk] + ((usize)offset * p->buf_size);
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
