// sys/io_uring.c - io_uring ring setup implementation

#include "sys/io_uring.h"
#include "mem/string.h"

// Offsets from mmap base
#define IORING_OFF_SQ_RING 0ULL
#define IORING_OFF_CQ_RING 0x8000000ULL
#define IORING_OFF_SQES    0x10000000ULL

i32 ring_init(struct ring *r, u32 entries) {
    struct io_uring_params p;
    memset(&p, 0, sizeof(p));

    // SINGLE_ISSUER: Only one task will submit - allows optimizations
    // COOP_TASKRUN: Completions delivered only during io_uring_enter (deterministic)
    p.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN;

    i32 fd = sys_io_uring_setup(entries, &p);
    if (fd < 0) {
        return fd;
    }

    r->fd         = fd;
    r->sq_entries = p.sq_entries;
    r->cq_entries = p.cq_entries;

    // Calculate mmap sizes
    r->sq_ring_sz = p.sq_off.array + p.sq_entries * sizeof(u32);
    r->cq_ring_sz = p.cq_off.cqes + p.cq_entries * sizeof(struct io_uring_cqe);
    r->sqes_sz    = p.sq_entries * sizeof(struct io_uring_sqe);

    // mmap the SQ ring (must be MAP_SHARED for kernel to see updates)
    r->sq_ring_ptr =
        sys_mmap(NULL, r->sq_ring_sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, IORING_OFF_SQ_RING);
    if (r->sq_ring_ptr == MAP_FAILED) {
        sys_close(fd);
        return -ENOMEM;
    }

    // Set up SQ pointers
    u8 *sq_ptr  = (u8 *)r->sq_ring_ptr;
    r->sq_head  = (u32 *)(sq_ptr + p.sq_off.head);
    r->sq_tail  = (u32 *)(sq_ptr + p.sq_off.tail);
    r->sq_mask  = *(u32 *)(sq_ptr + p.sq_off.ring_mask);
    r->sq_array = (u32 *)(sq_ptr + p.sq_off.array);

    // mmap the CQ ring (may share with SQ ring on newer kernels)
    if (p.features & (1U << 0)) { // IORING_FEAT_SINGLE_MMAP
        r->cq_ring_ptr = r->sq_ring_ptr;
    } else {
        r->cq_ring_ptr = sys_mmap(NULL, r->cq_ring_sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
                                  IORING_OFF_CQ_RING);
        if (r->cq_ring_ptr == MAP_FAILED) {
            sys_munmap(r->sq_ring_ptr, r->sq_ring_sz);
            sys_close(fd);
            return -ENOMEM;
        }
    }

    // Set up CQ pointers
    u8 *cq_ptr = (u8 *)r->cq_ring_ptr;
    r->cq_head = (u32 *)(cq_ptr + p.cq_off.head);
    r->cq_tail = (u32 *)(cq_ptr + p.cq_off.tail);
    r->cq_mask = *(u32 *)(cq_ptr + p.cq_off.ring_mask);
    r->cqes    = (struct io_uring_cqe *)(cq_ptr + p.cq_off.cqes);

    // Initialize local SQ tail for batched submission
    r->sq_tail_local = *r->sq_tail;

    // Initialize local CQ head/tail for batched advance
    r->cq_head_local = *r->cq_head;
    r->cq_tail_local = *r->cq_tail;

    // mmap the SQEs (must be MAP_SHARED for kernel to see updates)
    r->sqes_ptr =
        sys_mmap(NULL, r->sqes_sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, IORING_OFF_SQES);
    if (r->sqes_ptr == MAP_FAILED) {
        if (r->cq_ring_ptr != r->sq_ring_ptr) {
            sys_munmap(r->cq_ring_ptr, r->cq_ring_sz);
        }
        sys_munmap(r->sq_ring_ptr, r->sq_ring_sz);
        sys_close(fd);
        return -ENOMEM;
    }
    r->sqes = (struct io_uring_sqe *)r->sqes_ptr;

    return 0;
}

void ring_cleanup(struct ring *r) {
    if (r->sqes_ptr) {
        sys_munmap(r->sqes_ptr, r->sqes_sz);
    }
    if (r->cq_ring_ptr && r->cq_ring_ptr != r->sq_ring_ptr) {
        sys_munmap(r->cq_ring_ptr, r->cq_ring_sz);
    }
    if (r->sq_ring_ptr) {
        sys_munmap(r->sq_ring_ptr, r->sq_ring_sz);
    }
    if (r->fd >= 0) {
        sys_close(r->fd);
    }
}
