// sys/io_uring.h - io_uring structures and operations
// Based on Linux kernel 6.6+ io_uring interface

#ifndef SYS_IO_URING_H
#define SYS_IO_URING_H

#include "types.h"
#include "syscall.h"

// =============================================================================
// io_uring constants
// =============================================================================

// Setup flags
#define IORING_SETUP_SQPOLL        (1U << 1)
#define IORING_SETUP_SQ_AFF        (1U << 2)
#define IORING_SETUP_CQSIZE        (1U << 3)
#define IORING_SETUP_COOP_TASKRUN  (1U << 8)  // Allow task work in any kernel context
#define IORING_SETUP_SINGLE_ISSUER (1U << 12)
#define IORING_SETUP_DEFER_TASKRUN (1U << 13) // Defer task work to io_uring_enter

// Enter flags
#define IORING_ENTER_GETEVENTS (1U << 0)
#define IORING_ENTER_SQ_WAKEUP (1U << 1)
#define IORING_ENTER_SQ_WAIT   (1U << 2)
#define IORING_ENTER_EXT_ARG   (1U << 3)

// SQ ring flags (for SQPOLL mode)
#define IORING_SQ_NEED_WAKEUP (1U << 0) // Kernel SQ poll thread needs wakeup
#define IORING_SQ_CQ_OVERFLOW (1U << 1) // CQ overflowed

// SQE flags
#define IOSQE_FIXED_FILE       (1U << 0)
#define IOSQE_IO_DRAIN         (1U << 1)
#define IOSQE_IO_LINK          (1U << 2)
#define IOSQE_IO_HARDLINK      (1U << 3)
#define IOSQE_ASYNC            (1U << 4)
#define IOSQE_BUFFER_SELECT    (1U << 5)
#define IOSQE_CQE_SKIP_SUCCESS (1U << 6)

// CQE flags
#define IORING_CQE_F_BUFFER        (1U << 0)
#define IORING_CQE_F_MORE          (1U << 1)
#define IORING_CQE_F_SOCK_NONEMPTY (1U << 2)
#define IORING_CQE_F_NOTIF         (1U << 3)

// Opcodes
#define IORING_OP_NOP            0
#define IORING_OP_READV          1
#define IORING_OP_WRITEV         2
#define IORING_OP_READ_FIXED     4
#define IORING_OP_WRITE_FIXED    5
#define IORING_OP_POLL_ADD       6
#define IORING_OP_POLL_REMOVE    7
#define IORING_OP_ACCEPT         13
#define IORING_OP_ASYNC_CANCEL   14
#define IORING_OP_RECV           27
#define IORING_OP_SEND           26
#define IORING_OP_CLOSE          19
#define IORING_OP_SEND_ZC        53
#define IORING_OP_RECV_MULTISHOT 58 // Fake - we use RECV with flag

// Register opcodes
#define IORING_REGISTER_BUFFERS      0
#define IORING_REGISTER_FILES        2
#define IORING_UNREGISTER_FILES      3
#define IORING_REGISTER_FILES_UPDATE 6

// Accept flags
#define IORING_ACCEPT_MULTISHOT (1U << 0)

// Recv flags
#define IORING_RECV_MULTISHOT (1U << 1)

// Send ZC flags
#define IORING_SEND_ZC_REPORT_USAGE (1U << 3)

// File index for direct descriptors
#define IORING_FILE_INDEX_ALLOC (~0U)

// =============================================================================
// io_uring structures (must match kernel exactly)
// =============================================================================

struct io_uring_sqe {
    u8 opcode;
    u8 flags;
    u16 ioprio;
    i32 fd;
    union {
        u64 off;
        u64 addr2;
        struct {
            u32 cmd_op;
            u32 __pad1;
        };
    };
    union {
        u64 addr;
        u64 splice_off_in;
    };
    u32 len;
    union {
        u32 rw_flags;
        u32 fsync_flags;
        u32 poll_events;
        u32 poll32_events;
        u32 sync_range_flags;
        u32 msg_flags;
        u32 timeout_flags;
        u32 accept_flags;
        u32 cancel_flags;
        u32 open_flags;
        u32 statx_flags;
        u32 fadvise_advice;
        u32 splice_flags;
        u32 rename_flags;
        u32 unlink_flags;
        u32 hardlink_flags;
        u32 xattr_flags;
        u32 uring_cmd_flags;
    };
    u64 user_data;
    union {
        u16 buf_index;
        u16 buf_group;
    };
    u16 personality;
    union {
        i32 splice_fd_in;
        u32 file_index;
        struct {
            u16 addr_len;
            u16 __pad3[1];
        };
    };
    union {
        struct {
            u64 addr3;
            u64 __pad2[1];
        };
// NOLINTBEGIN(modernize-avoid-c-arrays)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wzero-length-array"
        u8 cmd[0]; // Matches kernel struct
#pragma clang diagnostic pop
        // NOLINTEND(modernize-avoid-c-arrays)
    };
};

STATIC_ASSERT(sizeof(struct io_uring_sqe) == 64, "io_uring_sqe must be 64 bytes");

struct io_uring_cqe {
    u64 user_data;
    i32 res;
    u32 flags;
};

STATIC_ASSERT(sizeof(struct io_uring_cqe) == 16, "io_uring_cqe must be 16 bytes");

struct io_sqring_offsets {
    u32 head;
    u32 tail;
    u32 ring_mask;
    u32 ring_entries;
    u32 flags;
    u32 dropped;
    u32 array;
    u32 resv1;
    u64 user_addr;
};

struct io_cqring_offsets {
    u32 head;
    u32 tail;
    u32 ring_mask;
    u32 ring_entries;
    u32 overflow;
    u32 cqes;
    u32 flags;
    u32 resv1;
    u64 user_addr;
};

struct io_uring_params {
    u32 sq_entries;
    u32 cq_entries;
    u32 flags;
    u32 sq_thread_cpu;
    u32 sq_thread_idle;
    u32 features;
    u32 wq_fd;
    u32 resv[3];
    struct io_sqring_offsets sq_off;
    struct io_cqring_offsets cq_off;
};

// =============================================================================
// Ring wrapper structure
// =============================================================================

struct ring {
    i32 fd;
    bool sqpoll; // Using SQPOLL mode

    // Submission queue
    u32 *sq_head;
    u32 *sq_tail;
    u32 *sq_flags; // For SQPOLL: check IORING_SQ_NEED_WAKEUP
    u32 *sq_array;
    u32 sq_mask;
    u32 sq_entries;
    struct io_uring_sqe *sqes;

    // Completion queue
    u32 *cq_head;
    u32 *cq_tail;
    u32 cq_mask;
    u32 cq_entries;
    u32 cq_head_local; // Local head for batched advance (no store-release until flush)
    u32 cq_tail_local; // Local tail snapshot (loaded once per batch with acquire)
    struct io_uring_cqe *cqes;

    // Memory mappings (for cleanup)
    void *sq_ring_ptr;
    usize sq_ring_sz;
    void *cq_ring_ptr;
    usize cq_ring_sz;
    void *sqes_ptr;
    usize sqes_sz;
};

// =============================================================================
// Memory barriers (x86_64 is strongly ordered, but we need compiler barriers)
// =============================================================================

#define io_uring_smp_store_release(p, v) __atomic_store_n(p, v, __ATOMIC_RELEASE)

#define io_uring_smp_load_acquire(p) __atomic_load_n(p, __ATOMIC_ACQUIRE)

// =============================================================================
// Ring operations
// =============================================================================

// Initialize the ring (returns 0 on success, -errno on failure)
// Initialize ring. If use_sqpoll is true, kernel thread polls SQ (no syscalls for submit).
// sq_thread_idle_ms: milliseconds before SQPOLL thread goes idle (0 = kernel default ~1000ms)
i32 ring_init(struct ring *r, u32 entries, bool use_sqpoll, u32 sq_thread_idle_ms);

// Cleanup the ring
void ring_cleanup(struct ring *r);

// Get next SQE (returns NULL if SQ is full)
INLINE struct io_uring_sqe *ring_get_sqe(struct ring *r) {
    u32 head = io_uring_smp_load_acquire(r->sq_head);
    u32 tail = *r->sq_tail;
    u32 next = tail + 1;

    if (unlikely(next - head > r->sq_entries)) {
        return NULL; // SQ is full
    }

    struct io_uring_sqe *sqe = &r->sqes[tail & r->sq_mask];

    // Zero the SQE (important for unions)
    for (usize i = 0; i < sizeof(*sqe) / sizeof(u64); i++) {
        ((u64 *)sqe)[i] = 0;
    }

    return sqe;
}

// Submit SQE (call after filling it in)
INLINE void ring_submit_sqe(struct ring *r) {
    u32 tail                       = *r->sq_tail;
    r->sq_array[tail & r->sq_mask] = tail & r->sq_mask;
    io_uring_smp_store_release(r->sq_tail, tail + 1);
}

// Get number of pending SQEs
INLINE u32 ring_sq_pending(struct ring *r) {
    return *r->sq_tail - io_uring_smp_load_acquire(r->sq_head);
}

// Check if SQPOLL kernel thread needs wakeup
INLINE bool ring_sq_need_wakeup(struct ring *r) {
    if (!r->sqpoll || !r->sq_flags) {
        return false;
    }
    return (io_uring_smp_load_acquire(r->sq_flags) & IORING_SQ_NEED_WAKEUP) != 0;
}

// Submit pending SQEs and optionally wait for completions
// With SQPOLL: kernel polls SQ, only syscall needed for wakeup or waiting
// Without SQPOLL: syscall needed for every submit
INLINE i32 ring_submit(struct ring *r, u32 wait_nr) {
    u32 submitted = ring_sq_pending(r);

    if (r->sqpoll) {
        // SQPOLL: kernel thread picks up SQEs automatically
        // Only syscall if we need to wake it up or wait for completions
        u32 flags = 0;
        if (ring_sq_need_wakeup(r)) {
            flags |= IORING_ENTER_SQ_WAKEUP;
        }
        if (wait_nr > 0) {
            flags |= IORING_ENTER_GETEVENTS;
        }
        if (flags == 0) {
            return (i32)submitted; // No syscall needed
        }
        return sys_io_uring_enter(r->fd, 0, wait_nr, flags, NULL);
    }

    // Non-SQPOLL: must syscall to submit
    if (submitted == 0 && wait_nr == 0) {
        return 0; // Nothing to do
    }
    u32 flags = wait_nr ? IORING_ENTER_GETEVENTS : 0;
    return sys_io_uring_enter(r->fd, submitted, wait_nr, flags, NULL);
}

// Submit and wait for at least min_complete CQEs
INLINE i32 ring_submit_and_wait(struct ring *r, u32 min_complete) {
    u32 submitted = ring_sq_pending(r);
    u32 flags     = IORING_ENTER_GETEVENTS;
    if (r->sqpoll && ring_sq_need_wakeup(r)) {
        flags |= IORING_ENTER_SQ_WAKEUP;
    }
    return sys_io_uring_enter(r->fd, r->sqpoll ? 0 : submitted, min_complete, flags, NULL);
}

// Sync local head/tail with kernel (call at start of CQE processing batch)
// Single acquire fence for entire batch - no atomics in processing loop
INLINE void ring_cq_sync(struct ring *r) {
    r->cq_head_local = *r->cq_head;
    r->cq_tail_local = io_uring_smp_load_acquire(r->cq_tail);
}

// Peek at next CQE using local head/tail (no memory barriers, fast)
INLINE struct io_uring_cqe *ring_peek_cqe(struct ring *r) {
    if (r->cq_head_local == r->cq_tail_local) {
        return NULL;
    }
    return &r->cqes[r->cq_head_local & r->cq_mask];
}

// Advance local CQ head (no store-release, just bump counter)
INLINE void ring_cq_advance(struct ring *r, u32 nr) {
    r->cq_head_local += nr;
}

// Flush local head to kernel (single store-release for entire batch)
INLINE void ring_cq_flush(struct ring *r) {
    if (r->cq_head_local != *r->cq_head) {
        io_uring_smp_store_release(r->cq_head, r->cq_head_local);
    }
}

// =============================================================================
// SQE helpers for common operations
// =============================================================================

INLINE void ring_prep_accept(struct io_uring_sqe *sqe, i32 fd, void *addr, u32 *addrlen,
                             u32 flags) {
    sqe->opcode       = IORING_OP_ACCEPT;
    sqe->fd           = fd;
    sqe->addr         = (u64)addr;
    sqe->addr2        = (u64)addrlen;
    sqe->accept_flags = flags;
}

INLINE void ring_prep_accept_direct(struct io_uring_sqe *sqe, i32 fd, void *addr, u32 *addrlen,
                                    u32 flags) {
    ring_prep_accept(sqe, fd, addr, addrlen, flags);
    sqe->file_index = IORING_FILE_INDEX_ALLOC;
}

// Multishot accept without registered files (simpler, works everywhere)
INLINE void ring_prep_accept_multishot(struct io_uring_sqe *sqe, i32 fd, void *addr, u32 *addrlen) {
    ring_prep_accept(sqe, fd, addr, addrlen, IORING_ACCEPT_MULTISHOT);
}

INLINE void ring_prep_recv(struct io_uring_sqe *sqe, i32 fd, void *buf, u32 len, u32 flags) {
    sqe->opcode    = IORING_OP_RECV;
    sqe->fd        = fd;
    sqe->addr      = (u64)buf;
    sqe->len       = len;
    sqe->msg_flags = flags;
}

INLINE void ring_prep_send(struct io_uring_sqe *sqe, i32 fd, const void *buf, u32 len, u32 flags) {
    sqe->opcode    = IORING_OP_SEND;
    sqe->fd        = fd;
    sqe->addr      = (u64)buf;
    sqe->len       = len;
    sqe->msg_flags = flags;
}

INLINE void ring_prep_send_zc(struct io_uring_sqe *sqe, i32 fd, const void *buf, u32 len,
                              u32 flags) {
    sqe->opcode    = IORING_OP_SEND_ZC;
    sqe->fd        = fd;
    sqe->addr      = (u64)buf;
    sqe->len       = len;
    sqe->msg_flags = flags;
}

INLINE void ring_prep_writev(struct io_uring_sqe *sqe, i32 fd, const struct iovec *iov, u32 nr_vecs,
                             u32 flags) {
    sqe->opcode    = IORING_OP_WRITEV;
    sqe->fd        = fd;
    sqe->addr      = (u64)iov;
    sqe->len       = nr_vecs;
    sqe->msg_flags = flags;
}

INLINE void ring_prep_close(struct io_uring_sqe *sqe, i32 fd) {
    sqe->opcode = IORING_OP_CLOSE;
    sqe->fd     = fd;
}

INLINE void ring_prep_close_direct(struct io_uring_sqe *sqe, u32 file_index) {
    sqe->opcode     = IORING_OP_CLOSE;
    sqe->fd         = 0;
    sqe->file_index = file_index;
}

#endif // SYS_IO_URING_H
