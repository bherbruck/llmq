# Section 5: io_uring Integration

## 5.1 Overview

io_uring is the sole I/O interface for the broker. All network operations (accept, recv, send, close) are submitted as SQEs and their results retrieved as CQEs.

## 5.2 Ring Setup

### 5.2.1 Parameters

The ring MUST be created with the following flags:

```c
struct io_uring_params params = {
    .flags = IORING_SETUP_SINGLE_ISSUER |    // Single thread submits
             IORING_SETUP_COOP_TASKRUN |     // Cooperative completion
             IORING_SETUP_DEFER_TASKRUN,     // Defer completion processing
    .sq_entries = SQ_SIZE,                    // Typically 4096
    .cq_entries = CQ_SIZE,                    // Typically 4× SQ size
};
```

**Flag rationale:**

| Flag | Purpose | Benefit |
|------|---------|---------|
| `SINGLE_ISSUER` | Promise only one thread submits | Kernel skips per-submission locking |
| `COOP_TASKRUN` | Completions run in our context | No async interrupt delivery |
| `DEFER_TASKRUN` | Defer completions to io_uring_enter | Predictable completion timing |

### 5.2.2 Initialization Sequence

```c
int ring_init(struct ring *r, u32 sq_entries) {
    struct io_uring_params params = {0};
    params.flags = IORING_SETUP_SINGLE_ISSUER |
                   IORING_SETUP_COOP_TASKRUN |
                   IORING_SETUP_DEFER_TASKRUN;
    
    // 1. Create ring
    r->fd = syscall2(SYS_io_uring_setup, sq_entries, (long)&params);
    if (r->fd < 0) return r->fd;
    
    // 2. mmap SQ ring
    usize sq_ring_size = params.sq_off.array + params.sq_entries * sizeof(u32);
    void *sq_ptr = mmap(NULL, sq_ring_size, PROT_READ | PROT_WRITE,
                        MAP_SHARED | MAP_POPULATE, r->fd, IORING_OFF_SQ_RING);
    
    // 3. mmap CQ ring (may be same mapping on newer kernels)
    usize cq_ring_size = params.cq_off.cqes + params.cq_entries * sizeof(struct io_uring_cqe);
    void *cq_ptr = mmap(NULL, cq_ring_size, PROT_READ | PROT_WRITE,
                        MAP_SHARED | MAP_POPULATE, r->fd, IORING_OFF_CQ_RING);
    
    // 4. mmap SQEs array
    usize sqes_size = params.sq_entries * sizeof(struct io_uring_sqe);
    r->sqes = mmap(NULL, sqes_size, PROT_READ | PROT_WRITE,
                   MAP_SHARED | MAP_POPULATE, r->fd, IORING_OFF_SQES);
    
    // 5. Set up pointers to ring heads/tails
    r->sq_head = sq_ptr + params.sq_off.head;
    r->sq_tail = sq_ptr + params.sq_off.tail;
    r->sq_mask = *((u32*)(sq_ptr + params.sq_off.ring_mask));
    r->sq_array = sq_ptr + params.sq_off.array;
    
    r->cq_head = cq_ptr + params.cq_off.head;
    r->cq_tail = cq_ptr + params.cq_off.tail;
    r->cq_mask = *((u32*)(cq_ptr + params.cq_off.ring_mask));
    r->cqes = cq_ptr + params.cq_off.cqes;
    
    return 0;
}
```

## 5.3 Resource Registration

### 5.3.1 Buffer Registration

Registering buffers allows zero-copy operations:

```c
struct iovec iovs[MAX_BUFS];
for (u32 i = 0; i < MAX_BUFS; i++) {
    iovs[i].iov_base = buf_ptr(&pool, i);
    iovs[i].iov_len = BUFFER_SIZE;
}

int ret = syscall4(SYS_io_uring_register, ring_fd,
                   IORING_REGISTER_BUFFERS, (long)iovs, MAX_BUFS);
```

After registration, operations can reference buffers by index rather than pointer, and the kernel can DMA directly without copying.

### 5.3.2 File (Direct Descriptor) Registration

```c
// Pre-allocate sparse file table
int ret = syscall4(SYS_io_uring_register, ring_fd,
                   IORING_REGISTER_FILES, NULL, MAX_CONNS);
```

New connections are accepted directly into this table via `IORING_OP_ACCEPT` with `IOSQE_FIXED_FILE` and `file_index` set.

## 5.4 Operations

### 5.4.1 user_data Encoding

Each SQE's `user_data` field encodes operation context for the CQE handler:

```c
// Encoding: [8-bit op][24-bit fd/slot][32-bit context]
#define MAKE_USER_DATA(op, fd, ctx) \
    (((u64)(op) << 56) | ((u64)(fd) << 32) | (u64)(ctx))

#define USER_DATA_OP(ud)  ((u8)((ud) >> 56))
#define USER_DATA_FD(ud)  ((u32)(((ud) >> 32) & 0xFFFFFF))
#define USER_DATA_CTX(ud) ((u32)((ud) & 0xFFFFFFFF))

// Operation codes
enum {
    OP_ACCEPT = 1,
    OP_RECV   = 2,
    OP_SEND   = 3,
    OP_CLOSE  = 4,
};
```

### 5.4.2 Accept (Multishot)

Multishot accept produces one CQE per accepted connection:

```c
void submit_accept(struct ring *r, int listen_fd) {
    struct io_uring_sqe *sqe = get_sqe(r);
    
    sqe->opcode = IORING_OP_ACCEPT;
    sqe->fd = listen_fd;
    sqe->addr = 0;                    // Don't need client address
    sqe->addr2 = 0;
    sqe->accept_flags = 0;
    sqe->ioprio = IORING_ACCEPT_MULTISHOT;
    sqe->file_index = IORING_FILE_INDEX_ALLOC;  // Allocate direct descriptor
    sqe->user_data = MAKE_USER_DATA(OP_ACCEPT, 0, 0);
}
```

CQE handling:

```c
void handle_accept_cqe(struct broker *b, struct io_uring_cqe *cqe) {
    if (cqe->res < 0) {
        // Accept failed, multishot terminates - resubmit
        submit_accept(&b->ring, b->listen_fd);
        return;
    }
    
    int direct_fd = cqe->res;  // Direct descriptor index
    init_connection(b, direct_fd);
    submit_recv(b, direct_fd);
    
    // Multishot continues automatically, no resubmit needed
}
```

### 5.4.3 Receive

For initial CONNECT packet, use single-shot recv:

```c
void submit_recv(struct ring *r, int direct_fd, u32 buf_idx) {
    struct io_uring_sqe *sqe = get_sqe(r);
    
    sqe->opcode = IORING_OP_RECV;
    sqe->fd = direct_fd;
    sqe->flags = IOSQE_FIXED_FILE;           // Using direct descriptor
    sqe->addr = (u64)buf_ptr(&pool, buf_idx);
    sqe->len = BUFFER_SIZE;
    sqe->buf_index = buf_idx;                 // For registered buffer ops
    sqe->user_data = MAKE_USER_DATA(OP_RECV, direct_fd, buf_idx);
}
```

After connection established, use multishot recv with provided buffers (buffer ring):

```c
void submit_recv_multishot(struct ring *r, int direct_fd, u16 buf_group) {
    struct io_uring_sqe *sqe = get_sqe(r);
    
    sqe->opcode = IORING_OP_RECV;
    sqe->fd = direct_fd;
    sqe->flags = IOSQE_FIXED_FILE | IOSQE_BUFFER_SELECT;
    sqe->ioprio = IORING_RECV_MULTISHOT;
    sqe->buf_group = buf_group;
    sqe->user_data = MAKE_USER_DATA(OP_RECV, direct_fd, 0);
}
```

### 5.4.4 Send (Zero-Copy)

Zero-copy send for publish fan-out:

```c
void submit_send_zc(struct ring *r, int direct_fd, u32 buf_idx, 
                    u32 offset, u32 len, u32 msg_ref_idx) {
    struct io_uring_sqe *sqe = get_sqe(r);
    
    sqe->opcode = IORING_OP_SEND_ZC;
    sqe->fd = direct_fd;
    sqe->flags = IOSQE_FIXED_FILE;
    sqe->addr = (u64)(buf_ptr(&pool, buf_idx) + offset);
    sqe->len = len;
    sqe->user_data = MAKE_USER_DATA(OP_SEND, direct_fd, msg_ref_idx);
}
```

**Important:** Zero-copy send may produce two CQEs:

1. First CQE: Submission notification (may have `IORING_CQE_F_MORE` flag)
2. Second CQE: Completion notification (buffer now safe to reuse)

The broker MUST wait for the final CQE (without `IORING_CQE_F_MORE`) before decrementing the refcount.

### 5.4.5 Close

```c
void submit_close(struct ring *r, int direct_fd) {
    struct io_uring_sqe *sqe = get_sqe(r);
    
    sqe->opcode = IORING_OP_CLOSE;
    sqe->fd = direct_fd;
    sqe->flags = IOSQE_FIXED_FILE;
    sqe->user_data = MAKE_USER_DATA(OP_CLOSE, direct_fd, 0);
}
```

## 5.5 Submission and Completion

### 5.5.1 Getting an SQE

```c
static inline struct io_uring_sqe *get_sqe(struct ring *r) {
    u32 tail = atomic_load(r->sq_tail);
    u32 head = atomic_load(r->sq_head);
    
    if (tail - head >= r->sq_mask + 1) {
        return NULL;  // SQ full
    }
    
    u32 idx = tail & r->sq_mask;
    r->sq_array[idx] = idx;
    
    return &r->sqes[idx];
}

static inline void submit_sqe(struct ring *r) {
    atomic_store(r->sq_tail, atomic_load(r->sq_tail) + 1);
}
```

### 5.5.2 Processing CQEs

```c
static inline struct io_uring_cqe *peek_cqe(struct ring *r) {
    u32 head = atomic_load(r->cq_head);
    u32 tail = atomic_load(r->cq_tail);
    
    if (head == tail) {
        return NULL;  // CQ empty
    }
    
    return &r->cqes[head & r->cq_mask];
}

static inline void advance_cq(struct ring *r) {
    atomic_store(r->cq_head, atomic_load(r->cq_head) + 1);
}
```

### 5.5.3 Event Loop Integration

```c
void event_loop(struct broker *b) {
    while (b->running) {
        u32 to_submit = pending_sqes(b);
        
        // Submit and wait for at least one completion
        syscall4(SYS_io_uring_enter, b->ring.fd, to_submit, 1,
                 IORING_ENTER_GETEVENTS);
        
        // Process all available completions
        struct io_uring_cqe *cqe;
        while ((cqe = peek_cqe(&b->ring)) != NULL) {
            dispatch_cqe(b, cqe);
            advance_cq(&b->ring);
        }
    }
}
```

## 5.6 Buffer Ring (Provided Buffers)

For high-connection-count scenarios, a buffer ring allows the kernel to select buffers:

```c
struct io_uring_buf_ring *setup_buf_ring(struct ring *r, u32 count, u16 bgid) {
    // Allocate buffer ring structure
    struct io_uring_buf_ring *br = mmap(NULL, count * sizeof(struct io_uring_buf),
                                         PROT_READ | PROT_WRITE,
                                         MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    
    // Register with io_uring
    struct io_uring_buf_reg reg = {
        .ring_addr = (u64)br,
        .ring_entries = count,
        .bgid = bgid,
    };
    syscall4(SYS_io_uring_register, r->fd, IORING_REGISTER_PBUF_RING, (long)&reg, 1);
    
    return br;
}

void buf_ring_add(struct io_uring_buf_ring *br, void *addr, u32 len, 
                  u16 bid, u32 mask, u32 idx) {
    struct io_uring_buf *buf = &br->bufs[idx & mask];
    buf->addr = (u64)addr;
    buf->len = len;
    buf->bid = bid;
}

void buf_ring_advance(struct io_uring_buf_ring *br, u32 count) {
    // Publish new buffers to kernel
    atomic_store(&br->tail, br->tail + count);
}
```

When a multishot recv completes, the CQE flags contain the buffer ID that was used, allowing the broker to identify which buffer received data.

## 5.7 Error Handling

### 5.7.1 CQE Result Codes

| Result | Meaning | Action |
|--------|---------|--------|
| > 0 | Success (bytes transferred) | Process data |
| 0 | EOF / Connection closed | Close connection |
| -EAGAIN | Would block | Retry (shouldn't happen with io_uring) |
| -ECONNRESET | Connection reset | Close connection |
| -EBADF | Bad file descriptor | Log error, cleanup slot |
| -ENOMEM | Out of memory | Fatal if persistent |

### 5.7.2 Ring Overflow

If CQEs are not consumed fast enough, the ring may overflow. The broker SHOULD:

1. Check `io_uring_params.cq_off.overflow` on each iteration
2. If overflow occurred, some CQEs were dropped
3. Close affected connections (they're in unknown state)
4. Log the overflow condition

Properly sized CQ (4× SQ entries) should prevent overflow under normal operation.
