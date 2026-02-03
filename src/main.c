// main.c - Entry point for the MQTT broker
// No libc - we provide our own _start

#include "sys/types.h"
#include "sys/syscall.h"
#include "sys/io_uring.h"
#include "mem/string.h"

// =============================================================================
// Configuration
// =============================================================================

#define LISTEN_PORT    1883
#define LISTEN_BACKLOG 128
#define RING_ENTRIES   256

// =============================================================================
// Simple output (writes to stderr)
// =============================================================================

static void print(const char *msg) {
    sys_write(2, msg, strlen(msg));
}

static void println(const char *msg) {
    print(msg);
    print("\n");
}

static void print_num(i64 n) {
    char buf[32];
    char *p = buf + sizeof(buf) - 1;
    *p      = '\0';

    bool neg = n < 0;
    if (neg) {
        n = -n;
    }

    do {
        *--p = (char)('0' + (n % 10));
        n /= 10;
    } while (n);

    if (neg) {
        *--p = '-';
    }

    print(p);
}

// =============================================================================
// Network structures (must match kernel)
// =============================================================================

struct sockaddr_in {
    u16 sin_family;
    u16 sin_port;
    u32 sin_addr;
    u8 sin_zero[8];
};

INLINE u16 htons(u16 x) {
    return (u16)((x >> 8) | (x << 8));
}

// =============================================================================
// User data encoding for CQE dispatch
// =============================================================================

enum op_type {
    OP_ACCEPT = 1,
    OP_RECV   = 2,
    OP_SEND   = 3,
    OP_CLOSE  = 4,
};

// Pack: [8-bit op][24-bit fd][32-bit context]
INLINE u64 make_user_data(u8 op, u32 fd, u32 ctx) {
    return ((u64)op << 56) | ((u64)(fd & 0xFFFFFF) << 32) | (u64)ctx;
}

INLINE u8 ud_op(u64 ud) {
    return (u8)(ud >> 56);
}
INLINE u32 ud_fd(u64 ud) {
    return (u32)((ud >> 32) & 0xFFFFFF);
}
INLINE u32 ud_ctx(u64 ud) {
    return (u32)(ud & 0xFFFFFFFF);
}

// =============================================================================
// Broker state
// =============================================================================

struct broker {
    struct ring ring;
    i32 listen_fd;
    volatile bool running;

    // Stats
    u64 accepts;
    u64 bytes_recv;
    u64 bytes_sent;
};

// =============================================================================
// Socket setup
// =============================================================================

static i32 create_listen_socket(u16 port) {
    i32 fd = sys_socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        print("socket() failed: ");
        print_num(fd);
        println("");
        return fd;
    }

    // SO_REUSEADDR
    i32 optval = 1;
    i32 rc     = sys_setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
    if (rc < 0) {
        print("setsockopt(SO_REUSEADDR) failed: ");
        print_num(rc);
        println("");
        sys_close(fd);
        return rc;
    }

    // SO_REUSEPORT
    rc = sys_setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval));
    if (rc < 0) {
        print("setsockopt(SO_REUSEPORT) failed: ");
        print_num(rc);
        println("");
        // Non-fatal, continue
    }

    // Bind
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    addr.sin_addr   = 0; // INADDR_ANY

    rc = sys_bind(fd, &addr, sizeof(addr));
    if (rc < 0) {
        print("bind() failed: ");
        print_num(rc);
        println("");
        sys_close(fd);
        return rc;
    }

    // Listen
    rc = sys_listen(fd, LISTEN_BACKLOG);
    if (rc < 0) {
        print("listen() failed: ");
        print_num(rc);
        println("");
        sys_close(fd);
        return rc;
    }

    return fd;
}

// =============================================================================
// Event handling
// =============================================================================

static void submit_accept(struct broker *b) {
    struct io_uring_sqe *sqe = ring_get_sqe(&b->ring);
    if (!sqe) {
        println("SQ full, can't submit accept");
        return;
    }

    ring_prep_accept(sqe, b->listen_fd, NULL, NULL, 0);
    sqe->user_data = make_user_data(OP_ACCEPT, 0, 0);
    ring_submit_sqe(&b->ring);
}

static void handle_accept(struct broker *b, struct io_uring_cqe *cqe) {
    i32 client_fd = cqe->res;

    if (client_fd < 0) {
        print("accept() failed: ");
        print_num(client_fd);
        println("");
        // Re-arm accept
        submit_accept(b);
        return;
    }

    b->accepts++;
    print("Accepted connection fd=");
    print_num(client_fd);
    println("");

    // For now, just close immediately (placeholder for connection handling)
    struct io_uring_sqe *sqe = ring_get_sqe(&b->ring);
    if (sqe) {
        ring_prep_close(sqe, client_fd);
        sqe->user_data = make_user_data(OP_CLOSE, (u32)client_fd, 0);
        ring_submit_sqe(&b->ring);
    }

    // Re-arm accept
    submit_accept(b);
}

static void handle_close(struct broker *b, struct io_uring_cqe *cqe) {
    (void)b;
    u32 fd = ud_fd(cqe->user_data);
    if (cqe->res < 0) {
        print("close(fd=");
        print_num(fd);
        print(") failed: ");
        print_num(cqe->res);
        println("");
    }
}

static void dispatch_cqe(struct broker *b, struct io_uring_cqe *cqe) {
    u8 op = ud_op(cqe->user_data);

    switch (op) {
    case OP_ACCEPT:
        handle_accept(b, cqe);
        break;
    case OP_CLOSE:
        handle_close(b, cqe);
        break;
    case OP_RECV:
    case OP_SEND:
        // TODO: implement
        break;
    default:
        print("Unknown op: ");
        print_num(op);
        println("");
        break;
    }
}

// =============================================================================
// Main event loop
// =============================================================================

static i32 run(struct broker *b) {
    b->running = true;

    // Arm initial accept
    submit_accept(b);

    println("Entering event loop...");

    while (b->running) {
        // Submit pending SQEs and wait for at least 1 completion
        i32 rc = ring_submit(&b->ring, 1);
        if (rc < 0) {
            print("io_uring_enter failed: ");
            print_num(rc);
            println("");
            return rc;
        }

        // Process all available CQEs
        struct io_uring_cqe *cqe;
        while ((cqe = ring_peek_cqe(&b->ring)) != NULL) {
            dispatch_cqe(b, cqe);
            ring_cq_advance(&b->ring, 1);
        }
    }

    return 0;
}

// =============================================================================
// Entry point (called by _start)
// =============================================================================

i32 broker_main(void) {
    println("llmq - io_uring MQTT broker");
    println("===========================");

    struct broker b;
    memset(&b, 0, sizeof(b));

    // Initialize io_uring
    print("Initializing io_uring with ");
    print_num(RING_ENTRIES);
    println(" entries...");

    i32 rc = ring_init(&b.ring, RING_ENTRIES);
    if (rc < 0) {
        print("ring_init failed: ");
        print_num(rc);
        println("");
        return 1;
    }
    println("io_uring initialized");

    // Create listening socket
    print("Binding to port ");
    print_num(LISTEN_PORT);
    println("...");

    b.listen_fd = create_listen_socket(LISTEN_PORT);
    if (b.listen_fd < 0) {
        ring_cleanup(&b.ring);
        return 1;
    }

    print("Listening on port ");
    print_num(LISTEN_PORT);
    println("");

    // Run event loop
    rc = run(&b);

    // Cleanup
    sys_close(b.listen_fd);
    ring_cleanup(&b.ring);

    print("Total accepts: ");
    print_num((i64)b.accepts);
    println("");

    return rc < 0 ? 1 : 0;
}

// =============================================================================
// _start - Entry point (no libc)
// =============================================================================

// We write _start in assembly to guarantee proper stack alignment.
// The kernel starts us with RSP 16-byte aligned pointing to argc.
// Before calling broker_main, we need RSP to be 16-byte aligned BEFORE
// the call (which will push a return address making it 8 mod 16 inside the callee).
// Since we start 16-byte aligned, we need to adjust RSP to be 16-byte aligned
// before calling. The "and $-16" ensures this.
__asm__(".global _start\n"
        "_start:\n"
        "    xor %rbp, %rbp\n"   // Clear frame pointer (ABI requirement)
        "    and $-16, %rsp\n"   // Ensure 16-byte stack alignment
        "    call broker_main\n" // Call main function
        "    mov %eax, %edi\n"   // Exit code from broker_main
        "    mov $60, %eax\n"    // SYS_exit
        "    syscall\n"          // Exit
);
