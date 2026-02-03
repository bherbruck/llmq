// main.c - Entry point for the MQTT broker
// No libc - we provide our own _start

#include "sys/types.h"
#include "sys/syscall.h"
#include "sys/io_uring.h"
#include "mem/string.h"
#include "mqtt/packet.h"
#include "mqtt/connect.h"

// =============================================================================
// Configuration
// =============================================================================

#define LISTEN_PORT    1883
#define LISTEN_BACKLOG 128
#define RING_ENTRIES   256
#define MAX_CONNS      1024
#define RECV_BUF_SIZE  4096 // Per-connection receive buffer

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
// Connection State
// =============================================================================

enum conn_state {
    CONN_FREE = 0,  // Slot available
    CONN_ACCEPTED,  // TCP connected, awaiting CONNECT packet
    CONN_CONNECTED, // MQTT connected, normal operation
    CONN_CLOSING,   // Close in progress
};

struct connection {
    enum conn_state state;
    i32 fd;

    // Receive buffer (owned by connection)
    u8 recv_buf[RECV_BUF_SIZE];
    u32 recv_len; // Data in buffer

    // MQTT session state
    u16 keepalive; // Keepalive interval (seconds, 0=disabled)
    bool clean_session;

    // Client ID (points into recv_buf during CONNECT, copied after)
    char client_id[64];
    u8 client_id_len;
};

// =============================================================================
// Broker state
// =============================================================================

struct broker {
    struct ring ring;
    i32 listen_fd;
    volatile bool running;

    // Connection table (indexed by fd for O(1) lookup)
    struct connection conns[MAX_CONNS];

    // Stats
    u64 accepts;
    u64 bytes_recv;
    u64 bytes_sent;
};

// =============================================================================
// Connection Management
// =============================================================================

static struct connection *conn_get(struct broker *b, i32 fd) {
    if (fd < 0 || fd >= MAX_CONNS) {
        return NULL;
    }
    return &b->conns[fd];
}

static struct connection *conn_alloc(struct broker *b, i32 fd) {
    struct connection *c = conn_get(b, fd);
    if (!c) {
        return NULL;
    }
    memset(c, 0, sizeof(*c));
    c->fd    = fd;
    c->state = CONN_ACCEPTED;
    return c;
}

static void conn_free(struct broker *b, i32 fd) {
    struct connection *c = conn_get(b, fd);
    if (c) {
        c->state = CONN_FREE;
        c->fd    = -1;
    }
}

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

static void submit_recv(struct broker *b, struct connection *c) {
    struct io_uring_sqe *sqe = ring_get_sqe(&b->ring);
    if (!sqe) {
        println("SQ full, can't submit recv");
        return;
    }

    // Receive into buffer at current position
    u8 *buf   = c->recv_buf + c->recv_len;
    u32 space = RECV_BUF_SIZE - c->recv_len;

    ring_prep_recv(sqe, c->fd, buf, space, 0);
    sqe->user_data = make_user_data(OP_RECV, (u32)c->fd, 0);
    ring_submit_sqe(&b->ring);
}

static void submit_send(struct broker *b, i32 fd, const u8 *buf, u32 len) {
    struct io_uring_sqe *sqe = ring_get_sqe(&b->ring);
    if (!sqe) {
        println("SQ full, can't submit send");
        return;
    }

    ring_prep_send(sqe, fd, buf, len, 0);
    sqe->user_data = make_user_data(OP_SEND, (u32)fd, 0);
    ring_submit_sqe(&b->ring);
}

static void submit_close(struct broker *b, i32 fd) {
    struct io_uring_sqe *sqe = ring_get_sqe(&b->ring);
    if (!sqe) {
        println("SQ full, can't submit close");
        return;
    }

    ring_prep_close(sqe, fd);
    sqe->user_data = make_user_data(OP_CLOSE, (u32)fd, 0);
    ring_submit_sqe(&b->ring);
}

static void handle_accept(struct broker *b, struct io_uring_cqe *cqe) {
    i32 client_fd = cqe->res;

    if (client_fd < 0) {
        print("accept() failed: ");
        print_num(client_fd);
        println("");
        submit_accept(b);
        return;
    }

    // Check fd is in range
    if (client_fd >= MAX_CONNS) {
        print("fd too large: ");
        print_num(client_fd);
        println("");
        sys_close(client_fd);
        submit_accept(b);
        return;
    }

    b->accepts++;

    // Allocate connection state
    struct connection *c = conn_alloc(b, client_fd);
    if (!c) {
        sys_close(client_fd);
        submit_accept(b);
        return;
    }

    print("Accepted fd=");
    print_num(client_fd);
    println("");

    // Start receiving MQTT CONNECT packet
    submit_recv(b, c);

    // Re-arm accept
    submit_accept(b);
}

// =============================================================================
// MQTT Packet Processing
// =============================================================================

// Process a complete MQTT packet
// Returns: bytes consumed, or -1 on error (close connection)
static i32 process_mqtt_packet(struct broker *b, struct connection *c, const u8 *buf, u32 len) {
    struct mqtt_fixed_header hdr;
    i32 hdr_len = mqtt_parse_fixed_header(buf, len, &hdr);
    if (hdr_len < 0) {
        return hdr_len;
    }

    u32 total_len = (u32)hdr_len + hdr.remaining_len;

    // Variable header starts after fixed header
    const u8 *var_hdr = buf + hdr_len;
    u32 var_len       = hdr.remaining_len;

    switch (hdr.type) {
    case MQTT_CONNECT: {
        if (c->state != CONN_ACCEPTED) {
            // Protocol error: CONNECT must be first packet
            return -1;
        }

        struct mqtt_connect conn_pkt;
        i32 rc = mqtt_parse_connect(var_hdr, var_len, &conn_pkt);
        if (rc < 0) {
            // Send CONNACK with error
            u8 connack[MQTT_CONNACK_SIZE];
            u8 reason =
                (rc == MQTT_ERR_PROTOCOL) ? MQTT_CONNACK_PROTO_VERSION : MQTT_CONNACK_ID_REJECTED;
            mqtt_encode_connack(connack, false, reason);
            submit_send(b, c->fd, connack, MQTT_CONNACK_SIZE);
            return -1; // Close after send
        }

        // Store session info
        c->keepalive     = conn_pkt.keepalive;
        c->clean_session = conn_pkt.clean_session;

        // Copy client ID (it points into recv buffer which will be reused)
        u8 id_len = conn_pkt.client_id.len;
        if (id_len > sizeof(c->client_id) - 1) {
            id_len = sizeof(c->client_id) - 1;
        }
        memcpy(c->client_id, conn_pkt.client_id.ptr, id_len);
        c->client_id[id_len] = '\0';
        c->client_id_len     = id_len;

        c->state = CONN_CONNECTED;

        print("CONNECT from '");
        sys_write(2, c->client_id, c->client_id_len);
        print("' keepalive=");
        print_num(c->keepalive);
        println("");

        // Send CONNACK success
        u8 connack[MQTT_CONNACK_SIZE];
        mqtt_encode_connack(connack, false, MQTT_CONNACK_ACCEPTED);
        submit_send(b, c->fd, connack, MQTT_CONNACK_SIZE);
        break;
    }

    case MQTT_PINGREQ: {
        if (c->state != CONN_CONNECTED) {
            return -1;
        }
        // Send PINGRESP (2 bytes: type + remaining length 0)
        u8 pingresp[2] = {MQTT_PINGRESP << 4, 0};
        submit_send(b, c->fd, pingresp, 2);
        break;
    }

    case MQTT_DISCONNECT: {
        // Clean disconnect
        print("DISCONNECT from fd=");
        print_num(c->fd);
        println("");
        return -1; // Signal to close
    }

    case MQTT_PUBLISH:
    case MQTT_SUBSCRIBE:
    case MQTT_UNSUBSCRIBE:
        // Not yet implemented
        print("Unhandled packet type: ");
        print_num(hdr.type);
        println("");
        break;

    default:
        print("Unknown packet type: ");
        print_num(hdr.type);
        println("");
        return -1;
    }

    return (i32)total_len;
}

// Process all complete packets in connection's receive buffer
static void process_recv_buffer(struct broker *b, struct connection *c) {
    while (c->recv_len > 0) {
        i32 packet_len = mqtt_packet_complete(c->recv_buf, c->recv_len);

        if (packet_len == MQTT_INCOMPLETE) {
            // Need more data
            break;
        }

        if (packet_len < 0) {
            // Malformed packet - close connection
            print("Malformed packet from fd=");
            print_num(c->fd);
            println("");
            c->state = CONN_CLOSING;
            submit_close(b, c->fd);
            return;
        }

        // Process the complete packet
        i32 consumed = process_mqtt_packet(b, c, c->recv_buf, (u32)packet_len);

        if (consumed < 0) {
            // Error or disconnect - close connection
            c->state = CONN_CLOSING;
            submit_close(b, c->fd);
            return;
        }

        // Remove processed bytes from buffer
        u32 remaining = c->recv_len - (u32)consumed;
        if (remaining > 0) {
            memmove(c->recv_buf, c->recv_buf + consumed, remaining);
        }
        c->recv_len = remaining;
    }
}

// =============================================================================
// CQE Handlers
// =============================================================================

static void handle_recv(struct broker *b, struct io_uring_cqe *cqe) {
    i32 fd               = (i32)ud_fd(cqe->user_data);
    struct connection *c = conn_get(b, fd);

    if (!c || c->state == CONN_FREE || c->state == CONN_CLOSING) {
        return;
    }

    if (cqe->res <= 0) {
        // Error or EOF
        if (cqe->res < 0) {
            print("recv error fd=");
            print_num(fd);
            print(" err=");
            print_num(cqe->res);
            println("");
        }
        c->state = CONN_CLOSING;
        submit_close(b, fd);
        return;
    }

    // Update buffer length
    c->recv_len += (u32)cqe->res;
    b->bytes_recv += (u64)cqe->res;

    // Process complete packets
    process_recv_buffer(b, c);

    // If still connected, submit another recv
    if (c->state == CONN_ACCEPTED || c->state == CONN_CONNECTED) {
        submit_recv(b, c);
    }
}

static void handle_send(struct broker *b, struct io_uring_cqe *cqe) {
    i32 fd = (i32)ud_fd(cqe->user_data);
    (void)b;

    if (cqe->res < 0) {
        print("send error fd=");
        print_num(fd);
        print(" err=");
        print_num(cqe->res);
        println("");
    } else {
        b->bytes_sent += (u64)cqe->res;
    }
}

static void handle_close(struct broker *b, struct io_uring_cqe *cqe) {
    i32 fd = (i32)ud_fd(cqe->user_data);

    if (cqe->res < 0) {
        print("close error fd=");
        print_num(fd);
        print(" err=");
        print_num(cqe->res);
        println("");
    }

    conn_free(b, fd);
}

static void dispatch_cqe(struct broker *b, struct io_uring_cqe *cqe) {
    u8 op = ud_op(cqe->user_data);

    switch (op) {
    case OP_ACCEPT:
        handle_accept(b, cqe);
        break;
    case OP_RECV:
        handle_recv(b, cqe);
        break;
    case OP_SEND:
        handle_send(b, cqe);
        break;
    case OP_CLOSE:
        handle_close(b, cqe);
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
