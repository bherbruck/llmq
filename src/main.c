// main.c - Entry point for the MQTT broker
// Thin orchestrator: init, run, cleanup

#include "config.h"
#include "config.def.h"
#include "sys/types.h"
#include "sys/syscall.h"
#include "sys/io_uring.h"
#include "mem/string.h"
#include "util/log.h"
#include "broker/state.h"
#include "broker/loop.h"

// =============================================================================
// Network Setup
// =============================================================================

struct sockaddr_in {
    u16 sin_family;
    u16 sin_port;
    u32 sin_addr;
    u8 sin_zero[SOCKADDR_PADDING_SIZE];
};

INLINE u16 htons(u16 x) {
    return (u16)((x >> BITS_PER_BYTE) | (x << BITS_PER_BYTE));
}

static i32 create_listen_socket(u16 port) {
    i32 fd = sys_socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        log_error("socket() failed: %d", fd);
        return fd;
    }

    // SO_REUSEADDR: allow quick restart after clean shutdown (TIME_WAIT)
    // We intentionally do NOT set SO_REUSEPORT - only one broker per port
    i32 optval = 1;
    i32 rc     = sys_setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
    if (rc < 0) {
        log_error("setsockopt(SO_REUSEADDR) failed: %d", rc);
        sys_close(fd);
        return rc;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    addr.sin_addr   = 0;

    rc = sys_bind(fd, &addr, sizeof(addr));
    if (rc < 0) {
        if (rc == -EADDRINUSE) {
            log_error("Port %d already in use (another broker running?)", port);
        } else {
            log_error("bind() failed: %d", rc);
        }
        sys_close(fd);
        return rc;
    }

    rc = sys_listen(fd, LLMQ_LISTEN_BACKLOG);
    if (rc < 0) {
        log_error("listen() failed: %d", rc);
        sys_close(fd);
        return rc;
    }

    return fd;
}

// =============================================================================
// Signal Handling
// =============================================================================

static volatile struct broker *g_broker = NULL;

static void signal_handler(i32 sig) {
    (void)sig;
    if (g_broker) {
        ((struct broker *)g_broker)->running = false;
    }
}

static void setup_signals(struct broker *b) {
    g_broker = b;

    // Ignore SIGPIPE (broken pipe when writing to closed socket)
    struct sigaction sa_ign;
    memset(&sa_ign, 0, sizeof(sa_ign));
    sa_ign.sa_handler = SIG_IGN;
    sys_sigaction(SIGPIPE, &sa_ign, NULL);

    // Handle SIGINT/SIGTERM for graceful shutdown
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler  = signal_handler;
    sa.sa_flags    = SA_RESTORER;
    sa.sa_restorer = sigreturn_trampoline;
    sys_sigaction(SIGINT, &sa, NULL);
    sys_sigaction(SIGTERM, &sa, NULL);
}

// =============================================================================
// Entry Point
// =============================================================================

i32 broker_main(i32 argc, char **argv, char **envp) {
    // Parse command-line arguments + env vars + INI file
    struct broker_config cfg;
    i32 rc = broker_config_parse(argc, argv, envp, &cfg);
    if (rc != 0) {
        return rc < 0 ? 1 : 0; // Error or --help
    }

    // Set log level from config
    log_set_level(cfg.debug_log_level);

    log_info("llmq - io_uring MQTT broker (zero-copy)");
    log_info("Config: port=%d max_clients=%d ring=%d egress=%d msg_pool=%d",
             cfg.network_port, cfg.limits_max_conns, cfg.limits_ring_entries,
             cfg.client_egress_capacity, cfg.limits_msg_pool_size);

    // Initialize broker state
    struct broker b;
    memset(&b, 0, sizeof(b));

    // Calculate pool sizes
    u32 msg_pool_size       = cfg.limits_msg_pool_size;
    u16 egress_capacity     = cfg.client_egress_capacity;

    // mmap client slots and fd mapping (max_fds = 2x clients for headroom)
    rc = broker_init(&b, cfg.limits_max_conns, cfg.limits_max_conns * FD_MULTIPLIER,
                     cfg.client_max_inflight, LLMQ_RECV_BUF_SIZE, msg_pool_size,
                     egress_capacity);
    if (rc < 0) {
        log_error("broker_init failed (mmap): %d", rc);
        return 1;
    }
    b.port = cfg.network_port;

    // Calculate memory usage from pools
    u64 client_mem    = (u64)cfg.limits_max_conns * sizeof(struct client_slot);
    u64 recv_mem      = (u64)cfg.limits_max_conns * LLMQ_RECV_BUF_SIZE;
    u64 msg_mem       = (u64)msg_pool_size * sizeof(struct canonical_msg);
    u64 egress_mem    = (u64)cfg.limits_max_conns * egress_capacity * sizeof(struct egress_segment);
    log_info("Memory: clients=%lu KB, recv=%lu MB, msg=%lu KB, egress=%lu KB",
             client_mem / KB_DIVISOR, recv_mem / MB_DIVISOR, msg_mem / KB_DIVISOR,
             egress_mem / KB_DIVISOR);

    log_info("Initializing io_uring with %d entries...", cfg.limits_ring_entries);

    rc = ring_init(&b.ring, cfg.limits_ring_entries);
    if (rc < 0) {
        log_error("ring_init failed: %d", rc);
        broker_cleanup(&b);
        return 1;
    }
    log_info("io_uring initialized");

    log_info("Binding to port %d...", cfg.network_port);

    b.listen_fd = create_listen_socket(cfg.network_port);
    if (b.listen_fd < 0) {
        ring_cleanup(&b.ring);
        broker_cleanup(&b);
        return 1;
    }

    log_info("Listening on port %d", cfg.network_port);

    setup_signals(&b);
    rc = broker_run(&b);

    sys_close(b.listen_fd);
    ring_cleanup(&b.ring);
    broker_cleanup(&b);

    log_info("Stats: accepts=%lu published=%lu", b.accepts, b.msgs_published);
    u32 msg_pct = b.msg_pool.capacity > 0
                      ? (b.msg_pool.high_water * PERCENT_MULTIPLIER) / b.msg_pool.capacity
                      : 0;
    log_info("Pools: msg=%u/%u (%u%%), recv=%u/%u, stolen=%lu",
             b.msg_pool.high_water, b.msg_pool.capacity, msg_pct,
             b.recv_pool.high_water, b.recv_pool.capacity, b.stolen_buffers);
    u64 total_drops = b.msgs_dropped + b.drops_inflight_full + b.drops_egress_full +
                      b.drops_msg_pool_empty + b.drops_sq_full + b.drops_send_failed +
                      b.drops_resp_full;
    if (total_drops > 0) {
        log_info("Drops: %lu total (if=%lu eg=%lu msg=%lu sq=%lu fail=%lu resp=%lu)",
                 total_drops, b.drops_inflight_full, b.drops_egress_full,
                 b.drops_msg_pool_empty, b.drops_sq_full, b.drops_send_failed,
                 b.drops_resp_full);
    }
    if (b.recv_retries > 0 || b.egress_retries > 0) {
        log_info("SQ recovery: %lu recv retries, %lu egress retries",
                 b.recv_retries, b.egress_retries);
    }

    return rc < 0 ? 1 : 0;
}

// =============================================================================
// _start - Entry point (no libc)
// =============================================================================

// Stack at _start: [argc] [argv[0]] [argv[1]] ... [NULL] [envp[0]] ... [NULL]
// Pass argc in rdi, argv in rsi, envp in rdx to broker_main
__asm__(".global _start\n"
        "_start:\n"
        "    xor %rbp, %rbp\n"           // Clear frame pointer
        "    mov (%rsp), %rdi\n"         // argc -> rdi (first arg)
        "    lea 8(%rsp), %rsi\n"        // &argv[0] -> rsi (second arg)
        "    lea 1(%rdi), %rax\n"        // argc + 1
        "    lea 8(%rsp,%rax,8), %rdx\n" // envp = &argv[argc+1] -> rdx (third arg)
        "    and $-16, %rsp\n"           // Align stack to 16 bytes
        "    call broker_main\n"
        "    mov %eax, %edi\n" // Return value -> exit code
        "    mov $60, %eax\n"  // sys_exit
        "    syscall\n");
