// broker/state.h - Broker state structures
// Connection table, subscription storage, buffer pools, and statistics

#ifndef BROKER_STATE_H
#define BROKER_STATE_H

#include "sys/types.h"
#include "sys/syscall.h"
#include "sys/io_uring.h"
#include "broker/client.h"
#include "broker/trie.h"
#include "mem/msgbuf.h"
#include "config.h"

// =============================================================================
// Broker State
// =============================================================================

struct broker {
    // io_uring
    struct ring ring;
    i32 listen_fd;
    volatile bool running;

    // Configuration
    u32 max_clients;
    u32 max_fds;
    u16 port;
    u8 max_inflight;    // Runtime limit for inflight messages per client
    u8 proto_buf_count; // Runtime limit for proto_buf slots per client

    // Client slots (mmap'd)
    struct client_slot *clients;

    // fd → slot index mapping (mmap'd)
    i32 *fd_to_slot;

    // Buffer pools (shared across all clients)
    struct buf_pool recv_pool; // Receive buffers (one per active client)
    struct buf_pool send_pool; // Send buffers for QoS 1/2 inflight messages
    struct msg_pool msg_pool;  // Shared message buffers for QoS 0 fan-out

    // Free slot tracking
    u32 free_head;     // Head of free list
    u32 active_count;  // Number of ACTIVE clients
    u32 dormant_count; // Number of DORMANT clients

    // Topic trie for subscription matching
    struct topic_trie trie;

    // Stats
    u64 accepts;
    u64 bytes_recv;
    u64 bytes_sent;
    u64 msgs_published;
    u64 msgs_dropped;        // Total drops (sum of below)
    u64 drops_inflight_full; // Per-client inflight limit hit
    u64 drops_pool_empty;    // Global send pool exhausted
};

// =============================================================================
// Broker Initialization
// =============================================================================

INLINE i32 broker_init(struct broker *b, u32 max_clients, u32 max_fds, u8 max_inflight,
                       u8 proto_buf_count, u32 recv_buf_size, u32 send_buf_size,
                       u32 send_pool_count) {
    b->max_clients     = max_clients;
    b->max_fds         = max_fds;
    b->max_inflight    = max_inflight;
    b->proto_buf_count = proto_buf_count;
    b->active_count    = 0;
    b->dormant_count   = 0;

    // Initialize buffer pools FIRST (before client slots need them)

    // Recv pool: one buffer per potential active client
    if (buf_pool_init(&b->recv_pool, max_clients, recv_buf_size) < 0) {
        return -1;
    }

    // Send pool: shared pool for QoS 1/2 inflight messages
    if (buf_pool_init(&b->send_pool, send_pool_count, send_buf_size) < 0) {
        buf_pool_cleanup(&b->recv_pool);
        return -1;
    }

    // Message pool: shared buffers for QoS 0 zero-copy fan-out
    if (msg_pool_init(&b->msg_pool, LLMQ_MSG_BUF_COUNT, LLMQ_MSG_BUF_SIZE) < 0) {
        buf_pool_cleanup(&b->recv_pool);
        buf_pool_cleanup(&b->send_pool);
        return -1;
    }

    // mmap client slots (now much smaller without embedded buffers)
    usize clients_size = max_clients * sizeof(struct client_slot);
    b->clients         = (struct client_slot *)sys_mmap(NULL, clients_size, PROT_READ | PROT_WRITE,
                                                        MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(b->clients)) {
        buf_pool_cleanup(&b->recv_pool);
        buf_pool_cleanup(&b->send_pool);
        msg_pool_cleanup(&b->msg_pool);
        return -1;
    }

    // mmap fd_to_slot mapping
    usize fd_map_size = max_fds * sizeof(i32);
    b->fd_to_slot =
        (i32 *)sys_mmap(NULL, fd_map_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(b->fd_to_slot)) {
        sys_munmap(b->clients, clients_size);
        buf_pool_cleanup(&b->recv_pool);
        buf_pool_cleanup(&b->send_pool);
        msg_pool_cleanup(&b->msg_pool);
        return -1;
    }

    // Initialize client slots as free list
    for (u32 i = 0; i < max_clients; i++) {
        b->clients[i].state        = CLIENT_FREE;
        b->clients[i].fd           = (i32)(i + 1); // Next free slot (abuse fd field)
        b->clients[i].recv_buf_idx = BUF_POOL_INVALID;
    }
    b->clients[max_clients - 1].fd = -1; // End of list
    b->free_head                   = 0;

    // Initialize fd_to_slot mapping
    for (u32 i = 0; i < max_fds; i++) {
        b->fd_to_slot[i] = -1;
    }

    // Initialize trie
    trie_init(&b->trie);

    return 0;
}

INLINE void broker_cleanup(struct broker *b) {
    // Free all client buffers first
    for (u32 i = 0; i < b->max_clients; i++) {
        struct client_slot *c = &b->clients[i];
        if (c->state != CLIENT_FREE) {
            // Free recv buffer
            if (c->recv_buf_idx != BUF_POOL_INVALID) {
                buf_pool_free(&b->recv_pool, c->recv_buf_idx);
            }
            // Free inflight buffers (decrements msg_pool refcounts)
            client_inflight_free_all(c, &b->msg_pool);
        }
    }

    if (b->clients) {
        sys_munmap(b->clients, b->max_clients * sizeof(struct client_slot));
    }
    if (b->fd_to_slot) {
        sys_munmap(b->fd_to_slot, b->max_fds * sizeof(i32));
    }
    buf_pool_cleanup(&b->recv_pool);
    buf_pool_cleanup(&b->send_pool);
    msg_pool_cleanup(&b->msg_pool);
}

// =============================================================================
// Slot Allocation
// =============================================================================

// Allocate a free slot for a new connection
INLINE i32 broker_alloc_slot(struct broker *b, i32 fd) {
    if (b->free_head >= b->max_clients) {
        return -1; // No free slots
    }
    if (fd < 0 || (u32)fd >= b->max_fds) {
        return -1; // fd out of range
    }

    // Allocate receive buffer from pool
    u32 recv_buf_idx = buf_pool_alloc(&b->recv_pool);
    if (recv_buf_idx == BUF_POOL_INVALID) {
        return -1; // No recv buffers
    }

    u32 slot_idx          = b->free_head;
    struct client_slot *c = &b->clients[slot_idx];

    // Update free list head
    b->free_head = (c->fd >= 0) ? (u32)c->fd : b->max_clients;

    // Initialize slot with allocated recv buffer
    client_init(c, fd, recv_buf_idx, b->max_inflight);
    b->fd_to_slot[fd] = (i32)slot_idx;
    b->active_count++;

    return (i32)slot_idx;
}

// Find existing slot by client ID (for session resume)
INLINE i32 broker_find_slot(struct broker *b, const u8 *client_id, u8 len) {
    u32 hash = fnv1a(client_id, len);

    for (u32 i = 0; i < b->max_clients; i++) {
        if (client_matches(&b->clients[i], client_id, len, hash)) {
            return (i32)i;
        }
    }
    return -1;
}

// Get slot by fd
INLINE struct client_slot *broker_get_client_by_fd(struct broker *b, i32 fd) {
    if (fd < 0 || (u32)fd >= b->max_fds)
        return NULL;
    i32 slot_idx = b->fd_to_slot[fd];
    if (slot_idx < 0)
        return NULL;
    return &b->clients[slot_idx];
}

// Get slot by index
INLINE struct client_slot *broker_get_client(struct broker *b, u32 slot_idx) {
    if (slot_idx >= b->max_clients)
        return NULL;
    return &b->clients[slot_idx];
}

// Free a slot (clean_session=1 disconnect or takeover)
INLINE void broker_free_slot(struct broker *b, u32 slot_idx) {
    struct client_slot *c = &b->clients[slot_idx];

    if (c->state == CLIENT_ACTIVE) {
        b->active_count--;
        if (c->fd >= 0 && (u32)c->fd < b->max_fds) {
            b->fd_to_slot[c->fd] = -1;
        }
        // Free recv buffer
        if (c->recv_buf_idx != BUF_POOL_INVALID) {
            buf_pool_free(&b->recv_pool, c->recv_buf_idx);
        }
    } else if (c->state == CLIENT_DORMANT) {
        b->dormant_count--;
    }

    // Free inflight buffers (decrements msg_pool refcounts)
    client_inflight_free_all(c, &b->msg_pool);

    // Remove from trie
    trie_remove_fd(&b->trie, slot_idx);

    // Add to free list
    client_free(c);
    c->fd        = (i32)b->free_head;
    b->free_head = slot_idx;
}

// Transition slot to DORMANT (clean_session=0 disconnect)
INLINE void broker_slot_go_dormant(struct broker *b, u32 slot_idx) {
    struct client_slot *c = &b->clients[slot_idx];

    if (c->state != CLIENT_ACTIVE)
        return;

    if (c->fd >= 0 && (u32)c->fd < b->max_fds) {
        b->fd_to_slot[c->fd] = -1;
    }

    // Free recv buffer (not needed while dormant)
    if (c->recv_buf_idx != BUF_POOL_INVALID) {
        buf_pool_free(&b->recv_pool, c->recv_buf_idx);
    }

    client_go_dormant(c);
    b->active_count--;
    b->dormant_count++;
    // Subscriptions remain in trie (slot_idx still valid)
    // Inflight buffers remain (for retransmission on reconnect)
}

// Resume a DORMANT slot with new fd
INLINE void broker_slot_resume(struct broker *b, u32 slot_idx, i32 fd) {
    struct client_slot *c = &b->clients[slot_idx];

    if (c->state != CLIENT_DORMANT)
        return;

    // Allocate new recv buffer
    u32 recv_buf_idx = buf_pool_alloc(&b->recv_pool);
    if (recv_buf_idx == BUF_POOL_INVALID) {
        return; // No buffers available, can't resume
    }

    c->state          = CLIENT_ACTIVE;
    c->fd             = fd;
    c->recv_buf_idx   = recv_buf_idx;
    c->recv_len       = 0;
    b->fd_to_slot[fd] = (i32)slot_idx;
    b->active_count++;
    b->dormant_count--;
}

// =============================================================================
// User data encoding for CQE dispatch
// =============================================================================

enum op_type {
    OP_ACCEPT      = 1,
    OP_RECV        = 2,
    OP_SEND        = 3,
    OP_CLOSE       = 4,
    OP_SEND_SHARED = 5, // QoS 0 fan-out: context = msg_pool index
    OP_SEND_WRITEV = 6, // QoS 1/2 scatter-gather: context = send_ctx (slot+inflight+qos)
};

// Pack: [8-bit op][24-bit fd][32-bit context]
#define UD_OP_SHIFT 56
#define UD_FD_SHIFT 32
#define UD_FD_MASK  0xFFFFFFU

INLINE u64 make_user_data(u8 op, u32 fd, u32 ctx) {
    return ((u64)op << UD_OP_SHIFT) | ((u64)(fd & UD_FD_MASK) << UD_FD_SHIFT) | (u64)ctx;
}

INLINE u8 ud_op(u64 ud) {
    return (u8)(ud >> UD_OP_SHIFT);
}

INLINE u32 ud_fd(u64 ud) {
    return (u32)((ud >> UD_FD_SHIFT) & UD_FD_MASK);
}

#define UD_CTX_MASK 0xFFFFFFFFU

INLINE u32 ud_ctx(u64 ud) {
    return (u32)(ud & UD_CTX_MASK);
}

// Context encoding for PUBLISH sends: [16-bit slot][8-bit inflight_idx][8-bit qos]
#define SEND_CTX_SLOT_SHIFT     16
#define SEND_CTX_INFLIGHT_SHIFT 8
#define SEND_CTX_QOS_MASK       0xFFU

INLINE u32 make_send_ctx(u32 slot_idx, u8 inflight_idx, u8 qos) {
    return (slot_idx << SEND_CTX_SLOT_SHIFT) | ((u32)inflight_idx << SEND_CTX_INFLIGHT_SHIFT) | qos;
}

INLINE u32 send_ctx_slot(u32 ctx) {
    return ctx >> SEND_CTX_SLOT_SHIFT;
}

INLINE u8 send_ctx_inflight(u32 ctx) {
    return (u8)((ctx >> SEND_CTX_INFLIGHT_SHIFT) & SEND_CTX_QOS_MASK);
}

INLINE u8 send_ctx_qos(u32 ctx) {
    return (u8)(ctx & SEND_CTX_QOS_MASK);
}

#endif // BROKER_STATE_H
