// broker/state.h - Broker state structures
// Connection table, subscription storage, and statistics

#ifndef BROKER_STATE_H
#define BROKER_STATE_H

#include "sys/types.h"
#include "sys/syscall.h"
#include "sys/io_uring.h"
#include "broker/client.h"
#include "broker/trie.h"
#include "config.h"

// =============================================================================
// Configuration Defaults (from config.h, with local aliases)
// =============================================================================

#define DEFAULT_LISTEN_PORT    LLMQ_LISTEN_PORT
#define DEFAULT_LISTEN_BACKLOG LLMQ_LISTEN_BACKLOG
#define DEFAULT_RING_ENTRIES   LLMQ_RING_ENTRIES
#define DEFAULT_MAX_CLIENTS    LLMQ_MAX_CLIENTS
#define DEFAULT_MAX_FDS        LLMQ_MAX_FDS

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
    u8 max_inflight;   // Runtime limit for inflight messages per client
    u8 proto_buf_count; // Runtime limit for proto_buf slots per client

    // Client slots (mmap'd)
    struct client_slot *clients;

    // fd → slot index mapping (mmap'd)
    // fd_to_slot[fd] = slot index, or -1 if fd not in use
    i32 *fd_to_slot;

    // Free slot tracking
    u32 free_head;     // Head of free list (slot index, or max_clients if none)
    u32 active_count;  // Number of ACTIVE clients
    u32 dormant_count; // Number of DORMANT clients

    // Topic trie for subscription matching
    struct topic_trie trie;

    // Stats
    u64 accepts;
    u64 bytes_recv;
    u64 bytes_sent;
    u64 msgs_published;
    u64 msgs_dropped; // Dropped due to inflight full
};

// =============================================================================
// Broker Initialization
// =============================================================================

INLINE i32 broker_init(struct broker *b, u32 max_clients, u32 max_fds, u8 max_inflight,
                       u8 proto_buf_count) {
    b->max_clients     = max_clients;
    b->max_fds         = max_fds;
    b->max_inflight    = max_inflight;
    b->proto_buf_count = proto_buf_count;
    b->active_count    = 0;
    b->dormant_count   = 0;

    // mmap client slots
    usize clients_size = max_clients * sizeof(struct client_slot);
    b->clients         = (struct client_slot *)sys_mmap(NULL, clients_size, PROT_READ | PROT_WRITE,
                                                        MAP_PRIVATE | MAP_ANON, -1, 0);
    if (b->clients == MAP_FAILED) {
        return -1;
    }

    // mmap fd_to_slot mapping
    usize fd_map_size = max_fds * sizeof(i32);
    b->fd_to_slot =
        (i32 *)sys_mmap(NULL, fd_map_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (b->fd_to_slot == MAP_FAILED) {
        sys_munmap(b->clients, clients_size);
        return -1;
    }

    // Initialize client slots as free list
    for (u32 i = 0; i < max_clients; i++) {
        b->clients[i].state = CLIENT_FREE;
        b->clients[i].fd    = (i32)(i + 1); // Next free slot (abuse fd field)
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
    if (b->clients) {
        sys_munmap(b->clients, b->max_clients * sizeof(struct client_slot));
    }
    if (b->fd_to_slot) {
        sys_munmap(b->fd_to_slot, b->max_fds * sizeof(i32));
    }
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

    u32 slot_idx          = b->free_head;
    struct client_slot *c = &b->clients[slot_idx];

    // Update free list head
    b->free_head = (c->fd >= 0) ? (u32)c->fd : b->max_clients;

    // Initialize slot
    client_init(c, fd);
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
    } else if (c->state == CLIENT_DORMANT) {
        b->dormant_count--;
    }

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

    client_go_dormant(c);
    b->active_count--;
    b->dormant_count++;
    // Subscriptions remain in trie (slot_idx still valid)
}

// Resume a DORMANT slot with new fd
INLINE void broker_slot_resume(struct broker *b, u32 slot_idx, i32 fd) {
    struct client_slot *c = &b->clients[slot_idx];

    if (c->state != CLIENT_DORMANT)
        return;

    c->state          = CLIENT_ACTIVE;
    c->fd             = fd;
    c->recv_len       = 0;
    b->fd_to_slot[fd] = (i32)slot_idx;
    b->active_count++;
    b->dormant_count--;
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
