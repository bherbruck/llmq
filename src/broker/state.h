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
    u16 max_inflight; // Runtime limit for inflight messages per client

    // Client slots (mmap'd)
    struct client_slot *clients;

    // Recv retry queue (for SQ-full recovery)
    // Circular buffer of slot indices that need recv resubmission
    u32 *recv_retry_queue;
    u32 recv_retry_head;
    u32 recv_retry_tail;
    u32 recv_retry_capacity;

    // fd → slot index mapping (mmap'd)
    i32 *fd_to_slot;

    // Buffer pools (shared across all clients)
    struct buf_pool recv_pool;          // Receive buffers (one per active client)
    struct msg_pool msg_pool;           // Canonical message metadata for zero-copy fan-out
    struct send_desc_pool send_desc_pool; // Send descriptors for active sends

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
    u64 msgs_dropped;             // Total drops (sum of below)
    u64 drops_inflight_full;      // Per-client inflight limit hit
    u64 drops_send_desc_empty;    // Send descriptor pool exhausted
    u64 drops_msg_pool_empty;     // Message pool exhausted
    u64 drops_sq_full;            // SQ full during fan-out submit
    u64 drops_send_failed;        // Send completions with error
    u64 stolen_buffers;           // Count of recv buffers stolen for fan-out
    u64 recv_retries;             // Count of recv submissions retried from SQ-full
};

// =============================================================================
// Broker Initialization
// =============================================================================

INLINE i32 broker_init(struct broker *b, u32 max_clients, u32 max_fds, u16 max_inflight,
                       u32 recv_buf_size, u32 msg_pool_size, u32 send_desc_pool_size) {
    b->max_clients   = max_clients;
    b->max_fds       = max_fds;
    b->max_inflight  = max_inflight;
    b->active_count  = 0;
    b->dormant_count = 0;

    // Initialize recv retry queue (sized to max_clients - worst case all need retry)
    b->recv_retry_capacity = max_clients;
    b->recv_retry_head     = 0;
    b->recv_retry_tail     = 0;
    usize retry_size       = (usize)max_clients * sizeof(u32);
    b->recv_retry_queue    = (u32 *)sys_mmap(NULL, retry_size, PROT_READ | PROT_WRITE,
                                              MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(b->recv_retry_queue)) {
        return -1;
    }

    // Initialize buffer pools FIRST (before client slots need them)

    // Recv pool: one buffer per potential active client
    if (buf_pool_init(&b->recv_pool, max_clients, recv_buf_size) < 0) {
        sys_munmap(b->recv_retry_queue, retry_size);
        return -1;
    }

    // Message pool: canonical message metadata for zero-copy fan-out
    if (msg_pool_init(&b->msg_pool, msg_pool_size) < 0) {
        sys_munmap(b->recv_retry_queue, retry_size);
        buf_pool_cleanup(&b->recv_pool);
        return -1;
    }

    // Send descriptor pool: ephemeral send state for active sends
    if (send_desc_pool_init(&b->send_desc_pool, send_desc_pool_size) < 0) {
        sys_munmap(b->recv_retry_queue, retry_size);
        buf_pool_cleanup(&b->recv_pool);
        msg_pool_cleanup(&b->msg_pool);
        return -1;
    }

    // mmap client slots (now much smaller without embedded buffers)
    usize clients_size = max_clients * sizeof(struct client_slot);
    b->clients         = (struct client_slot *)sys_mmap(NULL, clients_size, PROT_READ | PROT_WRITE,
                                                        MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(b->clients)) {
        sys_munmap(b->recv_retry_queue, retry_size);
        buf_pool_cleanup(&b->recv_pool);
        msg_pool_cleanup(&b->msg_pool);
        send_desc_pool_cleanup(&b->send_desc_pool);
        return -1;
    }

    // mmap fd_to_slot mapping
    usize fd_map_size = max_fds * sizeof(i32);
    b->fd_to_slot =
        (i32 *)sys_mmap(NULL, fd_map_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(b->fd_to_slot)) {
        sys_munmap(b->clients, clients_size);
        sys_munmap(b->recv_retry_queue, retry_size);
        buf_pool_cleanup(&b->recv_pool);
        msg_pool_cleanup(&b->msg_pool);
        send_desc_pool_cleanup(&b->send_desc_pool);
        return -1;
    }

    // Initialize client slots as free list
    for (u32 i = 0; i < max_clients; i++) {
        b->clients[i].state                = CLIENT_FREE;
        b->clients[i].fd                   = (i32)(i + 1); // Next free slot (abuse fd field)
        b->clients[i].recv_buf_idx         = BUF_POOL_INVALID;
        b->clients[i].orphaned_recv_buf_idx = BUF_POOL_INVALID;
        b->clients[i].generation           = 0;
        b->clients[i].recv_pending         = 0;
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
    // Free all client resources first
    for (u32 i = 0; i < b->max_clients; i++) {
        struct client_slot *c = &b->clients[i];
        if (c->state != CLIENT_FREE) {
            // Free recv buffer
            if (c->recv_buf_idx != BUF_POOL_INVALID) {
                buf_pool_free(&b->recv_pool, c->recv_buf_idx);
            }
            // Clear inflight entries (caller should have cleaned up pools)
            client_inflight_free_all(c);
        }
    }

    if (b->clients) {
        sys_munmap(b->clients, b->max_clients * sizeof(struct client_slot));
    }
    if (b->fd_to_slot) {
        sys_munmap(b->fd_to_slot, b->max_fds * sizeof(i32));
    }
    if (b->recv_retry_queue) {
        sys_munmap(b->recv_retry_queue, b->recv_retry_capacity * sizeof(u32));
    }
    buf_pool_cleanup(&b->recv_pool);
    msg_pool_cleanup(&b->msg_pool);
    send_desc_pool_cleanup(&b->send_desc_pool);
}

// =============================================================================
// Recv Retry Queue (for SQ-full recovery)
// =============================================================================

// Check if retry queue is empty
INLINE bool recv_retry_empty(struct broker *b) {
    return b->recv_retry_head == b->recv_retry_tail;
}

// Check if retry queue is full
INLINE bool recv_retry_full(struct broker *b) {
    u32 next = (b->recv_retry_tail + 1) % b->recv_retry_capacity;
    return next == b->recv_retry_head;
}

// Enqueue slot_idx for recv retry
INLINE void recv_retry_enqueue(struct broker *b, u32 slot_idx) {
    if (recv_retry_full(b)) {
        return; // Queue full - should never happen if sized to max_clients
    }
    b->recv_retry_queue[b->recv_retry_tail] = slot_idx;
    b->recv_retry_tail = (b->recv_retry_tail + 1) % b->recv_retry_capacity;
}

// Dequeue slot_idx for recv retry, returns max_clients if empty
INLINE u32 recv_retry_dequeue(struct broker *b) {
    if (recv_retry_empty(b)) {
        return b->max_clients; // Sentinel: invalid slot
    }
    u32 slot_idx       = b->recv_retry_queue[b->recv_retry_head];
    b->recv_retry_head = (b->recv_retry_head + 1) % b->recv_retry_capacity;
    return slot_idx;
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

    if (c->state == CLIENT_ACTIVE || c->state == CLIENT_CLOSING) {
        b->active_count--;
        if (c->fd >= 0 && (u32)c->fd < b->max_fds) {
            b->fd_to_slot[c->fd] = -1;
        }
        // Handle recv buffer
        if (c->recv_buf_idx != BUF_POOL_INVALID) {
            if (c->recv_pending) {
                // Recv is in-flight - kernel may still write to this buffer.
                // Save as orphaned; it will be freed when stale recv CQE arrives.
                c->orphaned_recv_buf_idx = c->recv_buf_idx;
            } else {
                // No recv pending - safe to free immediately
                buf_pool_free(&b->recv_pool, c->recv_buf_idx);
            }
        }
    } else if (c->state == CLIENT_DORMANT) {
        b->dormant_count--;
    }

    // Clear inflight entries (msg_pool refs handled separately during send completion)
    client_inflight_free_all(c);

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

    if (c->state != CLIENT_ACTIVE && c->state != CLIENT_CLOSING)
        return;

    if (c->fd >= 0 && (u32)c->fd < b->max_fds) {
        b->fd_to_slot[c->fd] = -1;
    }

    // Handle recv buffer
    if (c->recv_buf_idx != BUF_POOL_INVALID) {
        if (c->recv_pending) {
            // Recv is in-flight - kernel may still write to this buffer.
            // Save as orphaned; it will be freed when stale recv CQE arrives.
            c->orphaned_recv_buf_idx = c->recv_buf_idx;
        } else {
            // No recv pending - safe to free immediately
            buf_pool_free(&b->recv_pool, c->recv_buf_idx);
        }
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
    OP_SEND_ZC     = 5, // Zero-copy send: context = send_desc index
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

#endif // BROKER_STATE_H
