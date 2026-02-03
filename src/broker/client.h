// broker/client.h - Client slot with dynamic buffer allocation
// Uses pool allocators for large buffers instead of embedding them

#ifndef BROKER_CLIENT_H
#define BROKER_CLIENT_H

#include "sys/types.h"
#include "config.h"
#include "mem/pool.h"

// =============================================================================
// Compile-time limits (array sizes, can't change at runtime)
// =============================================================================

#define CLIENT_ID_MAX LLMQ_CLIENT_ID_MAX

// =============================================================================
// Client State
// =============================================================================

enum client_state {
    CLIENT_FREE    = 0, // Slot available
    CLIENT_ACTIVE  = 1, // Connected and operational
    CLIENT_DORMANT = 2, // Disconnected, session preserved (clean_session=0)
};

// =============================================================================
// Inflight Message (QoS 1/2 tracking) - metadata only, buffer is pooled
// =============================================================================

enum inflight_state {
    INFLIGHT_FREE         = 0, // Slot available
    INFLIGHT_SENDING      = 1, // QoS 0: buffer in use, freed on send CQE
    INFLIGHT_WAIT_PUBACK  = 2, // QoS 1: sent PUBLISH, waiting for PUBACK
    INFLIGHT_WAIT_PUBREC  = 3, // QoS 2 sender: sent PUBLISH, waiting for PUBREC
    INFLIGHT_WAIT_PUBREL  = 4, // QoS 2 receiver: sent PUBREC, waiting for PUBREL
    INFLIGHT_WAIT_PUBCOMP = 5, // QoS 2 sender: sent PUBREL, waiting for PUBCOMP
};

// Inflight message entry - metadata only, send buffer is separate
struct inflight_msg {
    u16 packet_id;  // MQTT packet identifier
    u8 state;       // enum inflight_state
    u8 qos;         // Original QoS level
    u8 dup_count;   // Retransmission count
    u8 direction;   // 0 = outgoing (we sent), 1 = incoming (we received)
    u16 data_len;   // Bytes used in send buffer
    u32 buf_idx;    // Index into send buffer pool (BUF_POOL_INVALID if none)
};

// =============================================================================
// Pending Message (for offline QoS 1/2 delivery)
// =============================================================================

struct pending_msg {
    u16 topic_offset; // Offset into data[]
    u16 topic_len;
    u16 payload_len;
    u8 qos;
    u8 _pad;
    u8 data[LLMQ_PENDING_MSG_DATA]; // topic + payload (small, kept inline)
};

// =============================================================================
// Client Slot - core structure, dynamically allocated buffers
// =============================================================================

struct client_slot {
    // === Identity (key for slot lookup) ===
    u32 client_id_hash; // FNV-1a hash for fast comparison
    u8 client_id[CLIENT_ID_MAX];
    u8 client_id_len;

    // === State ===
    u8 state;            // enum client_state
    u8 clean_session;    // 1 = destroy on disconnect
    u8 protocol_version; // 4 = MQTT 3.1.1, 5 = MQTT 5.0

    // === Connection (valid when ACTIVE) ===
    i32 fd;          // Socket fd (-1 when DORMANT/FREE)
    u16 keepalive;   // Keepalive interval (seconds)
    u16 _pad1;
    u32 last_active; // Timestamp of last packet

    // Receive buffer (dynamically allocated)
    u32 recv_buf_idx; // Index into recv buffer pool (BUF_POOL_INVALID if none)
    u32 recv_len;     // Bytes in receive buffer

    // Protocol response buffers (small, kept inline)
    u8 proto_buf[LLMQ_PROTO_BUF_COUNT][LLMQ_PROTO_BUF_SIZE];
    u8 proto_buf_idx; // Next buffer slot to use (rotates 0..COUNT-1)

    // === Session (persists when DORMANT if clean_session=0) ===
    // Subscriptions tracked in trie via slot index

    // Pending messages for offline delivery (small, kept inline)
    struct pending_msg pending[LLMQ_MAX_PENDING_MSGS];
    u8 pending_head;
    u8 pending_tail;
    u8 pending_count;
    u8 _pad2;

    // === QoS 1/2 Inflight tracking (metadata inline, buffers pooled) ===
    u16 next_packet_id; // Next packet ID to allocate (wraps at 65535)
    u8 inflight_count;  // Active inflight messages
    u8 max_inflight;    // Runtime limit for this client
    struct inflight_msg inflight[LLMQ_MAX_INFLIGHT]; // Small metadata array
};

// =============================================================================
// Hash Function (FNV-1a)
// =============================================================================

#define FNV_OFFSET_BASIS 2166136261u
#define FNV_PRIME        16777619u

INLINE u32 fnv1a(const u8 *data, u32 len) {
    u32 hash = FNV_OFFSET_BASIS;
    for (u32 i = 0; i < len; i++) {
        hash ^= data[i];
        hash *= FNV_PRIME;
    }
    return hash;
}

// =============================================================================
// Client Slot Operations
// =============================================================================

// Initialize a slot for a new client (call after allocating recv buffer)
INLINE void client_init(struct client_slot *c, i32 fd, u32 recv_buf_idx, u8 max_inflight) {
    c->state            = CLIENT_ACTIVE;
    c->fd               = fd;
    c->client_id_len    = 0;
    c->client_id_hash   = 0;
    c->clean_session    = 1;
    c->protocol_version = 0; // 0 = awaiting CONNECT
    c->keepalive        = 0;
    c->last_active      = 0;
    c->recv_buf_idx     = recv_buf_idx;
    c->recv_len         = 0;
    c->proto_buf_idx    = 0;
    c->pending_head     = 0;
    c->pending_tail     = 0;
    c->pending_count    = 0;
    c->next_packet_id   = 1;
    c->inflight_count   = 0;
    c->max_inflight     = max_inflight;
    for (u8 i = 0; i < LLMQ_MAX_INFLIGHT; i++) {
        c->inflight[i].state   = INFLIGHT_FREE;
        c->inflight[i].buf_idx = BUF_POOL_INVALID;
    }
}

// Get receive buffer pointer (requires pool reference)
INLINE u8 *client_recv_buf(struct client_slot *c, struct buf_pool *recv_pool) {
    if (c->recv_buf_idx == BUF_POOL_INVALID) {
        return NULL;
    }
    return buf_pool_get(recv_pool, c->recv_buf_idx);
}

// Get next protocol response buffer (rotates through available slots)
INLINE u8 *client_get_proto_buf(struct client_slot *c, u8 proto_buf_count) {
    u8 idx           = c->proto_buf_idx;
    c->proto_buf_idx = (idx + 1) % proto_buf_count;
    return c->proto_buf[idx];
}

// Set client identity after CONNECT
INLINE void client_set_identity(struct client_slot *c, const u8 *id, u8 len) {
    if (len > CLIENT_ID_MAX)
        len = CLIENT_ID_MAX;
    for (u8 i = 0; i < len; i++) {
        c->client_id[i] = id[i];
    }
    c->client_id_len  = len;
    c->client_id_hash = fnv1a(id, len);
}

// Transition to DORMANT (disconnect with clean_session=0)
// Note: recv_buf should be freed separately
INLINE void client_go_dormant(struct client_slot *c) {
    c->state        = CLIENT_DORMANT;
    c->fd           = -1;
    c->recv_len     = 0;
    c->recv_buf_idx = BUF_POOL_INVALID; // Caller should have freed it
    // Session data (subscriptions, pending) preserved
}

// Free the slot completely
// Note: All pooled buffers should be freed separately
INLINE void client_free(struct client_slot *c) {
    c->state          = CLIENT_FREE;
    c->fd             = -1;
    c->client_id_len  = 0;
    c->client_id_hash = 0;
    c->recv_len       = 0;
    c->recv_buf_idx   = BUF_POOL_INVALID;
    c->pending_head   = 0;
    c->pending_tail   = 0;
    c->pending_count  = 0;
}

// Check if slot matches a client ID
INLINE bool client_matches(struct client_slot *c, const u8 *id, u8 len, u32 hash) {
    if (c->state == CLIENT_FREE)
        return false;
    if (c->client_id_hash != hash)
        return false;
    if (c->client_id_len != len)
        return false;
    for (u8 i = 0; i < len; i++) {
        if (c->client_id[i] != id[i])
            return false;
    }
    return true;
}

// =============================================================================
// Pending Message Operations (unchanged - small, kept inline)
// =============================================================================

INLINE bool client_has_pending(struct client_slot *c) {
    return c->pending_count > 0;
}

INLINE struct pending_msg *client_pending_push(struct client_slot *c) {
    if (c->pending_count >= LLMQ_MAX_PENDING_MSGS) {
        return NULL;
    }
    struct pending_msg *msg = &c->pending[c->pending_tail];
    c->pending_tail         = (c->pending_tail + 1) % LLMQ_MAX_PENDING_MSGS;
    c->pending_count++;
    return msg;
}

INLINE struct pending_msg *client_pending_peek(struct client_slot *c) {
    if (c->pending_count == 0)
        return NULL;
    return &c->pending[c->pending_head];
}

INLINE void client_pending_pop(struct client_slot *c) {
    if (c->pending_count == 0)
        return;
    c->pending_head = (c->pending_head + 1) % LLMQ_MAX_PENDING_MSGS;
    c->pending_count--;
}

// =============================================================================
// Inflight Message Operations (with pooled buffers)
// =============================================================================

// Error codes for client_inflight_alloc
#define INFLIGHT_ERR_CLIENT_FULL (-1) // Per-client inflight limit exceeded
#define INFLIGHT_ERR_POOL_EMPTY  (-2) // Global send pool exhausted

// Allocate inflight slot and send buffer from pool
// Returns: inflight index (>=0), or error code (<0)
INLINE i32 client_inflight_alloc(struct client_slot *c, u8 qos, u16 *out_packet_id,
                                  struct buf_pool *send_pool) {
    if (c->inflight_count >= c->max_inflight) {
        return INFLIGHT_ERR_CLIENT_FULL;
    }

    // Find free slot
    for (u8 i = 0; i < c->max_inflight; i++) {
        if (c->inflight[i].state == INFLIGHT_FREE) {
            // Allocate send buffer from pool
            u32 buf_idx = buf_pool_alloc(send_pool);
            if (buf_idx == BUF_POOL_INVALID) {
                return INFLIGHT_ERR_POOL_EMPTY;
            }

            // Allocate unique packet ID
            u16 start_id = c->next_packet_id;
            do {
                c->next_packet_id++;
                if (c->next_packet_id == 0)
                    c->next_packet_id = 1; // 0 is invalid

                // Check if ID is in use
                bool in_use = false;
                for (u8 j = 0; j < c->max_inflight; j++) {
                    if (c->inflight[j].state != INFLIGHT_FREE &&
                        c->inflight[j].packet_id == c->next_packet_id) {
                        in_use = true;
                        break;
                    }
                }

                if (!in_use) {
                    *out_packet_id           = c->next_packet_id;
                    c->inflight[i].packet_id = c->next_packet_id;
                    c->inflight[i].qos       = qos;
                    c->inflight[i].dup_count = 0;
                    c->inflight[i].direction = 0; // outgoing
                    c->inflight[i].data_len  = 0;
                    c->inflight[i].buf_idx   = buf_idx;

                    if (qos == 0) {
                        c->inflight[i].state = INFLIGHT_SENDING;
                    } else if (qos == 1) {
                        c->inflight[i].state = INFLIGHT_WAIT_PUBACK;
                    } else {
                        c->inflight[i].state = INFLIGHT_WAIT_PUBREC;
                    }
                    c->inflight_count++;
                    return (i32)i;
                }
            } while (c->next_packet_id != start_id);

            // Failed to find unique ID - free the buffer we allocated
            buf_pool_free(send_pool, buf_idx);
            return -1;
        }
    }
    return -1;
}

// Track incoming QoS 2 message (no buffer needed - we're receiving, not sending)
INLINE i32 client_inflight_track_incoming(struct client_slot *c, u16 packet_id) {
    if (c->inflight_count >= c->max_inflight) {
        return -1;
    }

    // Check for duplicate
    for (u8 i = 0; i < c->max_inflight; i++) {
        if (c->inflight[i].state != INFLIGHT_FREE && c->inflight[i].packet_id == packet_id &&
            c->inflight[i].direction == 1) {
            return (i32)i;
        }
    }

    // Find free slot
    for (u8 i = 0; i < c->max_inflight; i++) {
        if (c->inflight[i].state == INFLIGHT_FREE) {
            c->inflight[i].packet_id = packet_id;
            c->inflight[i].qos       = 2;
            c->inflight[i].dup_count = 0;
            c->inflight[i].direction = 1; // incoming
            c->inflight[i].data_len  = 0;
            c->inflight[i].buf_idx   = BUF_POOL_INVALID; // No send buffer needed
            c->inflight[i].state     = INFLIGHT_WAIT_PUBREL;
            c->inflight_count++;
            return (i32)i;
        }
    }
    return -1;
}

// Find inflight entry by packet ID
INLINE struct inflight_msg *client_inflight_find(struct client_slot *c, u16 packet_id) {
    for (u8 i = 0; i < c->max_inflight; i++) {
        if (c->inflight[i].state != INFLIGHT_FREE && c->inflight[i].packet_id == packet_id) {
            return &c->inflight[i];
        }
    }
    return NULL;
}

// Free inflight entry and return its buffer to pool
INLINE void client_inflight_free(struct client_slot *c, struct inflight_msg *msg,
                                  struct buf_pool *send_pool) {
    if (msg->state != INFLIGHT_FREE) {
        // Return buffer to pool if we have one
        if (msg->buf_idx != BUF_POOL_INVALID && send_pool) {
            buf_pool_free(send_pool, msg->buf_idx);
        }
        msg->buf_idx = BUF_POOL_INVALID;
        msg->state   = INFLIGHT_FREE;
        if (c->inflight_count > 0)
            c->inflight_count--;
    }
}

// Get send buffer pointer for an inflight message
INLINE u8 *client_inflight_buffer(struct inflight_msg *msg, struct buf_pool *send_pool) {
    if (msg->buf_idx == BUF_POOL_INVALID) {
        return NULL;
    }
    return buf_pool_get(send_pool, msg->buf_idx);
}

// Free all inflight buffers for a client (call before client_free)
INLINE void client_inflight_free_all(struct client_slot *c, struct buf_pool *send_pool) {
    for (u8 i = 0; i < LLMQ_MAX_INFLIGHT; i++) {
        if (c->inflight[i].state != INFLIGHT_FREE) {
            client_inflight_free(c, &c->inflight[i], send_pool);
        }
    }
}

#endif // BROKER_CLIENT_H
