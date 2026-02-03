// broker/client.h - Unified client slot (connection + session)
// Each slot represents one MQTT client identity

#ifndef BROKER_CLIENT_H
#define BROKER_CLIENT_H

#include "sys/types.h"
#include "config.h"

// =============================================================================
// Configuration (from config.h, with backwards-compat aliases)
// =============================================================================

#define CLIENT_ID_MAX    LLMQ_CLIENT_ID_MAX
#define RECV_BUF_SIZE    LLMQ_RECV_BUF_SIZE
#define MAX_PENDING_MSGS LLMQ_MAX_PENDING_MSGS
#define MAX_CLIENT_SUBS  LLMQ_MAX_CLIENT_SUBS
#define PENDING_MSG_DATA LLMQ_PENDING_MSG_DATA
#define MAX_INFLIGHT     LLMQ_MAX_INFLIGHT
#define SEND_BUF_SIZE    LLMQ_SEND_BUF_SIZE

// =============================================================================
// Client State
// =============================================================================

enum client_state {
    CLIENT_FREE    = 0, // Slot available
    CLIENT_ACTIVE  = 1, // Connected and operational
    CLIENT_DORMANT = 2, // Disconnected, session preserved (clean_session=0)
};

// =============================================================================
// Inflight Message States (for QoS 1/2 tracking)
// =============================================================================

enum inflight_state {
    INFLIGHT_FREE         = 0, // Slot available
    INFLIGHT_SENDING      = 1, // QoS 0: buffer in use, freed on send CQE
    INFLIGHT_WAIT_PUBACK  = 2, // QoS 1: sent PUBLISH, waiting for PUBACK
    INFLIGHT_WAIT_PUBREC  = 3, // QoS 2 sender: sent PUBLISH, waiting for PUBREC
    INFLIGHT_WAIT_PUBREL  = 4, // QoS 2 receiver: sent PUBREC, waiting for PUBREL
    INFLIGHT_WAIT_PUBCOMP = 5, // QoS 2 sender: sent PUBREL, waiting for PUBCOMP
};

// Inflight message entry - tracks one pending QoS 1/2 delivery
struct inflight_msg {
    u16 packet_id;              // MQTT packet identifier
    u8 state;                   // enum inflight_state
    u8 qos;                     // Original QoS level
    u8 dup_count;               // Retransmission count
    u8 direction;               // 0 = outgoing (we sent), 1 = incoming (we received)
    u16 data_len;               // Bytes used in send_buf
    u8 send_buf[SEND_BUF_SIZE]; // Buffer for async sends (persists until CQE)
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
    u8 data[PENDING_MSG_DATA]; // topic + payload (small messages only)
};

// =============================================================================
// Client Slot - Unified connection + session
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
    i32 fd;        // Socket fd (-1 when DORMANT/FREE)
    u16 keepalive; // Keepalive interval (seconds)
    u16 _pad1;
    u32 last_active;            // Timestamp of last packet
    u32 recv_len;               // Bytes in receive buffer
    u8 recv_buf[RECV_BUF_SIZE]; // Per-client receive buffer

    // Rotating buffers for protocol responses (CONNACK, PUBACK, SUBACK, etc.)
    // Multiple buffers needed because io_uring sends are async - can't reuse
    // until CQE arrives. Rotation allows multiple concurrent responses.
    u8 proto_buf[LLMQ_PROTO_BUF_COUNT][LLMQ_PROTO_BUF_SIZE];
    u8 proto_buf_idx; // Next buffer slot to use (rotates 0..COUNT-1)

    // === Session (persists when DORMANT if clean_session=0) ===
    // Subscriptions tracked in trie via slot index

    // Pending messages for offline delivery (QoS 1/2)
    struct pending_msg pending[MAX_PENDING_MSGS];
    u8 pending_head;
    u8 pending_tail;
    u8 pending_count;
    u8 _pad2;

    // === QoS 1/2 Inflight tracking ===
    u16 next_packet_id; // Next packet ID to allocate (wraps at 65535)
    u8 inflight_count;  // Active inflight messages
    u8 _pad3;
    struct inflight_msg inflight[MAX_INFLIGHT]; // Inflight message slots
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

// Initialize a slot for a new client
// Note: protocol_version=0 means CONNECT not yet received
INLINE void client_init(struct client_slot *c, i32 fd) {
    c->state            = CLIENT_ACTIVE;
    c->fd               = fd;
    c->client_id_len    = 0;
    c->client_id_hash   = 0;
    c->clean_session    = 1;
    c->protocol_version = 0; // 0 = awaiting CONNECT
    c->keepalive        = 0;
    c->last_active      = 0;
    c->recv_len         = 0;
    c->proto_buf_idx    = 0;
    c->pending_head     = 0;
    c->pending_tail     = 0;
    c->pending_count    = 0;
    c->next_packet_id   = 1;
    c->inflight_count   = 0;
    for (u8 i = 0; i < MAX_INFLIGHT; i++) {
        c->inflight[i].state = INFLIGHT_FREE;
    }
}

// Get next protocol response buffer (rotates through available slots)
// Each call returns a different buffer to avoid overwriting pending async sends
// proto_buf_count: runtime limit from broker config
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
INLINE void client_go_dormant(struct client_slot *c) {
    c->state    = CLIENT_DORMANT;
    c->fd       = -1;
    c->recv_len = 0;
    // Session data (subscriptions, pending) preserved
}

// Free the slot completely
INLINE void client_free(struct client_slot *c) {
    c->state          = CLIENT_FREE;
    c->fd             = -1;
    c->client_id_len  = 0;
    c->client_id_hash = 0;
    c->recv_len       = 0;
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
// Pending Message Operations
// =============================================================================

INLINE bool client_has_pending(struct client_slot *c) {
    return c->pending_count > 0;
}

INLINE struct pending_msg *client_pending_push(struct client_slot *c) {
    if (c->pending_count >= MAX_PENDING_MSGS) {
        return NULL; // Queue full
    }
    struct pending_msg *msg = &c->pending[c->pending_tail];
    c->pending_tail         = (c->pending_tail + 1) % MAX_PENDING_MSGS;
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
    c->pending_head = (c->pending_head + 1) % MAX_PENDING_MSGS;
    c->pending_count--;
}

// =============================================================================
// Inflight Message Operations (QoS 1/2 tracking)
// =============================================================================

// Allocate a packet ID for outgoing QoS 1/2 message
// max_inflight: runtime limit from broker config
// Returns: inflight index, or -1 if full
INLINE i32 client_inflight_alloc(struct client_slot *c, u8 qos, u16 *out_packet_id, u8 max_inflight) {
    if (c->inflight_count >= max_inflight) {
        return -1; // Quota exceeded
    }

    // Find free slot (search up to runtime limit)
    for (u8 i = 0; i < max_inflight; i++) {
        if (c->inflight[i].state == INFLIGHT_FREE) {
            // Allocate unique packet ID
            u16 start_id = c->next_packet_id;
            do {
                c->next_packet_id++;
                if (c->next_packet_id == 0)
                    c->next_packet_id = 1; // 0 is invalid

                // Check if ID is in use
                bool in_use = false;
                for (u8 j = 0; j < max_inflight; j++) {
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
                    // Set state based on QoS level
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

            return -1; // All IDs in use (shouldn't happen with count check)
        }
    }
    return -1;
}

// Start tracking an incoming QoS 2 message (we received a PUBLISH)
// Returns: inflight index, or -1 if full
INLINE i32 client_inflight_track_incoming(struct client_slot *c, u16 packet_id) {
    if (c->inflight_count >= MAX_INFLIGHT) {
        return -1;
    }

    // Check for duplicate packet ID
    for (u8 i = 0; i < MAX_INFLIGHT; i++) {
        if (c->inflight[i].state != INFLIGHT_FREE && c->inflight[i].packet_id == packet_id &&
            c->inflight[i].direction == 1) {
            // Already tracking this - return existing index
            return (i32)i;
        }
    }

    // Find free slot
    for (u8 i = 0; i < MAX_INFLIGHT; i++) {
        if (c->inflight[i].state == INFLIGHT_FREE) {
            c->inflight[i].packet_id = packet_id;
            c->inflight[i].qos       = 2;
            c->inflight[i].dup_count = 0;
            c->inflight[i].direction = 1; // incoming
            c->inflight[i].data_len  = 0;
            c->inflight[i].state     = INFLIGHT_WAIT_PUBREL;
            c->inflight_count++;
            return (i32)i;
        }
    }
    return -1;
}

// Find inflight entry by packet ID
INLINE struct inflight_msg *client_inflight_find(struct client_slot *c, u16 packet_id) {
    for (u8 i = 0; i < MAX_INFLIGHT; i++) {
        if (c->inflight[i].state != INFLIGHT_FREE && c->inflight[i].packet_id == packet_id) {
            return &c->inflight[i];
        }
    }
    return NULL;
}

// Free an inflight entry
INLINE void client_inflight_free(struct client_slot *c, struct inflight_msg *msg) {
    if (msg->state != INFLIGHT_FREE) {
        msg->state = INFLIGHT_FREE;
        if (c->inflight_count > 0)
            c->inflight_count--;
    }
}

// Get inflight entry buffer for encoding
INLINE u8 *client_inflight_buffer(struct inflight_msg *msg) {
    return msg->send_buf;
}

#endif // BROKER_CLIENT_H
