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

// =============================================================================
// Client State
// =============================================================================

enum client_state {
    CLIENT_FREE    = 0, // Slot available
    CLIENT_ACTIVE  = 1, // Connected and operational
    CLIENT_DORMANT = 2, // Disconnected, session preserved (clean_session=0)
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

    // === Session (persists when DORMANT if clean_session=0) ===
    // Subscriptions tracked in trie via slot index

    // Pending messages for offline delivery (QoS 1/2)
    struct pending_msg pending[MAX_PENDING_MSGS];
    u8 pending_head;
    u8 pending_tail;
    u8 pending_count;
    u8 _pad2;

    // === QoS 1/2 Inflight tracking ===
    u16 next_packet_id;
    // TODO: Add inflight message tracking for QoS 1/2
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
    c->pending_head     = 0;
    c->pending_tail     = 0;
    c->pending_count    = 0;
    c->next_packet_id   = 1;
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

#endif // BROKER_CLIENT_H
