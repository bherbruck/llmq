// broker/client.h - Client slot with dynamic buffer allocation
// Uses pool allocators for large buffers instead of embedding them

#ifndef BROKER_CLIENT_H
#define BROKER_CLIENT_H

#include "sys/types.h"
#include "config.h"
#include "mem/pool.h"
#include "mem/msgbuf.h"

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
    CLIENT_CLOSING = 3, // Close submitted, waiting for CQE (don't submit new ops)
};

// =============================================================================
// Inflight Message (QoS 1/2 tracking) - simplified metadata only (~16 bytes)
// =============================================================================

enum inflight_state {
    INFLIGHT_FREE         = 0, // Slot available
    INFLIGHT_SENDING      = 1, // Send in progress (QoS 0 or initial QoS 1/2)
    INFLIGHT_WAIT_PUBACK  = 2, // QoS 1: sent PUBLISH, waiting for PUBACK
    INFLIGHT_WAIT_PUBREC  = 3, // QoS 2 sender: sent PUBLISH, waiting for PUBREC
    INFLIGHT_WAIT_PUBREL  = 4, // QoS 2 receiver: sent PUBREC, waiting for PUBREL
    INFLIGHT_WAIT_PUBCOMP = 5, // QoS 2 sender: sent PUBREL, waiting for PUBCOMP
};

// Inflight message entry - simplified metadata only
// The actual send state (header, iovecs) lives in send_desc_pool
struct inflight_msg {
    u32 msg_idx;         // Index into canonical_msg pool (MSG_POOL_INVALID if none)
    u32 send_desc_idx;   // Active send_desc (SEND_DESC_INVALID if not sending)
    u64 deadline;        // Monotonic timestamp for timeout (0 = no timeout)
    u16 packet_id;       // MQTT packet identifier (native endian for lookups)
    u8 packet_id_be[2];  // Big-endian packet ID for protocol responses
    u8 state;            // enum inflight_state
    u8 qos;              // Original QoS level
    u8 dup_count;        // Retransmission count
    u8 direction;        // 0 = outgoing (we sent), 1 = incoming (we received)
};

STATIC_ASSERT(sizeof(struct inflight_msg) == 24, "inflight_msg should be 24 bytes");

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
    u8 generation;       // Incremented each time slot is reused (for stale CQE detection)

    // === Connection (valid when ACTIVE) ===
    i32 fd;        // Socket fd (-1 when DORMANT/FREE)
    u16 keepalive; // Keepalive interval (seconds)
    u8 recv_pending; // 1 if recv is in-flight (kernel may write to buffer)
    u8 _pad1;
    u32 last_active; // Timestamp of last packet

    // Receive buffer (dynamically allocated)
    u32 recv_buf_idx;          // Index into recv buffer pool (BUF_POOL_INVALID if none)
    u32 recv_len;              // Bytes in receive buffer
    u32 orphaned_recv_buf_idx; // Buffer from prev connection awaiting stale recv CQE

    // Scratch area for immediate protocol responses
    // Multiple slots to avoid race: async send may not complete before next response
    #define RESP_SLOTS 16
    u8 resp_pkt_id[RESP_SLOTS][2];  // Big-endian packet_id for PUBACK/PUBREC/etc
    u8 resp_buf[RESP_SLOTS][16];    // Small responses (SUBACK, etc.)
    struct iovec resp_iov[RESP_SLOTS][3]; // Scatter-gather for response
    u8 resp_slot;                   // Next response slot hint (circular)
    u16 resp_in_flight;             // Bitmask: 1 = slot has pending async send

    // === Session (persists when DORMANT if clean_session=0) ===
    // Subscriptions tracked in trie via slot index

    // Pending messages for offline delivery (small, kept inline)
    struct pending_msg pending[LLMQ_MAX_PENDING_MSGS];
    u8 pending_head;
    u8 pending_tail;
    u8 pending_count;
    u8 _pad2;

    // === QoS 1/2 Inflight tracking (metadata inline, send state pooled) ===
    u16 next_packet_id;                              // Next packet ID to allocate (wraps at 65535)
    u16 inflight_count;                              // Active inflight messages
    u16 max_inflight;                                // Runtime limit for this client (must be power of 2)
    u16 inflight_mask;                               // max_inflight - 1 for fast modulo via &
    u16 inflight_free_hint;                          // Hint for next free slot (O(1) alloc)
    u16 inflight_generation;                         // Generation counter for packet_id cycling
    struct inflight_msg inflight[LLMQ_MAX_INFLIGHT]; // Small metadata array

    // Packet ID hash table for O(1) lookup (packet_id → inflight slot)
    // Size 2x max_inflight for good load factor with linear probing
    #define INFLIGHT_HASH_SIZE 512
    #define INFLIGHT_HASH_MASK (INFLIGHT_HASH_SIZE - 1)
    #define INFLIGHT_HASH_EMPTY 0xFFFF
    u16 inflight_pkt_hash[INFLIGHT_HASH_SIZE];       // packet_id hash → slot_idx
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
INLINE void client_init(struct client_slot *c, i32 fd, u32 recv_buf_idx, u16 max_inflight) {
    c->generation++;     // Increment to invalidate any stale CQEs from previous use
    c->state            = CLIENT_ACTIVE;
    c->fd               = fd;
    c->client_id_len    = 0;
    c->client_id_hash   = 0;
    c->clean_session    = 1;
    c->protocol_version = 0; // 0 = awaiting CONNECT
    c->keepalive        = 0;
    c->recv_pending     = 0; // No recv in-flight yet
    c->last_active      = 0;
    c->recv_buf_idx     = recv_buf_idx;
    c->recv_len         = 0;
    // Don't reset orphaned_recv_buf_idx - it may have a buffer waiting for stale CQE
    c->pending_head     = 0;
    c->pending_tail     = 0;
    c->pending_count    = 0;
    c->resp_slot        = 0;
    c->resp_in_flight   = 0; // Clear stale in-flight flags from previous connection
    c->next_packet_id       = 1;
    c->inflight_count       = 0;
    c->max_inflight         = max_inflight;
    c->inflight_mask        = max_inflight - 1;  // For fast modulo via &
    c->inflight_free_hint   = 0;
    c->inflight_generation  = 0;
    for (u16 i = 0; i < LLMQ_MAX_INFLIGHT; i++) {
        c->inflight[i].state         = INFLIGHT_FREE;
        c->inflight[i].msg_idx       = MSG_POOL_INVALID;
        c->inflight[i].send_desc_idx = SEND_DESC_INVALID;
    }
    // Initialize packet_id hash table to empty
    for (u32 i = 0; i < INFLIGHT_HASH_SIZE; i++) {
        c->inflight_pkt_hash[i] = INFLIGHT_HASH_EMPTY;
    }
}

// Get receive buffer pointer (requires pool reference)
INLINE u8 *client_recv_buf(struct client_slot *c, struct buf_pool *recv_pool) {
    if (c->recv_buf_idx == BUF_POOL_INVALID) {
        return NULL;
    }
    return buf_pool_get(recv_pool, c->recv_buf_idx);
}

// Setup response iovec for a 4-byte packet (header + packet_id)
// Get next response slot (advances counter, returns slot index)
// Get a response slot, skipping in-flight slots
// Returns RESP_SLOTS if all slots are busy (caller should fall back to sync)
INLINE u8 client_get_resp_slot(struct client_slot *c) {
    for (u8 i = 0; i < RESP_SLOTS; i++) {
        u8 slot = (c->resp_slot + i) % RESP_SLOTS;
        if (!(c->resp_in_flight & (1 << slot))) {
            c->resp_slot = (slot + 1) % RESP_SLOTS;
            return slot;
        }
    }
    return RESP_SLOTS; // All slots busy
}

// Mark a response slot as in-flight (async send pending)
INLINE void client_resp_slot_mark_inflight(struct client_slot *c, u8 slot) {
    c->resp_in_flight |= (1 << slot);
}

// Clear in-flight flag when async send completes
INLINE void client_resp_slot_complete(struct client_slot *c, u8 slot) {
    c->resp_in_flight &= ~(1 << slot);
}

// Setup response iovec for a 4-byte packet (header + packet_id)
// Returns pointer to iovec array for use with writev, advances slot counter
INLINE struct iovec *client_setup_resp_pkt(struct client_slot *c, const u8 *hdr, u16 packet_id) {
    u8 slot = client_get_resp_slot(c);

    c->resp_pkt_id[slot][0]       = (u8)(packet_id >> 8);
    c->resp_pkt_id[slot][1]       = (u8)(packet_id & 0xFF);
    c->resp_iov[slot][0].iov_base = (void *)hdr;
    c->resp_iov[slot][0].iov_len  = 2;
    c->resp_iov[slot][1].iov_base = c->resp_pkt_id[slot];
    c->resp_iov[slot][1].iov_len  = 2;

    return c->resp_iov[slot];
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
// Packet ID Hash Table Operations (O(1) lookup for any packet_id)
// =============================================================================

// Insert packet_id → slot mapping into hash table
INLINE void inflight_hash_insert(struct client_slot *c, u16 packet_id, u16 slot) {
    u32 idx = packet_id & INFLIGHT_HASH_MASK;
    // Linear probing
    for (u32 probe = 0; probe < INFLIGHT_HASH_SIZE; probe++) {
        if (c->inflight_pkt_hash[idx] == INFLIGHT_HASH_EMPTY) {
            c->inflight_pkt_hash[idx] = slot;
            return;
        }
        idx = (idx + 1) & INFLIGHT_HASH_MASK;
    }
    // Table full - shouldn't happen if sized correctly (2x max_inflight)
}

// Remove packet_id from hash table
INLINE void inflight_hash_remove(struct client_slot *c, u16 packet_id) {
    u32 idx = packet_id & INFLIGHT_HASH_MASK;
    // Linear probing to find the entry
    for (u32 probe = 0; probe < INFLIGHT_HASH_SIZE; probe++) {
        u16 slot = c->inflight_pkt_hash[idx];
        if (slot == INFLIGHT_HASH_EMPTY) {
            return; // Not found
        }
        if (c->inflight[slot].packet_id == packet_id) {
            c->inflight_pkt_hash[idx] = INFLIGHT_HASH_EMPTY;
            return;
        }
        idx = (idx + 1) & INFLIGHT_HASH_MASK;
    }
}

// =============================================================================
// Inflight Message Operations (simplified - no embedded iovecs)
// =============================================================================

// Error codes for client_inflight_alloc
#define INFLIGHT_ERR_CLIENT_FULL (-1) // Per-client inflight limit exceeded

// Allocate inflight slot for QoS 1/2 message - O(1) using direct indexing
// Packet ID is chosen so that (packet_id - 1) % max_inflight = slot_index
// deadline: monotonic timestamp when this entry expires (0 = no timeout)
// Returns: inflight index (>=0), or INFLIGHT_ERR_CLIENT_FULL
INLINE i32 client_inflight_alloc(struct client_slot *c, u8 qos, u32 msg_idx, u64 deadline,
                                  u16 *out_packet_id) {
    if (unlikely(c->inflight_count >= c->max_inflight)) {
        return INFLIGHT_ERR_CLIENT_FULL;
    }

    // Find free slot using free list hint, fall back to linear if needed
    u16 slot = c->inflight_free_hint;
    u16 searched = 0;
    // Hot path: free_hint usually points to free slot
    while (unlikely(c->inflight[slot].state != INFLIGHT_FREE) && searched < c->max_inflight) {
        slot = (slot + 1) & c->inflight_mask;
        searched++;
    }
    if (unlikely(c->inflight[slot].state != INFLIGHT_FREE)) {
        return -1; // Should never happen if count is accurate
    }

    // Assign packet_id such that (packet_id - 1) % max_inflight == slot
    // This ensures O(1) lookup later
    // packet_id = slot + 1 + (generation * max_inflight), where generation cycles
    u16 base_id = slot + 1;
    u16 packet_id = base_id + (c->inflight_generation * c->max_inflight);
    if (packet_id == 0) packet_id = base_id; // Avoid 0

    *out_packet_id                    = packet_id;
    c->inflight[slot].packet_id       = packet_id;
    c->inflight[slot].packet_id_be[0] = (u8)(packet_id >> 8);
    c->inflight[slot].packet_id_be[1] = (u8)(packet_id & 0xFF);
    c->inflight[slot].qos             = qos;
    c->inflight[slot].dup_count       = 0;
    c->inflight[slot].direction       = 0; // outgoing
    c->inflight[slot].msg_idx         = msg_idx;
    c->inflight[slot].send_desc_idx   = SEND_DESC_INVALID;
    c->inflight[slot].deadline        = deadline;
    c->inflight[slot].state           = (qos == 1) ? INFLIGHT_WAIT_PUBACK : INFLIGHT_WAIT_PUBREC;

    c->inflight_count++;
    c->inflight_free_hint = (slot + 1) & c->inflight_mask;

    // Insert into hash table for O(1) lookup
    inflight_hash_insert(c, packet_id, slot);

    // Advance generation when we wrap around slot 0
    if (slot == 0) {
        c->inflight_generation++;
    }

    return (i32)slot;
}

// Track incoming QoS 2 message (no buffer needed - we're receiving, not sending)
// Optimized: use packet_id % max_inflight as hint for both duplicate check and allocation
// deadline: monotonic timestamp when this entry expires (0 = no timeout)
INLINE i32 client_inflight_track_incoming(struct client_slot *c, u16 packet_id, u64 deadline) {
    if (c->inflight_count >= c->max_inflight) {
        return -1;
    }

    // Check for duplicate - O(1) hint first, then linear fallback
    u16 hint = (packet_id - 1) & c->inflight_mask;
    if (c->inflight[hint].state != INFLIGHT_FREE && c->inflight[hint].packet_id == packet_id &&
        c->inflight[hint].direction == 1) {
        return (i32)hint; // Found duplicate at hint slot
    }
    // Linear fallback for duplicate check (rare if clients use sequential IDs)
    for (u16 i = 0; i < c->max_inflight; i++) {
        if (c->inflight[i].state != INFLIGHT_FREE && c->inflight[i].packet_id == packet_id &&
            c->inflight[i].direction == 1) {
            return (i32)i;
        }
    }

    // Find free slot - use free_hint for O(1) typical case
    u16 slot = c->inflight_free_hint;
    u16 searched = 0;
    while (c->inflight[slot].state != INFLIGHT_FREE && searched < c->max_inflight) {
        slot = (slot + 1) & c->inflight_mask;
        searched++;
    }
    if (c->inflight[slot].state != INFLIGHT_FREE) {
        return -1; // No free slots
    }

    c->inflight[slot].packet_id       = packet_id;
    c->inflight[slot].packet_id_be[0] = (u8)(packet_id >> 8);
    c->inflight[slot].packet_id_be[1] = (u8)(packet_id & 0xFF);
    c->inflight[slot].qos             = 2;
    c->inflight[slot].dup_count       = 0;
    c->inflight[slot].direction       = 1;                  // incoming
    c->inflight[slot].msg_idx         = MSG_POOL_INVALID;   // No buffer needed
    c->inflight[slot].send_desc_idx   = SEND_DESC_INVALID;
    c->inflight[slot].deadline        = deadline;
    c->inflight[slot].state           = INFLIGHT_WAIT_PUBREL;
    c->inflight_count++;
    c->inflight_free_hint = (slot + 1) & c->inflight_mask;

    // Insert into hash table for O(1) lookup
    inflight_hash_insert(c, packet_id, slot);

    return (i32)slot;
}

// Find inflight entry by packet ID - O(1) via hash table
INLINE struct inflight_msg *client_inflight_find(struct client_slot *c, u16 packet_id) {
    if (unlikely(packet_id == 0)) return NULL;

    // Hash table lookup - O(1) average
    u32 idx = packet_id & INFLIGHT_HASH_MASK;
    for (u32 probe = 0; probe < INFLIGHT_HASH_SIZE; probe++) {
        u16 slot = c->inflight_pkt_hash[idx];
        if (slot == INFLIGHT_HASH_EMPTY) {
            return NULL; // Not found
        }
        // Verify packet_id match (handle hash collisions)
        if (slot < c->max_inflight &&
            c->inflight[slot].state != INFLIGHT_FREE &&
            c->inflight[slot].packet_id == packet_id) {
            return &c->inflight[slot];
        }
        idx = (idx + 1) & INFLIGHT_HASH_MASK;
    }
    return NULL;
}

// Free inflight entry
// Note: Caller must handle msg_pool refcount and send_desc_pool separately
INLINE void client_inflight_free(struct client_slot *c, struct inflight_msg *msg) {
    if (msg->state != INFLIGHT_FREE) {
        // Remove from hash table first (while packet_id is still valid)
        inflight_hash_remove(c, msg->packet_id);
        msg->msg_idx       = MSG_POOL_INVALID;
        msg->send_desc_idx = SEND_DESC_INVALID;
        msg->state         = INFLIGHT_FREE;
        if (c->inflight_count > 0)
            c->inflight_count--;
    }
}

// Free all inflight entries for a client
// Note: Caller must handle msg_pool refcounts separately
INLINE void client_inflight_free_all(struct client_slot *c) {
    // Early exit if nothing to free
    if (c->inflight_count == 0) {
        return;
    }
    // O(max_inflight) but only iterates until we've found all active entries
    u16 found = 0;
    for (u16 i = 0; i < LLMQ_MAX_INFLIGHT && found < c->inflight_count; i++) {
        if (c->inflight[i].state != INFLIGHT_FREE) {
            c->inflight[i].msg_idx       = MSG_POOL_INVALID;
            c->inflight[i].send_desc_idx = SEND_DESC_INVALID;
            c->inflight[i].state         = INFLIGHT_FREE;
            found++;
        }
    }
    c->inflight_count     = 0;
    c->inflight_free_hint = 0; // Reset hint since all are free

    // Clear hash table
    for (u32 i = 0; i < INFLIGHT_HASH_SIZE; i++) {
        c->inflight_pkt_hash[i] = INFLIGHT_HASH_EMPTY;
    }
}

#endif // BROKER_CLIENT_H
