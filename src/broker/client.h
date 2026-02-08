// broker/client.h - Client slot with dynamic buffer allocation
// Uses pool allocators for large buffers instead of embedding them

#ifndef BROKER_CLIENT_H
#define BROKER_CLIENT_H

#include "sys/types.h"
#include "config.h"
#include "mem/pool.h"
#include "mem/msgbuf.h"
#include "mem/string.h"
#include "broker/egress.h"

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
// Inflight Message (QoS 1/2 tracking) - HOT/COLD split for cache efficiency
// =============================================================================
//
// The ACK hot path (PUBREC/PUBCOMP) only needs to validate packet_id and state,
// then access msg_idx for refcount release. By splitting into hot/cold structs,
// the hot path loads 8 bytes instead of 32, improving cache density 4x.
//
// Hot array: 8 bytes/entry → 8 entries per cache line
// Cold array: 24 bytes/entry → accessed only on state transitions

enum inflight_state {
    INFLIGHT_FREE         = 0, // Slot available
    INFLIGHT_SENDING      = 1, // Send in progress (QoS 0 or initial QoS 1/2)
    INFLIGHT_WAIT_PUBACK  = 2, // QoS 1: sent PUBLISH, waiting for PUBACK
    INFLIGHT_WAIT_PUBREC  = 3, // QoS 2 sender: sent PUBLISH, waiting for PUBREC
    INFLIGHT_WAIT_PUBREL  = 4, // QoS 2 receiver: sent PUBREC, waiting for PUBREL
    INFLIGHT_WAIT_PUBCOMP = 5, // QoS 2 sender: sent PUBREL, waiting for PUBCOMP
};

// Hot path data - accessed on every ACK lookup (8 bytes)
struct inflight_hot {
    u16 packet_id;       // MQTT packet identifier (native endian for lookups)
    u8 state;            // enum inflight_state
    u8 direction;        // 0 = outgoing (we sent), 1 = incoming (we received)
    u32 msg_idx;         // Index into canonical_msg pool (MSG_POOL_INVALID if none)
};

STATIC_ASSERT(sizeof(struct inflight_hot) == 8, "inflight_hot should be 8 bytes");

// Cold path data - accessed only on state transitions (16 bytes)
// Ordered for optimal packing: u64 first for natural alignment
struct inflight_cold {
    u64 deadline;        // Monotonic timestamp for timeout (0 = no timeout)
    u8 packet_id_be[2];  // Big-endian packet ID for protocol responses
    u8 qos;              // Original QoS level
    u8 dup_count;        // Retransmission count
    u8 _pad[4];          // Pad to 16 bytes
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
    u8 generation;       // Incremented each time slot is reused (for stale CQE detection)

    // === Connection (valid when ACTIVE) ===
    i32 fd;        // Socket fd (-1 when DORMANT/FREE)
    u16 keepalive; // Keepalive interval (seconds)
    u8 recv_pending; // 1 if recv is in-flight (kernel may write to buffer)
    u8 _pad1;
    u32 last_active; // Timestamp of last packet

    // Receive buffer (dynamically allocated)
    u32 recv_buf_idx;          // Index into recv buffer pool (BUF_POOL_INVALID if none)
    u32 recv_start;            // Offset where unread data begins (allows no-copy advance)
    u32 recv_len;              // Bytes of valid data starting at recv_start
    u32 orphaned_recv_buf_idx; // Buffer from prev connection awaiting stale recv CQE

    // Scratch area for immediate protocol responses
    // Multiple slots to avoid race: async send may not complete before next response
    #define RESP_SLOTS 16
    #define RESP_BUF_SIZE 16
    u8 resp_pkt_id[RESP_SLOTS][2];  // Big-endian packet_id for PUBACK/PUBREC/etc
    u8 resp_buf[RESP_SLOTS][RESP_BUF_SIZE]; // Small responses (SUBACK, etc.)
    struct iovec resp_iov[RESP_SLOTS][3]; // Scatter-gather for response
    u8 resp_slot;                   // Next response slot hint (circular)
    u16 resp_in_flight;             // Bitmask: 1 = slot has pending async send

    // === Session (persists when DORMANT if clean_session=0) ===
    // Subscriptions tracked in trie via slot index

    // Pending messages for offline delivery (arena-allocated, not embedded)
    struct pending_msg *pending;
    u8 pending_head;
    u8 pending_tail;
    u8 pending_count;
    u8 _pad2;

    // === QoS 1/2 Inflight tracking (hot/cold split for cache efficiency) ===
    // Arrays are arena-allocated (not embedded) to keep client_slot compact for cache.
    // At 1000 clients, embedded 68KB structs = 68MB working set (exceeds L3).
    // With pointers, client_slot is ~1.2KB → 1000 clients = 1.2MB (fits in L3).
    u16 next_packet_id;                               // Next packet ID to allocate (wraps at 65535)
    u16 inflight_count;                               // Active inflight messages
    u16 max_inflight;                                 // Runtime limit for this client (must be power of 2)
    u16 inflight_mask;                                // max_inflight - 1 for fast modulo via &
    u16 inflight_free_hint;                           // Hint for next free slot (O(1) alloc)
    u16 inflight_generation;                          // Generation counter for packet_id cycling
    u16 inflight_hash_mask;                           // Hash table mask (2 * max_inflight - 1)
    u16 _pad3;
    struct inflight_hot *inflight_hot;                // Hot: packet_id, state, msg_idx (8B each)
    struct inflight_cold *inflight_cold;              // Cold: deadline, qos, etc (24B each)

    // Packet ID hash table for O(1) lookup (packet_id → inflight slot)
    // Size 2x max_inflight for good load factor with linear probing
    #define INFLIGHT_HASH_EMPTY 0xFFFF
    u16 *inflight_pkt_hash;                           // packet_id hash → slot_idx

    // === Per-client egress queue (arena-allocated ring buffer) ===
    struct egress_segment *egress;   // Ring buffer of segments
    u16 egress_head;                 // Ring head index
    u16 egress_count;                // Current occupancy
    u16 egress_capacity;             // Ring size (power of 2)
    u16 egress_mask;                 // capacity - 1

    u8 egress_inflight;              // 1 if a writev is in-flight for this client
    u8 egress_retry_queued;          // 1 if queued in global egress flush retry queue
    u8 egress_batch_count;           // Segments in current batched writev (1..EGRESS_BATCH_MAX)
    u8 egress_batch_qos0_refs;       // 1 if batch has QoS 0 PUBLISH needing ref release
    u32 egress_batch_wire_len;       // Total wire bytes for current batch (fast-path CQE)

    // Scratch area for materializing batched iov at submit time.
    // Valid only while egress_inflight == 1 (one writev per client).
    // Up to EGRESS_BATCH_MAX PUBLISH messages per writev to reduce SQE/CQE volume.
    u8 egress_headers[EGRESS_BATCH_MAX][7]; // Materialized PUBLISH fixed headers
    u8 egress_pkt_ids[EGRESS_BATCH_MAX][2]; // Big-endian packet_ids for iov
    u8 _pad_scratch[2];
    struct iovec egress_iov[EGRESS_BATCH_MAX * 4]; // [hdr][topic][pktid][payload] × batch
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
    c->recv_start       = 0;
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
    c->inflight_hash_mask   = 2 * max_inflight - 1;
    // Fast init: memset arrays to 0xFF (sets msg_idx to INVALID)
    // Then fix up state field which needs to be 0 (INFLIGHT_FREE)
    memset(c->inflight_hot, 0xFF, (usize)max_inflight * sizeof(struct inflight_hot));
    memset(c->inflight_cold, 0xFF, (usize)max_inflight * sizeof(struct inflight_cold));
    for (u16 i = 0; i < max_inflight; i++) {
        c->inflight_hot[i].state = INFLIGHT_FREE;
    }
    // Initialize hash table with memset (INFLIGHT_HASH_EMPTY = 0xFFFF = all 1s)
    memset(c->inflight_pkt_hash, 0xFF, (usize)(2 * max_inflight) * sizeof(u16));

    // Egress queue state
    c->egress_head      = 0;
    c->egress_count     = 0;
    c->egress_inflight        = 0;
    c->egress_retry_queued    = 0;
    c->egress_batch_count     = 0;
    c->egress_batch_qos0_refs = 0;
    c->egress_batch_wire_len  = 0;
    // egress, egress_capacity, egress_mask set by broker_init wiring
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
        u8 slot = (c->resp_slot + i) & (RESP_SLOTS - 1);
        if (!(c->resp_in_flight & (1 << slot))) {
            c->resp_slot = (slot + 1) & (RESP_SLOTS - 1);
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
    c->recv_start   = 0;
    c->recv_len     = 0;
    c->recv_buf_idx = BUF_POOL_INVALID; // Caller should have freed it
    c->egress_inflight = 0;
    c->egress_retry_queued = 0;
    // Session data (subscriptions, pending) preserved
}

// Free the slot completely
// Note: All pooled buffers should be freed separately
INLINE void client_free(struct client_slot *c) {
    c->state          = CLIENT_FREE;
    c->fd             = -1;
    c->client_id_len  = 0;
    c->client_id_hash = 0;
    c->recv_start     = 0;
    c->recv_len       = 0;
    c->recv_buf_idx   = BUF_POOL_INVALID;
    c->pending_head   = 0;
    c->pending_tail   = 0;
    c->pending_count  = 0;
    c->egress_inflight = 0;
    c->egress_retry_queued = 0;
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
    c->pending_tail         = (c->pending_tail + 1) & (LLMQ_MAX_PENDING_MSGS - 1);
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
    c->pending_head = (c->pending_head + 1) & (LLMQ_MAX_PENDING_MSGS - 1);
    c->pending_count--;
}

// =============================================================================
// Packet ID Hash Table Operations (O(1) lookup for any packet_id)
// =============================================================================

// Insert packet_id → slot mapping into hash table
INLINE void inflight_hash_insert(struct client_slot *c, u16 packet_id, u16 slot) {
    u32 idx = packet_id & c->inflight_hash_mask;
    // Linear probing
    for (u32 probe = 0; probe < (c->inflight_hash_mask + 1); probe++) {
        if (c->inflight_pkt_hash[idx] == INFLIGHT_HASH_EMPTY) {
            c->inflight_pkt_hash[idx] = slot;
            return;
        }
        idx = (idx + 1) & c->inflight_hash_mask;
    }
    // Table full - shouldn't happen if sized correctly (2x max_inflight)
}

// Remove packet_id from hash table using backward-shift deletion.
// Simple EMPTY marking would break linear probing chains; instead we
// shift subsequent entries back to fill the gap, preserving probe order.
INLINE void inflight_hash_remove(struct client_slot *c, u16 packet_id) {
    u32 idx = packet_id & c->inflight_hash_mask;
    // Find the entry
    for (u32 probe = 0; probe < (c->inflight_hash_mask + 1); probe++) {
        u16 slot = c->inflight_pkt_hash[idx];
        if (slot == INFLIGHT_HASH_EMPTY) {
            return; // Not found
        }
        if (c->inflight_hot[slot].packet_id == packet_id) {
            // Found - backward shift to fill gap
            // Two pointers: 'empty' is the gap, 'j' scans forward independently
            u32 empty = idx;
            u32 j = idx;
            for (;;) {
                j = (j + 1) & c->inflight_hash_mask;
                u16 j_slot = c->inflight_pkt_hash[j];
                if (j_slot == INFLIGHT_HASH_EMPTY) {
                    break; // End of cluster
                }
                u32 natural = c->inflight_hot[j_slot].packet_id & c->inflight_hash_mask;
                // Shift if natural position is at or before the gap (with wraparound)
                bool should_shift = (j > empty)
                    ? (natural <= empty || natural > j)
                    : (natural <= empty && natural > j);
                if (should_shift) {
                    c->inflight_pkt_hash[empty] = j_slot;
                    empty = j;
                }
            }
            c->inflight_pkt_hash[empty] = INFLIGHT_HASH_EMPTY;
            return;
        }
        idx = (idx + 1) & c->inflight_hash_mask;
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
    while (unlikely(c->inflight_hot[slot].state != INFLIGHT_FREE) && searched < c->max_inflight) {
        slot = (slot + 1) & c->inflight_mask;
        searched++;
    }
    if (unlikely(c->inflight_hot[slot].state != INFLIGHT_FREE)) {
        return -1; // Should never happen if count is accurate
    }

    // Assign packet_id such that (packet_id - 1) % max_inflight == slot
    // This ensures O(1) lookup later
    // packet_id = slot + 1 + (generation * max_inflight), where generation cycles
    u16 base_id = slot + 1;
    u16 packet_id = base_id + (c->inflight_generation * c->max_inflight);
    if (packet_id == 0) packet_id = base_id; // Avoid 0

    *out_packet_id = packet_id;

    // Hot path data
    c->inflight_hot[slot].packet_id  = packet_id;
    c->inflight_hot[slot].direction  = 0; // outgoing
    c->inflight_hot[slot].msg_idx    = msg_idx;
    c->inflight_hot[slot].state      = (qos == 1) ? INFLIGHT_WAIT_PUBACK : INFLIGHT_WAIT_PUBREC;

    // Cold path data
    c->inflight_cold[slot].packet_id_be[0] = (u8)(packet_id >> 8);
    c->inflight_cold[slot].packet_id_be[1] = (u8)(packet_id & 0xFF);
    c->inflight_cold[slot].qos             = qos;
    c->inflight_cold[slot].dup_count       = 0;
    c->inflight_cold[slot].deadline        = deadline;

    c->inflight_count++;
    c->inflight_free_hint = (slot + 1) & c->inflight_mask;

    // Skip hash table for outgoing - direct indexing works since we control packet_id
    // Hash table only needed for incoming messages with arbitrary packet_ids

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
    if (c->inflight_hot[hint].state != INFLIGHT_FREE &&
        c->inflight_hot[hint].packet_id == packet_id &&
        c->inflight_hot[hint].direction == 1) {
        return (i32)hint; // Found duplicate at hint slot
    }
    // Linear fallback for duplicate check (rare if clients use sequential IDs)
    for (u16 i = 0; i < c->max_inflight; i++) {
        if (c->inflight_hot[i].state != INFLIGHT_FREE &&
            c->inflight_hot[i].packet_id == packet_id &&
            c->inflight_hot[i].direction == 1) {
            return (i32)i;
        }
    }

    // Find free slot - use free_hint for O(1) typical case
    u16 slot = c->inflight_free_hint;
    u16 searched = 0;
    while (c->inflight_hot[slot].state != INFLIGHT_FREE && searched < c->max_inflight) {
        slot = (slot + 1) & c->inflight_mask;
        searched++;
    }
    if (c->inflight_hot[slot].state != INFLIGHT_FREE) {
        return -1; // No free slots
    }

    // Hot path data
    c->inflight_hot[slot].packet_id  = packet_id;
    c->inflight_hot[slot].direction  = 1;                  // incoming
    c->inflight_hot[slot].msg_idx    = MSG_POOL_INVALID;   // No buffer needed
    c->inflight_hot[slot].state      = INFLIGHT_WAIT_PUBREL;

    // Cold path data
    c->inflight_cold[slot].packet_id_be[0] = (u8)(packet_id >> 8);
    c->inflight_cold[slot].packet_id_be[1] = (u8)(packet_id & 0xFF);
    c->inflight_cold[slot].qos             = 2;
    c->inflight_cold[slot].dup_count       = 0;
    c->inflight_cold[slot].deadline        = deadline;
    c->inflight_count++;
    c->inflight_free_hint = (slot + 1) & c->inflight_mask;

    // Insert into hash table for O(1) lookup
    inflight_hash_insert(c, packet_id, slot);

    return (i32)slot;
}

// Find inflight entry by packet ID - O(1) direct indexing only
// Use this when we KNOW we assigned the packet_id (all subscriber-side lookups)
// Returns slot index (>=0) or -1 if not found (stale ACK for freed slot)
INLINE i32 client_inflight_find_direct(struct client_slot *c, u16 packet_id) {
    if (unlikely(packet_id == 0)) return -1;

    u16 slot = (packet_id - 1) & c->inflight_mask;
    if (c->inflight_hot[slot].state != INFLIGHT_FREE &&
        c->inflight_hot[slot].packet_id == packet_id) {
        return (i32)slot;
    }
    return -1; // Stale ACK - slot reused or freed
}

// Find inflight entry by packet ID - O(1) direct indexing for outgoing messages
// Falls back to hash table for incoming messages with arbitrary packet_ids
// Use client_inflight_find_direct() when you know the packet_id was assigned by us
// Returns slot index (>=0) or -1 if not found
INLINE i32 client_inflight_find(struct client_slot *c, u16 packet_id) {
    if (unlikely(packet_id == 0)) return -1;

    // Fast path: direct index for outgoing messages (most common case)
    u16 slot = (packet_id - 1) & c->inflight_mask;
    if (likely(c->inflight_hot[slot].state != INFLIGHT_FREE &&
               c->inflight_hot[slot].packet_id == packet_id)) {
        return (i32)slot;
    }

    // Slow path: hash table for incoming messages with arbitrary packet_ids
    u32 idx = packet_id & c->inflight_hash_mask;
    for (u32 probe = 0; probe < (c->inflight_hash_mask + 1); probe++) {
        slot = c->inflight_pkt_hash[idx];
        if (slot == INFLIGHT_HASH_EMPTY) {
            return -1;
        }
        if (slot < c->max_inflight &&
            c->inflight_hot[slot].state != INFLIGHT_FREE &&
            c->inflight_hot[slot].packet_id == packet_id) {
            return (i32)slot;
        }
        idx = (idx + 1) & c->inflight_hash_mask;
    }
    return -1;
}

// Free inflight entry by slot index
// Note: Caller must handle msg_pool refcount separately
INLINE void client_inflight_free_slot(struct client_slot *c, u16 slot) {
    if (c->inflight_hot[slot].state != INFLIGHT_FREE) {
        // Only remove from hash table for incoming messages (direction=1)
        // Outgoing messages use direct indexing, not hash table
        if (c->inflight_hot[slot].direction == 1) {
            inflight_hash_remove(c, c->inflight_hot[slot].packet_id);
        }
        c->inflight_hot[slot].msg_idx       = MSG_POOL_INVALID;
        c->inflight_hot[slot].state         = INFLIGHT_FREE;
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

    // Fast reset: memset to 0xFF then fix state fields
    memset(c->inflight_hot, 0xFF, (usize)c->max_inflight * sizeof(struct inflight_hot));
    memset(c->inflight_cold, 0xFF, (usize)c->max_inflight * sizeof(struct inflight_cold));
    for (u16 i = 0; i < c->max_inflight; i++) {
        c->inflight_hot[i].state = INFLIGHT_FREE;
    }
    c->inflight_count     = 0;
    c->inflight_free_hint = 0;

    // Clear hash table with memset
    memset(c->inflight_pkt_hash, 0xFF, (usize)(c->inflight_hash_mask + 1) * sizeof(u16));
}

#endif // BROKER_CLIENT_H
