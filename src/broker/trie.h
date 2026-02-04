// broker/trie.h - Topic trie for subscription matching
// O(1) child lookup via hash table, O(topic_levels) total lookup with wildcard support

#ifndef BROKER_TRIE_H
#define BROKER_TRIE_H

#include "sys/types.h"
#include "mem/string.h"
#include "mqtt/packet.h"
#include "config.h"

// =============================================================================
// Configuration (from config.h, with local aliases)
// =============================================================================

#define TRIE_MAX_NODES    LLMQ_TRIE_MAX_NODES
#define TRIE_MAX_CHILDREN LLMQ_TRIE_MAX_CHILDREN
#define TRIE_MAX_SEGMENT  LLMQ_TRIE_MAX_SEGMENT
#define BITS_PER_SLOT     64                                 // Bits per u64 bitmap slot
#define TRIE_FD_SLOTS     (LLMQ_MAX_CLIENTS / BITS_PER_SLOT) // Scales with max clients

// Hash table size for child lookup (power of 2, >= TRIE_MAX_CHILDREN)
#define TRIE_CHILD_HASH_SIZE 64
#define TRIE_CHILD_HASH_MASK (TRIE_CHILD_HASH_SIZE - 1)

// =============================================================================
// Hash Function (FNV-1a for segment names)
// =============================================================================

#define TRIE_FNV_OFFSET 2166136261u
#define TRIE_FNV_PRIME  16777619u

INLINE u32 trie_hash_segment(const u8 *seg, u8 len) {
    u32 hash = TRIE_FNV_OFFSET;
    for (u8 i = 0; i < len; i++) {
        hash ^= seg[i];
        hash *= TRIE_FNV_PRIME;
    }
    return hash;
}

// =============================================================================
// Trie Node
// =============================================================================

struct trie_node {
    // Subscriber tracking (fd bitmap + generation for slot reuse detection)
    u64 fd_bitmap[TRIE_FD_SLOTS];                    // 1024 bits for slot_idx
    u8 fd_gen[TRIE_FD_SLOTS * BITS_PER_SLOT];        // Generation when subscribed
    u32 sub_count;                                   // Active subscriber count

    // Trie structure
    u32 parent;                             // Parent node index
    u32 plus_child;                         // Direct '+' wildcard child (0 = none)
    u32 hash_child;                         // Direct '#' wildcard child (0 = none)
    u32 child_hash[TRIE_CHILD_HASH_SIZE];   // Hash table: segment hash → child_idx (0 = empty)
    u32 children[TRIE_MAX_CHILDREN];        // Linear array for iteration
    u16 child_count;

    // Segment name
    u8 name[TRIE_MAX_SEGMENT];
    u8 name_len;

    // Wildcard flags
    u8 is_plus; // '+' single-level wildcard
    u8 is_hash; // '#' multi-level wildcard
};

// =============================================================================
// Trie Manager
// =============================================================================

struct topic_trie {
    struct trie_node nodes[TRIE_MAX_NODES];
    u32 free_head;  // Freelist head
    u32 node_count; // Allocated nodes
};

// =============================================================================
// Initialization
// =============================================================================

INLINE void trie_init(struct topic_trie *t) {
    memset(t, 0, sizeof(*t));

    // Initialize freelist (skip node 0 - it's the root)
    for (u32 i = 1; i < TRIE_MAX_NODES - 1; i++) {
        t->nodes[i].children[0] = i + 1;
    }
    t->nodes[TRIE_MAX_NODES - 1].children[0] = 0; // End of list
    t->free_head                             = 1;
    t->node_count                            = 1; // Root is always allocated
}

// =============================================================================
// Node Allocation
// =============================================================================

INLINE u32 trie_alloc(struct topic_trie *t) {
    if (t->free_head == 0) {
        return 0; // Pool exhausted
    }

    u32 idx      = t->free_head;
    t->free_head = t->nodes[idx].children[0];
    t->node_count++;

    memset(&t->nodes[idx], 0, sizeof(struct trie_node));
    return idx;
}

INLINE void trie_free(struct topic_trie *t, u32 idx) {
    if (idx == 0)
        return; // Don't free root
    t->nodes[idx].children[0] = t->free_head;
    t->free_head              = idx;
    t->node_count--;
}

// =============================================================================
// Child Node Operations (O(1) via hash table and direct wildcard pointers)
// =============================================================================

// Find child matching segment - O(1) average via hash table
INLINE u32 trie_find_child(struct topic_trie *t, u32 node_idx, const u8 *seg, u8 seg_len) {
    struct trie_node *node = &t->nodes[node_idx];
    u32 hash               = trie_hash_segment(seg, seg_len);
    u32 idx                = hash & TRIE_CHILD_HASH_MASK;

    // Linear probing (table is sparse, usually finds on first try)
    for (u32 probe = 0; probe < TRIE_CHILD_HASH_SIZE; probe++) {
        u32 child_idx = node->child_hash[idx];
        if (child_idx == 0) {
            return 0; // Empty slot - not found
        }

        struct trie_node *child = &t->nodes[child_idx];
        // Verify full match (handle hash collisions)
        if (child->name_len == seg_len && likely(memcmp(child->name, seg, seg_len) == 0)) {
            return child_idx;
        }

        idx = (idx + 1) & TRIE_CHILD_HASH_MASK;
    }
    return 0; // Table full (shouldn't happen with proper sizing)
}

// Find '+' wildcard child - O(1) direct lookup
INLINE u32 trie_find_plus(struct topic_trie *t, u32 node_idx) {
    return t->nodes[node_idx].plus_child;
}

// Find '#' wildcard child - O(1) direct lookup
INLINE u32 trie_find_hash(struct topic_trie *t, u32 node_idx) {
    return t->nodes[node_idx].hash_child;
}

// Insert child into hash table
INLINE bool trie_hash_insert(struct trie_node *parent, u32 child_idx, u32 hash) {
    u32 idx = hash & TRIE_CHILD_HASH_MASK;

    for (u32 probe = 0; probe < TRIE_CHILD_HASH_SIZE; probe++) {
        if (parent->child_hash[idx] == 0) {
            parent->child_hash[idx] = child_idx;
            return true;
        }
        idx = (idx + 1) & TRIE_CHILD_HASH_MASK;
    }
    return false; // Table full
}

// Add child to node - maintains hash table and wildcard pointers
INLINE bool trie_add_child(struct topic_trie *t, u32 parent_idx, u32 child_idx) {
    struct trie_node *parent = &t->nodes[parent_idx];
    struct trie_node *child  = &t->nodes[child_idx];

    if (parent->child_count >= TRIE_MAX_CHILDREN) {
        return false;
    }

    // Add to linear array (for iteration)
    parent->children[parent->child_count++] = child_idx;
    child->parent                           = parent_idx;

    // Add to hash table for O(1) lookup
    u32 hash = trie_hash_segment(child->name, child->name_len);
    if (!trie_hash_insert(parent, child_idx, hash)) {
        parent->child_count--; // Rollback
        return false;
    }

    // Update direct wildcard pointers
    if (child->is_plus) {
        parent->plus_child = child_idx;
    } else if (child->is_hash) {
        parent->hash_child = child_idx;
    }

    return true;
}

// =============================================================================
// Subscribe
// =============================================================================

INLINE i32 trie_subscribe(struct topic_trie *t, const u8 *filter, u16 len, u32 slot_idx, u8 gen) {
    if (slot_idx >= TRIE_FD_SLOTS * BITS_PER_SLOT) {
        return -1; // slot_idx out of range
    }

    u32 node_idx         = 0; // Start at root
    struct topic_iter it = topic_iter_init(filter, len);
    struct mqtt_str seg;

    while (topic_iter_next(&it, &seg)) {
        // Limit segment length
        u8 seg_len = (seg.len > TRIE_MAX_SEGMENT) ? TRIE_MAX_SEGMENT : (u8)seg.len;

        // Find or create child
        u32 child_idx = trie_find_child(t, node_idx, seg.ptr, seg_len);

        if (child_idx == 0) {
            // Create new child
            child_idx = trie_alloc(t);
            if (child_idx == 0) {
                return -1; // Out of nodes
            }

            struct trie_node *child = &t->nodes[child_idx];
            child->name_len         = seg_len;
            memcpy(child->name, seg.ptr, seg_len);

            // Check wildcards
            if (seg_len == 1 && seg.ptr[0] == '+') {
                child->is_plus = 1;
            } else if (seg_len == 1 && seg.ptr[0] == '#') {
                child->is_hash = 1;
            }

            if (!trie_add_child(t, node_idx, child_idx)) {
                trie_free(t, child_idx);
                return -1;
            }
        }

        node_idx = child_idx;
    }

    // Add slot_idx to bitmap with generation
    struct trie_node *sub_node = &t->nodes[node_idx];
    u32 slot                   = slot_idx / BITS_PER_SLOT;
    u64 bit                    = 1ULL << (slot_idx % BITS_PER_SLOT);

    if (!(sub_node->fd_bitmap[slot] & bit)) {
        sub_node->fd_bitmap[slot] |= bit;
        sub_node->fd_gen[slot_idx] = gen;  // Store generation
        sub_node->sub_count++;
    } else {
        // Already subscribed - update generation (handles reconnect with same slot)
        sub_node->fd_gen[slot_idx] = gen;
    }

    return 0;
}

// =============================================================================
// Unsubscribe
// =============================================================================

INLINE i32 trie_unsubscribe(struct topic_trie *t, const u8 *filter, u16 len, u32 fd) {
    if (fd >= TRIE_FD_SLOTS * BITS_PER_SLOT) {
        return -1;
    }

    u32 node_idx         = 0;
    struct topic_iter it = topic_iter_init(filter, len);
    struct mqtt_str seg;

    while (topic_iter_next(&it, &seg)) {
        u8 seg_len    = (seg.len > TRIE_MAX_SEGMENT) ? TRIE_MAX_SEGMENT : (u8)seg.len;
        u32 child_idx = trie_find_child(t, node_idx, seg.ptr, seg_len);
        if (child_idx == 0) {
            return -1; // Not found
        }
        node_idx = child_idx;
    }

    // Remove fd from bitmap
    struct trie_node *sub_node = &t->nodes[node_idx];
    u32 slot                   = fd / BITS_PER_SLOT;
    u64 bit                    = 1ULL << (fd % BITS_PER_SLOT);

    if (sub_node->fd_bitmap[slot] & bit) {
        sub_node->fd_bitmap[slot] &= ~bit;
        sub_node->sub_count--;
    }

    return 0;
}

// =============================================================================
// Remove All Subscriptions for FD
// =============================================================================

INLINE void trie_remove_fd(struct topic_trie *t, u32 fd) {
    if (fd >= TRIE_FD_SLOTS * BITS_PER_SLOT) {
        return;
    }

    u32 slot = fd / BITS_PER_SLOT;
    u64 bit  = 1ULL << (fd % BITS_PER_SLOT);

    // Scan all nodes
    for (u32 i = 0; i < TRIE_MAX_NODES; i++) {
        struct trie_node *node = &t->nodes[i];
        if (node->fd_bitmap[slot] & bit) {
            node->fd_bitmap[slot] &= ~bit;
            node->sub_count--;
        }
    }
}

// =============================================================================
// Matching (recursive with callback)
// =============================================================================

typedef void (*trie_match_cb)(void *ctx, struct trie_node *node);

// Forward declaration for recursion
static void trie_match_recursive(struct topic_trie *t, u32 node_idx, const u8 *topic, u16 pos,
                                 u16 len, trie_match_cb cb, void *ctx);

INLINE void trie_match(struct topic_trie *t, const u8 *topic, u16 len, trie_match_cb cb,
                       void *ctx) {
    trie_match_recursive(t, 0, topic, 0, len, cb, ctx);
}

static void trie_match_recursive(struct topic_trie *t, u32 node_idx, const u8 *topic, u16 pos,
                                 u16 len, trie_match_cb cb, void *ctx) {
    // Check for '#' wildcard child (matches all remaining) - rare in practice
    u32 hash_child = trie_find_hash(t, node_idx);
    if (unlikely(hash_child != 0) && t->nodes[hash_child].sub_count > 0) {
        cb(ctx, &t->nodes[hash_child]);
    }

    // At end of topic - check for subscribers here
    if (unlikely(pos >= len)) {
        if (t->nodes[node_idx].sub_count > 0) {
            cb(ctx, &t->nodes[node_idx]);
        }
        return;
    }

    // Extract current segment
    struct mqtt_str seg;
    u16 next_pos = topic_extract_segment(topic, len, pos, &seg);

    // Check '+' wildcard child (matches this segment) - less common than exact match
    u32 plus_child = trie_find_plus(t, node_idx);
    if (unlikely(plus_child != 0)) {
        trie_match_recursive(t, plus_child, topic, next_pos, len, cb, ctx);
    }

    // Check exact match child - most common case
    if (likely(seg.len <= TRIE_MAX_SEGMENT)) {
        u32 exact_child = trie_find_child(t, node_idx, seg.ptr, (u8)seg.len);
        if (likely(exact_child != 0)) {
            trie_match_recursive(t, exact_child, topic, next_pos, len, cb, ctx);
        }
    }
}

// =============================================================================
// Matching (collect mode - no callback overhead)
// =============================================================================

#define TRIE_MAX_MATCHES 64

struct trie_match_result {
    struct trie_node *nodes[TRIE_MAX_MATCHES];
    u32 count;
};

// Forward declaration for recursion
static void trie_match_collect_recursive(struct topic_trie *t, u32 node_idx, const u8 *topic,
                                         u16 pos, u16 len, struct trie_match_result *result);

INLINE void trie_match_collect(struct topic_trie *t, const u8 *topic, u16 len,
                               struct trie_match_result *result) {
    result->count = 0;
    trie_match_collect_recursive(t, 0, topic, 0, len, result);
}

static void trie_match_collect_recursive(struct topic_trie *t, u32 node_idx, const u8 *topic,
                                         u16 pos, u16 len, struct trie_match_result *result) {
    // Check for '#' wildcard child (matches all remaining)
    // No branch hints - pattern is stable, let CPU predictor learn
    u32 hash_child = trie_find_hash(t, node_idx);
    if (hash_child != 0 && t->nodes[hash_child].sub_count > 0) {
        if (result->count < TRIE_MAX_MATCHES) {
            result->nodes[result->count++] = &t->nodes[hash_child];
        }
    }

    // At end of topic - check for subscribers here
    if (pos >= len) {
        if (t->nodes[node_idx].sub_count > 0 && result->count < TRIE_MAX_MATCHES) {
            result->nodes[result->count++] = &t->nodes[node_idx];
        }
        return;
    }

    // Extract current segment
    struct mqtt_str seg;
    u16 next_pos = topic_extract_segment(topic, len, pos, &seg);

    // Check '+' wildcard child (matches this segment)
    u32 plus_child = trie_find_plus(t, node_idx);
    if (plus_child != 0) {
        trie_match_collect_recursive(t, plus_child, topic, next_pos, len, result);
    }

    // Check exact match child
    if (seg.len <= TRIE_MAX_SEGMENT) {
        u32 exact_child = trie_find_child(t, node_idx, seg.ptr, (u8)seg.len);
        if (exact_child != 0) {
            trie_match_collect_recursive(t, exact_child, topic, next_pos, len, result);
        }
    }
}

// =============================================================================
// Iterate Subscribers in a Node (callback version - kept for compatibility)
// =============================================================================

typedef void (*trie_fd_cb)(void *ctx, u32 fd);

INLINE void trie_for_each_fd(struct trie_node *node, trie_fd_cb cb, void *ctx) {
    for (u32 slot = 0; slot < TRIE_FD_SLOTS; slot++) {
        u64 bits = node->fd_bitmap[slot];
        // Most slots are empty - skip quickly
        if (likely(bits == 0)) continue;

        while (bits) {
            u32 bit_idx = (u32)__builtin_ctzll(bits);
            u32 fd      = slot * BITS_PER_SLOT + bit_idx;

            cb(ctx, fd);

            bits &= bits - 1; // Clear lowest bit
        }
    }
}

#endif // BROKER_TRIE_H
