// broker/trie.h - Topic trie for subscription matching
// O(topic_levels) lookup with wildcard support

#ifndef BROKER_TRIE_H
#define BROKER_TRIE_H

#include "sys/types.h"
#include "mem/string.h"
#include "config.h"

// =============================================================================
// Configuration (from config.h, with local aliases)
// =============================================================================

#define TRIE_MAX_NODES    LLMQ_TRIE_MAX_NODES
#define TRIE_MAX_CHILDREN LLMQ_TRIE_MAX_CHILDREN
#define TRIE_MAX_SEGMENT  LLMQ_TRIE_MAX_SEGMENT
#define BITS_PER_SLOT     64                                 // Bits per u64 bitmap slot
#define TRIE_FD_SLOTS     (LLMQ_MAX_CLIENTS / BITS_PER_SLOT) // Scales with max clients

// =============================================================================
// Trie Node
// =============================================================================

struct trie_node {
    // Subscriber tracking (fd bitmap)
    u64 fd_bitmap[TRIE_FD_SLOTS]; // 1024 bits for fds
    u32 sub_count;                // Active subscriber count

    // Trie structure
    u32 parent; // Parent node index
    u32 children[TRIE_MAX_CHILDREN];
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
// Child Node Operations
// =============================================================================

// Find child matching segment
INLINE u32 trie_find_child(struct topic_trie *t, u32 node_idx, const u8 *seg, u8 seg_len) {
    struct trie_node *node = &t->nodes[node_idx];

    for (u16 i = 0; i < node->child_count; i++) {
        u32 child_idx           = node->children[i];
        struct trie_node *child = &t->nodes[child_idx];

        if (child->name_len == seg_len && memcmp(child->name, seg, seg_len) == 0) {
            return child_idx;
        }
    }
    return 0; // Not found
}

// Find '+' wildcard child
INLINE u32 trie_find_plus(struct topic_trie *t, u32 node_idx) {
    struct trie_node *node = &t->nodes[node_idx];

    for (u16 i = 0; i < node->child_count; i++) {
        if (t->nodes[node->children[i]].is_plus) {
            return node->children[i];
        }
    }
    return 0;
}

// Find '#' wildcard child
INLINE u32 trie_find_hash(struct topic_trie *t, u32 node_idx) {
    struct trie_node *node = &t->nodes[node_idx];

    for (u16 i = 0; i < node->child_count; i++) {
        if (t->nodes[node->children[i]].is_hash) {
            return node->children[i];
        }
    }
    return 0;
}

// Add child to node
INLINE bool trie_add_child(struct topic_trie *t, u32 parent_idx, u32 child_idx) {
    struct trie_node *parent = &t->nodes[parent_idx];

    if (parent->child_count >= TRIE_MAX_CHILDREN) {
        return false;
    }

    parent->children[parent->child_count++] = child_idx;
    t->nodes[child_idx].parent              = parent_idx;
    return true;
}

// =============================================================================
// Subscribe
// =============================================================================

INLINE i32 trie_subscribe(struct topic_trie *t, const u8 *filter, u16 len, u32 fd) {
    if (fd >= TRIE_FD_SLOTS * BITS_PER_SLOT) {
        return -1; // fd out of range
    }

    u32 node_idx = 0; // Start at root
    u16 pos      = 0;

    while (pos < len) {
        // Extract segment
        u16 seg_start = pos;
        while (pos < len && filter[pos] != '/') {
            pos++;
        }
        u16 seg_len       = pos - seg_start;
        const u8 *segment = filter + seg_start;

        // Skip separator
        if (pos < len)
            pos++;

        // Limit segment length
        if (seg_len > TRIE_MAX_SEGMENT) {
            seg_len = TRIE_MAX_SEGMENT;
        }

        // Find or create child
        u32 child_idx = trie_find_child(t, node_idx, segment, (u8)seg_len);

        if (child_idx == 0) {
            // Create new child
            child_idx = trie_alloc(t);
            if (child_idx == 0) {
                return -1; // Out of nodes
            }

            struct trie_node *child = &t->nodes[child_idx];
            child->name_len         = (u8)seg_len;
            memcpy(child->name, segment, seg_len);

            // Check wildcards
            if (seg_len == 1 && segment[0] == '+') {
                child->is_plus = 1;
            } else if (seg_len == 1 && segment[0] == '#') {
                child->is_hash = 1;
            }

            if (!trie_add_child(t, node_idx, child_idx)) {
                trie_free(t, child_idx);
                return -1;
            }
        }

        node_idx = child_idx;
    }

    // Add fd to bitmap
    struct trie_node *sub_node = &t->nodes[node_idx];
    u32 slot                   = fd / BITS_PER_SLOT;
    u64 bit                    = 1ULL << (fd % BITS_PER_SLOT);

    if (!(sub_node->fd_bitmap[slot] & bit)) {
        sub_node->fd_bitmap[slot] |= bit;
        sub_node->sub_count++;
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

    u32 node_idx = 0;
    u16 pos      = 0;

    while (pos < len) {
        u16 seg_start = pos;
        while (pos < len && filter[pos] != '/') {
            pos++;
        }
        u16 seg_len       = pos - seg_start;
        const u8 *segment = filter + seg_start;

        if (pos < len)
            pos++;

        if (seg_len > TRIE_MAX_SEGMENT) {
            seg_len = TRIE_MAX_SEGMENT;
        }

        u32 child_idx = trie_find_child(t, node_idx, segment, (u8)seg_len);
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
    // Check for '#' wildcard child (matches all remaining)
    u32 hash_child = trie_find_hash(t, node_idx);
    if (hash_child != 0 && t->nodes[hash_child].sub_count > 0) {
        cb(ctx, &t->nodes[hash_child]);
    }

    // At end of topic - check for subscribers here
    if (pos >= len) {
        if (t->nodes[node_idx].sub_count > 0) {
            cb(ctx, &t->nodes[node_idx]);
        }
        return;
    }

    // Extract current segment
    u16 seg_start = pos;
    while (pos < len && topic[pos] != '/') {
        pos++;
    }
    u16 seg_len       = pos - seg_start;
    const u8 *segment = topic + seg_start;

    // Skip separator
    u16 next_pos = pos;
    if (next_pos < len)
        next_pos++;

    // Check '+' wildcard child (matches this segment)
    u32 plus_child = trie_find_plus(t, node_idx);
    if (plus_child != 0) {
        trie_match_recursive(t, plus_child, topic, next_pos, len, cb, ctx);
    }

    // Check exact match child
    if (seg_len <= TRIE_MAX_SEGMENT) {
        u32 exact_child = trie_find_child(t, node_idx, segment, (u8)seg_len);
        if (exact_child != 0) {
            trie_match_recursive(t, exact_child, topic, next_pos, len, cb, ctx);
        }
    }
}

// =============================================================================
// Iterate Subscribers in a Node
// =============================================================================

typedef void (*trie_fd_cb)(void *ctx, u32 fd);

INLINE void trie_for_each_fd(struct trie_node *node, trie_fd_cb cb, void *ctx) {
    for (u32 slot = 0; slot < TRIE_FD_SLOTS; slot++) {
        u64 bits = node->fd_bitmap[slot];

        while (bits) {
            u32 bit_idx = (u32)__builtin_ctzll(bits);
            u32 fd      = slot * BITS_PER_SLOT + bit_idx;

            cb(ctx, fd);

            bits &= bits - 1; // Clear lowest bit
        }
    }
}

#endif // BROKER_TRIE_H
