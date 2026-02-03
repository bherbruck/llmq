# Section 8: Topic Trie

## 8.1 Overview

The topic trie indexes subscriptions for efficient matching of published messages to subscribers. It supports exact topics and wildcard patterns (`+` for single level, `#` for multi-level).

## 8.2 Structure

### 8.2.1 Trie Node

```c
struct topic_node {
    // === Subscriber tracking ===
    _Atomic u32 sub_count;           // Number of active subscribers
    u64 fd_bitmap[16];               // 1024 bits for subscriber fds
    
    // === Trie structure ===
    u32 parent;                      // Parent node index (0 = root)
    u32 children[MAX_CHILDREN];      // Child node indices
    u8  child_keys[MAX_CHILDREN];    // First byte of child names (for quick search)
    u16 child_count;                 // Number of children
    
    // === Topic segment ===
    u16 name_len;                    // Length of this segment's name
    u8  name[MAX_SEGMENT_LEN];       // Segment name (e.g., "sensors")
    
    // === Flags ===
    u8  is_wildcard_plus;            // This node is a '+' wildcard
    u8  is_wildcard_hash;            // This node is a '#' wildcard
    u8  _reserved[2];
};

#define MAX_CHILDREN 64
#define MAX_SEGMENT_LEN 128
```

### 8.2.2 Trie Manager

```c
struct topic_trie {
    struct topic_node nodes[MAX_TOPICS];
    u32 free_head;                   // Freelist head
    u32 node_count;                  // Allocated nodes
    u32 root;                        // Root node index (always 0)
};
```

### 8.2.3 Visual Structure

```
Topic: "home/living/temp"
Topic: "home/+/temp"
Topic: "sensors/#"

                    [root]
                   /      \
              [home]      [sensors]
              /    \           \
         [living] [+]         [#]──► subs: {fd_bitmap}
            |       |
         [temp]  [temp]
            |       |
         subs    subs
```

## 8.3 Operations

### 8.3.1 Insert/Subscribe

```c
int trie_subscribe(struct topic_trie *trie, const u8 *filter, u16 len, u32 fd) {
    u32 node = trie->root;
    u32 pos = 0;
    
    while (pos < len) {
        // Extract next segment
        u32 seg_start = pos;
        while (pos < len && filter[pos] != '/') {
            pos++;
        }
        u32 seg_len = pos - seg_start;
        const u8 *segment = filter + seg_start;
        
        // Skip separator
        if (pos < len) pos++;
        
        // Find or create child for this segment
        u32 child = trie_find_child(trie, node, segment, seg_len);
        
        if (child == 0) {
            // Create new child
            child = trie_alloc_node(trie);
            if (child == 0) return -1;  // Out of nodes
            
            struct topic_node *child_node = &trie->nodes[child];
            child_node->parent = node;
            child_node->name_len = seg_len;
            memcpy(child_node->name, segment, seg_len);
            
            // Check for wildcards
            if (seg_len == 1 && segment[0] == '+') {
                child_node->is_wildcard_plus = 1;
            } else if (seg_len == 1 && segment[0] == '#') {
                child_node->is_wildcard_hash = 1;
            }
            
            // Add to parent's children
            trie_add_child(trie, node, child, segment[0]);
        }
        
        node = child;
    }
    
    // Add subscriber to final node
    struct topic_node *sub_node = &trie->nodes[node];
    u32 slot = fd / 64;
    u64 bit = 1ULL << (fd % 64);
    
    u64 old = atomic_or(&sub_node->fd_bitmap[slot], bit);
    if (!(old & bit)) {
        // New subscription
        atomic_add(&sub_node->sub_count, 1);
    }
    
    return 0;
}
```

### 8.3.2 Remove/Unsubscribe

```c
int trie_unsubscribe(struct topic_trie *trie, const u8 *filter, u16 len, u32 fd) {
    u32 node = trie_find_node(trie, filter, len);
    if (node == 0) return -1;  // Not found
    
    struct topic_node *sub_node = &trie->nodes[node];
    u32 slot = fd / 64;
    u64 bit = 1ULL << (fd % 64);
    
    u64 old = atomic_and(&sub_node->fd_bitmap[slot], ~bit);
    if (old & bit) {
        // Was subscribed
        atomic_sub(&sub_node->sub_count, 1);
    }
    
    // Optionally prune empty nodes (deferred for simplicity)
    return 0;
}
```

### 8.3.3 Remove All Subscriptions for FD

Called on disconnect:

```c
void trie_remove_subscriber(struct topic_trie *trie, u32 fd) {
    u32 slot = fd / 64;
    u64 bit = 1ULL << (fd % 64);
    
    // Scan all nodes (could optimize with per-fd subscription list)
    for (u32 i = 0; i < MAX_TOPICS; i++) {
        struct topic_node *node = &trie->nodes[i];
        
        u64 old = atomic_and(&node->fd_bitmap[slot], ~bit);
        if (old & bit) {
            atomic_sub(&node->sub_count, 1);
        }
    }
}
```

## 8.4 Matching

### 8.4.1 Match Context

```c
struct match_ctx {
    u32 *total_subs;                 // Running count of matched subscribers
    void (*callback)(struct match_ctx *, struct topic_node *);
    void *user_data;
};
```

### 8.4.2 Match Algorithm

```c
void trie_match(struct topic_trie *trie, const u8 *topic, u16 len,
                void (*callback)(void *, struct topic_node *), void *ctx) {
    // Start recursive match from root
    trie_match_recursive(trie, trie->root, topic, 0, len, callback, ctx);
}

static void trie_match_recursive(
    struct topic_trie *trie,
    u32 node_idx,
    const u8 *topic,
    u32 pos,
    u32 len,
    void (*callback)(void *, struct topic_node *),
    void *ctx
) {
    struct topic_node *node = &trie->nodes[node_idx];
    
    // Check for '#' wildcard child (matches rest of topic)
    u32 hash_child = trie_find_wildcard_hash(trie, node_idx);
    if (hash_child != 0) {
        struct topic_node *hash_node = &trie->nodes[hash_child];
        if (atomic_load(&hash_node->sub_count) > 0) {
            callback(ctx, hash_node);
        }
    }
    
    // If at end of topic, check for subscribers here
    if (pos >= len) {
        if (atomic_load(&node->sub_count) > 0) {
            callback(ctx, node);
        }
        return;
    }
    
    // Extract current segment
    u32 seg_start = pos;
    while (pos < len && topic[pos] != '/') {
        pos++;
    }
    u32 seg_len = pos - seg_start;
    const u8 *segment = topic + seg_start;
    
    // Skip separator
    u32 next_pos = pos;
    if (next_pos < len) next_pos++;
    
    // Check for '+' wildcard child (matches this segment)
    u32 plus_child = trie_find_wildcard_plus(trie, node_idx);
    if (plus_child != 0) {
        trie_match_recursive(trie, plus_child, topic, next_pos, len, callback, ctx);
    }
    
    // Check for exact match child
    u32 exact_child = trie_find_child(trie, node_idx, segment, seg_len);
    if (exact_child != 0) {
        trie_match_recursive(trie, exact_child, topic, next_pos, len, callback, ctx);
    }
}
```

### 8.4.3 Counting Subscribers

Before fan-out, count total matching subscribers:

```c
static void count_callback(void *ctx, struct topic_node *node) {
    u32 *total = (u32 *)ctx;
    *total += atomic_load(&node->sub_count);
}

u32 trie_count_subscribers(struct topic_trie *trie, const u8 *topic, u16 len) {
    u32 total = 0;
    trie_match(trie, topic, len, count_callback, &total);
    return total;
}
```

## 8.5 Bitmap Operations

### 8.5.1 Iterating Subscribers

```c
typedef void (*subscriber_callback)(void *ctx, u32 fd);

void trie_for_each_subscriber(struct topic_node *node,
                              subscriber_callback cb, void *ctx) {
    for (u32 slot = 0; slot < 16; slot++) {
        u64 bits = node->fd_bitmap[slot];
        
        while (bits) {
            u32 bit_idx = __builtin_ctzll(bits);  // Count trailing zeros
            u32 fd = slot * 64 + bit_idx;
            
            cb(ctx, fd);
            
            bits &= bits - 1;  // Clear lowest set bit
        }
    }
}
```

### 8.5.2 Bitmap Atomicity

The fd_bitmap operations use atomic fetch-and-or/fetch-and-and to allow concurrent subscribe/unsubscribe. However, iteration during match is NOT atomic with respect to modifications. This is acceptable because:

1. Missing a just-added subscriber on one message is tolerable
2. Including a just-removed subscriber will fail harmlessly (send to closed fd)
3. The sub_count is separately atomic and used for refcount initialization

## 8.6 Memory Layout

### 8.6.1 Cache Optimization

Hot data for matching is placed first in the node:

```c
struct topic_node {
    // Cache line 1: Match hot path
    _Atomic u32 sub_count;           // +0
    u64 fd_bitmap[8];                // +4  (first 512 fds)
    
    // Cache line 2: Bitmap continued + trie navigation
    u64 fd_bitmap_cont[8];           // +68
    u32 children[16];                // +132 (most common children)
    
    // Cache line 3+: Less frequently accessed
    u32 children_cont[48];           // Extended children
    u8  child_keys[64];
    u16 child_count;
    u32 parent;
    u16 name_len;
    u8  name[MAX_SEGMENT_LEN];
    u8  flags;
    // ...
};
```

### 8.6.2 Node Allocation

Nodes are allocated from a fixed pool with freelist:

```c
u32 trie_alloc_node(struct topic_trie *trie) {
    if (trie->free_head == 0) {
        // Pool exhausted
        return 0;
    }
    
    u32 idx = trie->free_head;
    // Free nodes use children[0] as next pointer
    trie->free_head = trie->nodes[idx].children[0];
    trie->node_count++;
    
    // Zero the node
    memset(&trie->nodes[idx], 0, sizeof(struct topic_node));
    
    return idx;
}

void trie_free_node(struct topic_trie *trie, u32 idx) {
    trie->nodes[idx].children[0] = trie->free_head;
    trie->free_head = idx;
    trie->node_count--;
}
```

## 8.7 Wildcard Semantics

### 8.7.1 Single-Level Wildcard (+)

- Matches exactly one topic level
- Must be sole character in its level
- Examples:
  - `home/+/temp` matches `home/living/temp`, `home/bedroom/temp`
  - Does NOT match `home/temp` or `home/living/room/temp`

### 8.7.2 Multi-Level Wildcard (#)

- Matches zero or more topic levels
- Must be last character in filter
- Must be preceded by `/` or be entire filter
- Examples:
  - `home/#` matches `home`, `home/living`, `home/living/temp`
  - `#` matches all topics

### 8.7.3 Matching Priority

When a topic matches multiple filters, all matching subscribers receive the message (no deduplication at trie level). Subscribers to multiple matching filters receive one copy per subscription.

## 8.8 Shared Subscriptions (MQTT 5.0)

Shared subscriptions use a special topic prefix: `$share/{group}/{filter}`

```c
struct shared_group {
    u32 group_hash;                  // Hash of group name
    u32 member_fds[MAX_SHARED_MEMBERS];
    u32 member_count;
    u32 next_member;                 // Round-robin index
};
```

For shared subscriptions, only ONE member of the group receives each message, selected round-robin:

```c
u32 shared_select_member(struct shared_group *group) {
    if (group->member_count == 0) return -1;
    
    u32 idx = atomic_add(&group->next_member, 1) % group->member_count;
    return group->member_fds[idx];
}
```
