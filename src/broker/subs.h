// broker/subs.h - Subscription management
// Uses trie for storage, this file provides subscription operations

#ifndef BROKER_SUBS_H
#define BROKER_SUBS_H

#include "broker/state.h"
#include "mem/string.h"

// =============================================================================
// Subscription Management (uses trie)
// =============================================================================

// Add subscription for a slot with generation for slot-reuse detection
// Returns: 0 on success, -1 on failure
INLINE i32 sub_add(struct broker *b, u32 slot_idx, u8 gen, const u8 *topic, u16 topic_len, u8 qos) {
    (void)qos; // TODO: Track QoS per subscription in trie or slot

    if (topic_len == 0) {
        return -1;
    }

    // Add to topic trie with generation (for slot-reuse validation during fan-out)
    return trie_subscribe(&b->trie, topic, topic_len, slot_idx, gen);
}

// Remove subscription for a slot
INLINE void sub_remove(struct broker *b, u32 slot_idx, const u8 *topic, u16 topic_len) {
    trie_unsubscribe(&b->trie, topic, topic_len, slot_idx);
}

// Remove all subscriptions for a slot
INLINE void sub_remove_all(struct broker *b, u32 slot_idx) {
    trie_remove_fd(&b->trie, slot_idx);
}

// =============================================================================
// Topic Matching (with MQTT wildcards)
// =============================================================================

// Match topic against filter with + and # wildcards
// + matches exactly one level, # matches remaining levels (must be last)
INLINE bool topic_matches(const char *filter, u8 filter_len, const u8 *topic, u16 topic_len) {
    u8 fi  = 0; // filter index
    u16 ti = 0; // topic index

    while (fi < filter_len && ti < topic_len) {
        if (filter[fi] == '#') {
            // # matches everything remaining (must be at end)
            return true;
        }

        if (filter[fi] == '+') {
            // + matches one level - skip to next / or end in topic
            while (ti < topic_len && topic[ti] != '/') {
                ti++;
            }
            fi++;
            // If filter has more after +, it should be /
            if (fi < filter_len && filter[fi] == '/') {
                if (ti < topic_len && topic[ti] == '/') {
                    fi++;
                    ti++;
                } else {
                    return false;
                }
            }
            continue;
        }

        // Regular character - must match exactly
        if (filter[fi] != (char)topic[ti]) {
            return false;
        }
        fi++;
        ti++;
    }

    // Check for trailing # in filter
    if (fi < filter_len && filter[fi] == '#') {
        return true;
    }

    // Both must be exhausted for a match
    return fi == filter_len && ti == topic_len;
}

#endif // BROKER_SUBS_H
