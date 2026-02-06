// client.test.c - Tests for client slot and inflight message tracking

#include "test/test.h"
#include "broker/client.h"

// Test backing arrays (client_slot no longer embeds these)
static struct inflight_hot test_hot[LLMQ_MAX_INFLIGHT];
static struct inflight_cold test_cold[LLMQ_MAX_INFLIGHT];
static u16 test_hash[LLMQ_MAX_INFLIGHT * 2];
static struct pending_msg test_pending[LLMQ_MAX_PENDING_MSGS];

static void test_client_wire(struct client_slot *c) {
    c->inflight_hot      = test_hot;
    c->inflight_cold     = test_cold;
    c->inflight_pkt_hash = test_hash;
    c->pending           = test_pending;
}

// Hash size for collision tests (2x max_inflight, same as broker_init)
#define TEST_HASH_SIZE (2 * LLMQ_MAX_INFLIGHT)

// =============================================================================
// Inflight Allocation Tests
// =============================================================================

TEST(inflight_alloc_qos1) {
    struct client_slot c;
    test_client_wire(&c);
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    u16 packet_id;
    i32 idx = client_inflight_alloc(&c, 1, MSG_POOL_INVALID, 0, &packet_id);

    ASSERT(idx >= 0);
    ASSERT(packet_id > 0);
    ASSERT(c.inflight_count == 1);
    ASSERT(c.inflight_hot[idx].state == INFLIGHT_WAIT_PUBACK);
    ASSERT(c.inflight_cold[idx].qos == 1);
    ASSERT(c.inflight_hot[idx].direction == 0); // outgoing
    ASSERT(c.inflight_cold[idx].send_desc_idx == SEND_DESC_INVALID);

    client_inflight_free_all(&c);
}

TEST(inflight_alloc_qos2) {
    struct client_slot c;
    test_client_wire(&c);
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    u16 packet_id;
    i32 idx = client_inflight_alloc(&c, 2, MSG_POOL_INVALID, 0, &packet_id);

    ASSERT(idx >= 0);
    ASSERT(packet_id > 0);
    ASSERT(c.inflight_count == 1);
    ASSERT(c.inflight_hot[idx].state == INFLIGHT_WAIT_PUBREC);
    ASSERT(c.inflight_cold[idx].qos == 2);

    client_inflight_free_all(&c);
}

TEST(inflight_alloc_unique_packet_ids) {
    struct client_slot c;
    test_client_wire(&c);
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    u16 ids[LLMQ_MAX_INFLIGHT];
    for (u16 i = 0; i < LLMQ_MAX_INFLIGHT; i++) {
        i32 idx = client_inflight_alloc(&c, 1, MSG_POOL_INVALID, 0, &ids[i]);
        ASSERT(idx >= 0);
    }

    // All IDs should be unique
    for (u16 i = 0; i < LLMQ_MAX_INFLIGHT; i++) {
        for (u16 j = i + 1; j < LLMQ_MAX_INFLIGHT; j++) {
            ASSERT(ids[i] != ids[j]);
        }
    }

    client_inflight_free_all(&c);
}

TEST(inflight_alloc_full) {
    struct client_slot c;
    test_client_wire(&c);
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    // Fill up inflight slots
    for (u16 i = 0; i < LLMQ_MAX_INFLIGHT; i++) {
        u16 packet_id;
        i32 idx = client_inflight_alloc(&c, 1, MSG_POOL_INVALID, 0, &packet_id);
        ASSERT(idx >= 0);
    }

    // Next allocation should fail
    u16 packet_id;
    i32 idx = client_inflight_alloc(&c, 1, MSG_POOL_INVALID, 0, &packet_id);
    ASSERT(idx == -1);

    client_inflight_free_all(&c);
}

// =============================================================================
// Inflight Find and Free Tests
// =============================================================================

TEST(inflight_find) {
    struct client_slot c;
    test_client_wire(&c);
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    u16 packet_id;
    client_inflight_alloc(&c, 1, MSG_POOL_INVALID, 0, &packet_id);

    i32 slot = client_inflight_find(&c, packet_id);
    ASSERT(slot >= 0);
    ASSERT(c.inflight_hot[slot].packet_id == packet_id);

    // Non-existent packet ID
    i32 not_found = client_inflight_find(&c, 9999);
    ASSERT(not_found < 0);

    client_inflight_free_all(&c);
}

TEST(inflight_free) {
    struct client_slot c;
    test_client_wire(&c);
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    u16 packet_id;
    client_inflight_alloc(&c, 1, MSG_POOL_INVALID, 0, &packet_id);
    ASSERT(c.inflight_count == 1);

    i32 slot = client_inflight_find(&c, packet_id);
    client_inflight_free_slot(&c, (u16)slot);

    ASSERT(c.inflight_count == 0);
    ASSERT(c.inflight_hot[slot].state == INFLIGHT_FREE);

    // Should not find it anymore
    i32 not_found = client_inflight_find(&c, packet_id);
    ASSERT(not_found < 0);
}

TEST(inflight_reuse_after_free) {
    struct client_slot c;
    test_client_wire(&c);
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    // Allocate and free
    u16 packet_id1;
    client_inflight_alloc(&c, 1, MSG_POOL_INVALID, 0, &packet_id1);
    i32 slot = client_inflight_find(&c, packet_id1);
    client_inflight_free_slot(&c, (u16)slot);

    // Should be able to allocate again
    u16 packet_id2;
    i32 idx2 = client_inflight_alloc(&c, 1, MSG_POOL_INVALID, 0, &packet_id2);
    ASSERT(idx2 >= 0);
    ASSERT(c.inflight_count == 1);

    client_inflight_free_all(&c);
}

// =============================================================================
// QoS 2 Incoming (Track Incoming) Tests
// =============================================================================

TEST(inflight_track_incoming) {
    struct client_slot c;
    test_client_wire(&c);
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    i32 idx = client_inflight_track_incoming(&c, 42, 0);

    ASSERT(idx >= 0);
    ASSERT(c.inflight_count == 1);
    ASSERT(c.inflight_hot[idx].packet_id == 42);
    ASSERT(c.inflight_hot[idx].state == INFLIGHT_WAIT_PUBREL);
    ASSERT(c.inflight_hot[idx].direction == 1); // incoming
    ASSERT(c.inflight_cold[idx].qos == 2);

    client_inflight_free_all(&c);
}

TEST(inflight_track_incoming_duplicate) {
    struct client_slot c;
    test_client_wire(&c);
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    // Track same packet ID twice (simulating DUP PUBLISH)
    i32 idx1 = client_inflight_track_incoming(&c, 42, 0);
    i32 idx2 = client_inflight_track_incoming(&c, 42, 0);

    // Should return same index, not create new entry
    ASSERT(idx1 == idx2);
    ASSERT(c.inflight_count == 1);

    client_inflight_free_all(&c);
}

// =============================================================================
// State Transition Tests
// =============================================================================

TEST(qos1_state_transition) {
    struct client_slot c;
    test_client_wire(&c);
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    // Allocate QoS 1 - starts in WAIT_PUBACK
    u16 packet_id;
    client_inflight_alloc(&c, 1, MSG_POOL_INVALID, 0, &packet_id);
    i32 slot = client_inflight_find(&c, packet_id);

    ASSERT(c.inflight_hot[slot].state == INFLIGHT_WAIT_PUBACK);

    // Simulate PUBACK received - free the entry
    client_inflight_free_slot(&c, (u16)slot);
    ASSERT(c.inflight_count == 0);
}

TEST(qos2_outgoing_state_transitions) {
    struct client_slot c;
    test_client_wire(&c);
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    // Allocate QoS 2 outgoing - starts in WAIT_PUBREC
    u16 packet_id;
    client_inflight_alloc(&c, 2, MSG_POOL_INVALID, 0, &packet_id);
    i32 slot = client_inflight_find(&c, packet_id);

    ASSERT(c.inflight_hot[slot].state == INFLIGHT_WAIT_PUBREC);

    // Simulate PUBREC received - transition to WAIT_PUBCOMP
    c.inflight_hot[slot].state = INFLIGHT_WAIT_PUBCOMP;
    ASSERT(c.inflight_hot[slot].state == INFLIGHT_WAIT_PUBCOMP);

    // Simulate PUBCOMP received - free the entry
    client_inflight_free_slot(&c, (u16)slot);
    ASSERT(c.inflight_count == 0);
}

TEST(qos2_incoming_state_transitions) {
    struct client_slot c;
    test_client_wire(&c);
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    // Track incoming QoS 2 - starts in WAIT_PUBREL
    client_inflight_track_incoming(&c, 100, 0);
    i32 slot = client_inflight_find(&c, 100);

    ASSERT(c.inflight_hot[slot].state == INFLIGHT_WAIT_PUBREL);
    ASSERT(c.inflight_hot[slot].direction == 1);

    // Simulate PUBREL received - free the entry (after sending PUBCOMP)
    client_inflight_free_slot(&c, (u16)slot);
    ASSERT(c.inflight_count == 0);
}

// =============================================================================
// Hash Table Backward-Shift Deletion Tests
// =============================================================================

TEST(inflight_hash_remove_preserves_chain) {
    struct client_slot c;
    test_client_wire(&c);
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    // Insert two incoming QoS 2 messages that hash to the same bucket.
    // INFLIGHT_HASH_MASK = 511, so packet_ids that differ by 512 collide.
    u16 id_a = 100;
    u16 id_b = 100 + TEST_HASH_SIZE; // Same hash bucket as id_a

    i32 slot_a = client_inflight_track_incoming(&c, id_a, 0);
    i32 slot_b = client_inflight_track_incoming(&c, id_b, 0);
    ASSERT(slot_a >= 0);
    ASSERT(slot_b >= 0);
    ASSERT(c.inflight_count == 2);

    // Both should be findable via hash table
    ASSERT(client_inflight_find(&c, id_a) >= 0);
    ASSERT(client_inflight_find(&c, id_b) >= 0);

    // Remove the FIRST entry (id_a) - backward-shift should preserve id_b's chain
    client_inflight_free_slot(&c, (u16)slot_a);
    ASSERT(c.inflight_count == 1);

    // id_b must still be findable (this fails with naive EMPTY marking)
    i32 found_b = client_inflight_find(&c, id_b);
    ASSERT(found_b >= 0);
    ASSERT(c.inflight_hot[found_b].packet_id == id_b);

    client_inflight_free_all(&c);
}

TEST(inflight_hash_remove_triple_collision) {
    struct client_slot c;
    test_client_wire(&c);
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    // Three colliding packet_ids
    u16 id_a = 50;
    u16 id_b = 50 + TEST_HASH_SIZE;
    u16 id_c = 50 + TEST_HASH_SIZE * 2;

    i32 slot_a = client_inflight_track_incoming(&c, id_a, 0);
    i32 slot_b = client_inflight_track_incoming(&c, id_b, 0);
    i32 slot_c = client_inflight_track_incoming(&c, id_c, 0);
    ASSERT(slot_a >= 0 && slot_b >= 0 && slot_c >= 0);
    ASSERT(c.inflight_count == 3);

    // Remove middle entry (id_b) - chain must stay intact
    client_inflight_free_slot(&c, (u16)slot_b);
    ASSERT(c.inflight_count == 2);

    ASSERT(client_inflight_find(&c, id_a) >= 0);
    ASSERT(client_inflight_find(&c, id_c) >= 0);
    ASSERT(client_inflight_find(&c, id_b) < 0); // Should be gone

    // Remove first entry (id_a) - id_c must survive
    client_inflight_free_slot(&c, (u16)slot_a);
    ASSERT(c.inflight_count == 1);
    ASSERT(client_inflight_find(&c, id_c) >= 0);

    client_inflight_free_all(&c);
}
