// client.test.c - Tests for client slot and inflight message tracking

#include "test/test.h"
#include "broker/client.h"

// =============================================================================
// Inflight Allocation Tests
// =============================================================================

TEST(inflight_alloc_qos0) {
    struct client_slot c;
    client_init(&c, 10);

    u16 packet_id;
    i32 idx = client_inflight_alloc(&c, 0, &packet_id, MAX_INFLIGHT);

    ASSERT(idx >= 0);
    ASSERT(c.inflight_count == 1);
    ASSERT(c.inflight[idx].state == INFLIGHT_SENDING);
    ASSERT(c.inflight[idx].qos == 0);
    ASSERT(c.inflight[idx].direction == 0); // outgoing
}

TEST(inflight_alloc_qos1) {
    struct client_slot c;
    client_init(&c, 10);

    u16 packet_id;
    i32 idx = client_inflight_alloc(&c, 1, &packet_id, MAX_INFLIGHT);

    ASSERT(idx >= 0);
    ASSERT(packet_id > 0);
    ASSERT(c.inflight_count == 1);
    ASSERT(c.inflight[idx].state == INFLIGHT_WAIT_PUBACK);
    ASSERT(c.inflight[idx].qos == 1);
    ASSERT(c.inflight[idx].direction == 0); // outgoing
}

TEST(inflight_alloc_qos2) {
    struct client_slot c;
    client_init(&c, 10);

    u16 packet_id;
    i32 idx = client_inflight_alloc(&c, 2, &packet_id, MAX_INFLIGHT);

    ASSERT(idx >= 0);
    ASSERT(packet_id > 0);
    ASSERT(c.inflight_count == 1);
    ASSERT(c.inflight[idx].state == INFLIGHT_WAIT_PUBREC);
    ASSERT(c.inflight[idx].qos == 2);
}

TEST(inflight_alloc_unique_packet_ids) {
    struct client_slot c;
    client_init(&c, 10);

    u16 ids[MAX_INFLIGHT];
    for (u8 i = 0; i < MAX_INFLIGHT; i++) {
        i32 idx = client_inflight_alloc(&c, 1, &ids[i], MAX_INFLIGHT);
        ASSERT(idx >= 0);
    }

    // All IDs should be unique
    for (u8 i = 0; i < MAX_INFLIGHT; i++) {
        for (u8 j = i + 1; j < MAX_INFLIGHT; j++) {
            ASSERT(ids[i] != ids[j]);
        }
    }
}

TEST(inflight_alloc_full) {
    struct client_slot c;
    client_init(&c, 10);

    // Fill up inflight slots
    for (u8 i = 0; i < MAX_INFLIGHT; i++) {
        u16 packet_id;
        i32 idx = client_inflight_alloc(&c, 1, &packet_id, MAX_INFLIGHT);
        ASSERT(idx >= 0);
    }

    // Next allocation should fail
    u16 packet_id;
    i32 idx = client_inflight_alloc(&c, 1, &packet_id, MAX_INFLIGHT);
    ASSERT(idx == -1);
}

// =============================================================================
// Inflight Find and Free Tests
// =============================================================================

TEST(inflight_find) {
    struct client_slot c;
    client_init(&c, 10);

    u16 packet_id;
    client_inflight_alloc(&c, 1, &packet_id, MAX_INFLIGHT);

    struct inflight_msg *found = client_inflight_find(&c, packet_id);
    ASSERT(found != NULL);
    ASSERT(found->packet_id == packet_id);

    // Non-existent packet ID
    struct inflight_msg *not_found = client_inflight_find(&c, 9999);
    ASSERT(not_found == NULL);
}

TEST(inflight_free) {
    struct client_slot c;
    client_init(&c, 10);

    u16 packet_id;
    client_inflight_alloc(&c, 1, &packet_id, MAX_INFLIGHT);
    ASSERT(c.inflight_count == 1);

    struct inflight_msg *msg = client_inflight_find(&c, packet_id);
    client_inflight_free(&c, msg);

    ASSERT(c.inflight_count == 0);
    ASSERT(msg->state == INFLIGHT_FREE);

    // Should not find it anymore
    struct inflight_msg *not_found = client_inflight_find(&c, packet_id);
    ASSERT(not_found == NULL);
}

TEST(inflight_reuse_after_free) {
    struct client_slot c;
    client_init(&c, 10);

    // Allocate and free
    u16 packet_id1;
    client_inflight_alloc(&c, 1, &packet_id1, MAX_INFLIGHT);
    struct inflight_msg *msg = client_inflight_find(&c, packet_id1);
    client_inflight_free(&c, msg);

    // Should be able to allocate again
    u16 packet_id2;
    i32 idx2 = client_inflight_alloc(&c, 1, &packet_id2, MAX_INFLIGHT);
    ASSERT(idx2 >= 0);
    ASSERT(c.inflight_count == 1);
}

// =============================================================================
// QoS 2 Incoming (Track Incoming) Tests
// =============================================================================

TEST(inflight_track_incoming) {
    struct client_slot c;
    client_init(&c, 10);

    i32 idx = client_inflight_track_incoming(&c, 42);

    ASSERT(idx >= 0);
    ASSERT(c.inflight_count == 1);
    ASSERT(c.inflight[idx].packet_id == 42);
    ASSERT(c.inflight[idx].state == INFLIGHT_WAIT_PUBREL);
    ASSERT(c.inflight[idx].direction == 1); // incoming
    ASSERT(c.inflight[idx].qos == 2);
}

TEST(inflight_track_incoming_duplicate) {
    struct client_slot c;
    client_init(&c, 10);

    // Track same packet ID twice (simulating DUP PUBLISH)
    i32 idx1 = client_inflight_track_incoming(&c, 42);
    i32 idx2 = client_inflight_track_incoming(&c, 42);

    // Should return same index, not create new entry
    ASSERT(idx1 == idx2);
    ASSERT(c.inflight_count == 1);
}

// =============================================================================
// State Transition Tests
// =============================================================================

TEST(qos1_state_transition) {
    struct client_slot c;
    client_init(&c, 10);

    // Allocate QoS 1 - starts in WAIT_PUBACK
    u16 packet_id;
    client_inflight_alloc(&c, 1, &packet_id, MAX_INFLIGHT);
    struct inflight_msg *msg = client_inflight_find(&c, packet_id);

    ASSERT(msg->state == INFLIGHT_WAIT_PUBACK);

    // Simulate PUBACK received - free the entry
    client_inflight_free(&c, msg);
    ASSERT(c.inflight_count == 0);
}

TEST(qos2_outgoing_state_transitions) {
    struct client_slot c;
    client_init(&c, 10);

    // Allocate QoS 2 outgoing - starts in WAIT_PUBREC
    u16 packet_id;
    client_inflight_alloc(&c, 2, &packet_id, MAX_INFLIGHT);
    struct inflight_msg *msg = client_inflight_find(&c, packet_id);

    ASSERT(msg->state == INFLIGHT_WAIT_PUBREC);

    // Simulate PUBREC received - transition to WAIT_PUBCOMP
    msg->state = INFLIGHT_WAIT_PUBCOMP;
    ASSERT(msg->state == INFLIGHT_WAIT_PUBCOMP);

    // Simulate PUBCOMP received - free the entry
    client_inflight_free(&c, msg);
    ASSERT(c.inflight_count == 0);
}

TEST(qos2_incoming_state_transitions) {
    struct client_slot c;
    client_init(&c, 10);

    // Track incoming QoS 2 - starts in WAIT_PUBREL
    client_inflight_track_incoming(&c, 100);
    struct inflight_msg *msg = client_inflight_find(&c, 100);

    ASSERT(msg->state == INFLIGHT_WAIT_PUBREL);
    ASSERT(msg->direction == 1);

    // Simulate PUBREL received - free the entry (after sending PUBCOMP)
    client_inflight_free(&c, msg);
    ASSERT(c.inflight_count == 0);
}
