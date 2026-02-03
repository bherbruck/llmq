// client.test.c - Tests for client slot and inflight message tracking

#include "test/test.h"
#include "broker/client.h"

// Test buffer pool for inflight messages
static struct buf_pool test_send_pool;

// Setup/teardown for tests
static void test_setup(void) {
    buf_pool_init(&test_send_pool, 128, LLMQ_SEND_BUF_SIZE);
}

static void test_teardown(void) {
    buf_pool_cleanup(&test_send_pool);
}

// =============================================================================
// Inflight Allocation Tests
// =============================================================================

TEST(inflight_alloc_qos0) {
    test_setup();
    struct client_slot c;
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    u16 packet_id;
    i32 idx = client_inflight_alloc(&c, 0, &packet_id, &test_send_pool);

    ASSERT(idx >= 0);
    ASSERT(c.inflight_count == 1);
    ASSERT(c.inflight[idx].state == INFLIGHT_SENDING);
    ASSERT(c.inflight[idx].qos == 0);
    ASSERT(c.inflight[idx].direction == 0); // outgoing

    client_inflight_free_all(&c, &test_send_pool);
    test_teardown();
}

TEST(inflight_alloc_qos1) {
    test_setup();
    struct client_slot c;
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    u16 packet_id;
    i32 idx = client_inflight_alloc(&c, 1, &packet_id, &test_send_pool);

    ASSERT(idx >= 0);
    ASSERT(packet_id > 0);
    ASSERT(c.inflight_count == 1);
    ASSERT(c.inflight[idx].state == INFLIGHT_WAIT_PUBACK);
    ASSERT(c.inflight[idx].qos == 1);
    ASSERT(c.inflight[idx].direction == 0); // outgoing

    client_inflight_free_all(&c, &test_send_pool);
    test_teardown();
}

TEST(inflight_alloc_qos2) {
    test_setup();
    struct client_slot c;
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    u16 packet_id;
    i32 idx = client_inflight_alloc(&c, 2, &packet_id, &test_send_pool);

    ASSERT(idx >= 0);
    ASSERT(packet_id > 0);
    ASSERT(c.inflight_count == 1);
    ASSERT(c.inflight[idx].state == INFLIGHT_WAIT_PUBREC);
    ASSERT(c.inflight[idx].qos == 2);

    client_inflight_free_all(&c, &test_send_pool);
    test_teardown();
}

TEST(inflight_alloc_unique_packet_ids) {
    test_setup();
    struct client_slot c;
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    u16 ids[LLMQ_MAX_INFLIGHT];
    for (u8 i = 0; i < LLMQ_MAX_INFLIGHT; i++) {
        i32 idx = client_inflight_alloc(&c, 1, &ids[i], &test_send_pool);
        ASSERT(idx >= 0);
    }

    // All IDs should be unique
    for (u8 i = 0; i < LLMQ_MAX_INFLIGHT; i++) {
        for (u8 j = i + 1; j < LLMQ_MAX_INFLIGHT; j++) {
            ASSERT(ids[i] != ids[j]);
        }
    }

    client_inflight_free_all(&c, &test_send_pool);
    test_teardown();
}

TEST(inflight_alloc_full) {
    test_setup();
    struct client_slot c;
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    // Fill up inflight slots
    for (u8 i = 0; i < LLMQ_MAX_INFLIGHT; i++) {
        u16 packet_id;
        i32 idx = client_inflight_alloc(&c, 1, &packet_id, &test_send_pool);
        ASSERT(idx >= 0);
    }

    // Next allocation should fail
    u16 packet_id;
    i32 idx = client_inflight_alloc(&c, 1, &packet_id, &test_send_pool);
    ASSERT(idx == -1);

    client_inflight_free_all(&c, &test_send_pool);
    test_teardown();
}

// =============================================================================
// Inflight Find and Free Tests
// =============================================================================

TEST(inflight_find) {
    test_setup();
    struct client_slot c;
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    u16 packet_id;
    client_inflight_alloc(&c, 1, &packet_id, &test_send_pool);

    struct inflight_msg *found = client_inflight_find(&c, packet_id);
    ASSERT(found != NULL);
    ASSERT(found->packet_id == packet_id);

    // Non-existent packet ID
    struct inflight_msg *not_found = client_inflight_find(&c, 9999);
    ASSERT(not_found == NULL);

    client_inflight_free_all(&c, &test_send_pool);
    test_teardown();
}

TEST(inflight_free) {
    test_setup();
    struct client_slot c;
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    u16 packet_id;
    client_inflight_alloc(&c, 1, &packet_id, &test_send_pool);
    ASSERT(c.inflight_count == 1);

    struct inflight_msg *msg = client_inflight_find(&c, packet_id);
    client_inflight_free(&c, msg, &test_send_pool);

    ASSERT(c.inflight_count == 0);
    ASSERT(msg->state == INFLIGHT_FREE);

    // Should not find it anymore
    struct inflight_msg *not_found = client_inflight_find(&c, packet_id);
    ASSERT(not_found == NULL);

    test_teardown();
}

TEST(inflight_reuse_after_free) {
    test_setup();
    struct client_slot c;
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    // Allocate and free
    u16 packet_id1;
    client_inflight_alloc(&c, 1, &packet_id1, &test_send_pool);
    struct inflight_msg *msg = client_inflight_find(&c, packet_id1);
    client_inflight_free(&c, msg, &test_send_pool);

    // Should be able to allocate again
    u16 packet_id2;
    i32 idx2 = client_inflight_alloc(&c, 1, &packet_id2, &test_send_pool);
    ASSERT(idx2 >= 0);
    ASSERT(c.inflight_count == 1);

    client_inflight_free_all(&c, &test_send_pool);
    test_teardown();
}

// =============================================================================
// QoS 2 Incoming (Track Incoming) Tests
// =============================================================================

TEST(inflight_track_incoming) {
    test_setup();
    struct client_slot c;
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    i32 idx = client_inflight_track_incoming(&c, 42);

    ASSERT(idx >= 0);
    ASSERT(c.inflight_count == 1);
    ASSERT(c.inflight[idx].packet_id == 42);
    ASSERT(c.inflight[idx].state == INFLIGHT_WAIT_PUBREL);
    ASSERT(c.inflight[idx].direction == 1); // incoming
    ASSERT(c.inflight[idx].qos == 2);

    client_inflight_free_all(&c, &test_send_pool);
    test_teardown();
}

TEST(inflight_track_incoming_duplicate) {
    test_setup();
    struct client_slot c;
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    // Track same packet ID twice (simulating DUP PUBLISH)
    i32 idx1 = client_inflight_track_incoming(&c, 42);
    i32 idx2 = client_inflight_track_incoming(&c, 42);

    // Should return same index, not create new entry
    ASSERT(idx1 == idx2);
    ASSERT(c.inflight_count == 1);

    client_inflight_free_all(&c, &test_send_pool);
    test_teardown();
}

// =============================================================================
// State Transition Tests
// =============================================================================

TEST(qos1_state_transition) {
    test_setup();
    struct client_slot c;
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    // Allocate QoS 1 - starts in WAIT_PUBACK
    u16 packet_id;
    client_inflight_alloc(&c, 1, &packet_id, &test_send_pool);
    struct inflight_msg *msg = client_inflight_find(&c, packet_id);

    ASSERT(msg->state == INFLIGHT_WAIT_PUBACK);

    // Simulate PUBACK received - free the entry
    client_inflight_free(&c, msg, &test_send_pool);
    ASSERT(c.inflight_count == 0);

    test_teardown();
}

TEST(qos2_outgoing_state_transitions) {
    test_setup();
    struct client_slot c;
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    // Allocate QoS 2 outgoing - starts in WAIT_PUBREC
    u16 packet_id;
    client_inflight_alloc(&c, 2, &packet_id, &test_send_pool);
    struct inflight_msg *msg = client_inflight_find(&c, packet_id);

    ASSERT(msg->state == INFLIGHT_WAIT_PUBREC);

    // Simulate PUBREC received - transition to WAIT_PUBCOMP
    msg->state = INFLIGHT_WAIT_PUBCOMP;
    ASSERT(msg->state == INFLIGHT_WAIT_PUBCOMP);

    // Simulate PUBCOMP received - free the entry
    client_inflight_free(&c, msg, &test_send_pool);
    ASSERT(c.inflight_count == 0);

    test_teardown();
}

TEST(qos2_incoming_state_transitions) {
    test_setup();
    struct client_slot c;
    client_init(&c, 10, BUF_POOL_INVALID, LLMQ_MAX_INFLIGHT);

    // Track incoming QoS 2 - starts in WAIT_PUBREL
    client_inflight_track_incoming(&c, 100);
    struct inflight_msg *msg = client_inflight_find(&c, 100);

    ASSERT(msg->state == INFLIGHT_WAIT_PUBREL);
    ASSERT(msg->direction == 1);

    // Simulate PUBREL received - free the entry (after sending PUBCOMP)
    client_inflight_free(&c, msg, &test_send_pool);
    ASSERT(c.inflight_count == 0);

    test_teardown();
}
