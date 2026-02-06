// broker/mqtt_fsm.h - MQTT Protocol State Machine
// Manages QoS ceremonies and owns the refcount lifecycle.
// All protocol state transitions happen here.
//
// NOTE: This file must be included AFTER msg_path.h and submit functions are defined.

#ifndef BROKER_MQTT_FSM_H
#define BROKER_MQTT_FSM_H

#include "broker/state.h"
#include "broker/client.h"
#include "broker/msg_path.h"
#include "config.h"

// =============================================================================
// Configuration
// =============================================================================

#ifndef LLMQ_MAX_RETRIES
#define LLMQ_MAX_RETRIES 3 // Max retransmission attempts before giving up
#endif

// =============================================================================
// Forward Declarations
// =============================================================================

// Protocol response senders (defined after this file is included)
INLINE void send_puback(struct broker *b, struct client_slot *c, u16 packet_id);
INLINE void send_pubrec(struct broker *b, struct client_slot *c, u16 packet_id);
INLINE void send_pubrel(struct broker *b, struct client_slot *c, u16 packet_id);
INLINE void send_pubcomp(struct broker *b, struct client_slot *c, u16 packet_id);

// Message reference release (defined in loop.h)
INLINE void release_msg_ref(struct broker *b, u32 msg_idx);

// =============================================================================
// Publisher Ceremony (we receive PUBLISH from them)
// =============================================================================

// Called when we receive a PUBLISH from a publisher.
// Handles the publisher side of the QoS ceremony.
// Returns: true if message should be fanned out, false if duplicate (QoS 2)
INLINE bool fsm_pub_received(struct broker *b, struct client_slot *pub,
                             u8 qos, u16 packet_id) {
    switch (qos) {
    case 0:
        // QoS 0: fire and forget - nothing to do
        return true;

    case 1:
        // QoS 1: acknowledge immediately
        send_puback(b, pub, packet_id);
        return true;

    case 2: {
        // QoS 2: check for duplicate first
        i32 existing = client_inflight_find(pub, packet_id);
        if (existing >= 0 && pub->inflight_hot[existing].direction == 1) {
            // Duplicate - resend PUBREC, don't fan-out again
            send_pubrec(b, pub, packet_id);
            return false;
        }

        // New message - track incoming, send PUBREC
        u64 deadline = b->now + LLMQ_INFLIGHT_TIMEOUT_MS;
        i32 inf_idx  = client_inflight_track_incoming(pub, packet_id, deadline);
        if (inf_idx < 0) {
            // Inflight full - can't track, drop silently
            return false;
        }
        send_pubrec(b, pub, packet_id);
        return true;
    }

    default:
        return false;
    }
}

// Called when we receive PUBREL from publisher (QoS 2 step 3).
// Completes the publisher ceremony.
INLINE void fsm_pub_pubrel_received(struct broker *b, struct client_slot *pub,
                                    u16 packet_id) {
    i32 slot = client_inflight_find(pub, packet_id);
    if (slot >= 0 &&
        pub->inflight_hot[slot].state == INFLIGHT_WAIT_PUBREL &&
        pub->inflight_hot[slot].direction == 1) {
        client_inflight_free_slot(pub, (u16)slot);
    }
    // Always send PUBCOMP even if we didn't find tracking (idempotent)
    send_pubcomp(b, pub, packet_id);
}

// =============================================================================
// Subscriber Ceremony (we send PUBLISH to them)
// =============================================================================

// Start delivery to a subscriber.
// For QoS 0: fire and forget (ref released on send completion)
// For QoS 1/2: track inflight, FSM owns refcount until ceremony completes
//
// Always increments refcount - caller should have base ref.
// Returns: 0 on success, -1 on failure (backpressure)
INLINE i32 fsm_sub_start_delivery(struct broker *b, struct client_slot *sub,
                                   u32 msg_idx, u8 sub_qos, u32 slot_idx) {
    // Take reference for this subscriber (all QoS levels)
    // QoS 0: released on send completion
    // QoS 1/2: released on ACK ceremony completion
    msg_ref(b, msg_idx);

    if (sub_qos == 0) {
        // QoS 0: fire and forget, no tracking needed
        i32 sd_idx = msg_prepare_send(b, msg_idx, 0, 0, 0, slot_idx, sub->generation);
        if (unlikely(sd_idx < 0)) {
            release_msg_ref(b, msg_idx);
            return -1;
        }
        msg_submit(b, (u32)sd_idx, sub->fd);
        return 0;
    }

    // QoS 1/2: allocate inflight entry
    u16 packet_id;
    u64 deadline = b->now + LLMQ_INFLIGHT_TIMEOUT_MS;
    i32 inf_idx  = client_inflight_alloc(sub, sub_qos, msg_idx, deadline, &packet_id);
    if (unlikely(inf_idx < 0)) {
        // Client inflight full - backpressure
        release_msg_ref(b, msg_idx);
        b->drops_inflight_full++;
        return -1;
    }

    // Prepare send descriptor
    i32 sd_idx = msg_prepare_send(b, msg_idx, packet_id, sub_qos, 0, slot_idx, sub->generation);
    if (unlikely(sd_idx < 0)) {
        // Failed to prepare - clean up
        release_msg_ref(b, msg_idx);
        client_inflight_free_slot(sub, (u16)inf_idx);
        b->drops_send_desc_empty++;
        return -1;
    }

    // Link send_desc to inflight for completion handling
    sub->inflight_cold[inf_idx].send_desc_idx = (u32)sd_idx;

    // Submit to io_uring
    msg_submit(b, (u32)sd_idx, sub->fd);
    return 0;
}

// Called when we receive PUBACK from subscriber (QoS 1 complete).
// Releases message reference and frees inflight entry.
INLINE void fsm_sub_puback_received(struct broker *b, struct client_slot *sub,
                                    u16 packet_id) {
    // Direct lookup - we assigned this packet_id, no hash table needed
    i32 slot = client_inflight_find_direct(sub, packet_id);
    if (slot < 0 || sub->inflight_hot[slot].state != INFLIGHT_WAIT_PUBACK) {
        return; // Unexpected or duplicate - ignore
    }

    // Ceremony complete - release reference
    release_msg_ref(b, sub->inflight_hot[slot].msg_idx);
    client_inflight_free_slot(sub, (u16)slot);
}

// Called when we receive PUBREC from subscriber (QoS 2 step 2).
// Transitions to WAIT_PUBCOMP state, sends PUBREL.
INLINE void fsm_sub_pubrec_received(struct broker *b, struct client_slot *sub,
                                    u16 packet_id) {
    // Direct lookup - we assigned this packet_id, no hash table needed
    i32 slot = client_inflight_find_direct(sub, packet_id);
    if (slot < 0 || sub->inflight_hot[slot].state != INFLIGHT_WAIT_PUBREC) {
        return; // Unexpected - ignore
    }

    // Transition state (hot write + cold write)
    sub->inflight_hot[slot].state    = INFLIGHT_WAIT_PUBCOMP;
    sub->inflight_cold[slot].deadline = b->now + LLMQ_INFLIGHT_TIMEOUT_MS;

    // Send PUBREL async (hot path - 1000s of these per message)
    submit_send_inf(b, sub, (u16)slot, HDR_PUBREL);
}

// Called when we receive PUBCOMP from subscriber (QoS 2 complete).
// Releases message reference and frees inflight entry.
INLINE void fsm_sub_pubcomp_received(struct broker *b, struct client_slot *sub,
                                     u16 packet_id) {
    // Direct lookup - we assigned this packet_id, no hash table needed
    i32 slot = client_inflight_find_direct(sub, packet_id);
    if (slot < 0 || sub->inflight_hot[slot].state != INFLIGHT_WAIT_PUBCOMP) {
        return; // Unexpected - ignore
    }

    // Ceremony complete - release reference
    release_msg_ref(b, sub->inflight_hot[slot].msg_idx);
    client_inflight_free_slot(sub, (u16)slot);
}

// =============================================================================
// Timeout and Retransmission
// =============================================================================

// Called when an inflight entry times out.
// Either retries with DUP=1 or gives up and releases reference.
INLINE void fsm_sub_timeout(struct broker *b, struct client_slot *sub,
                            u16 inf_slot, u32 client_slot_idx) {
    struct inflight_hot *hot = &sub->inflight_hot[inf_slot];
    struct inflight_cold *cold = &sub->inflight_cold[inf_slot];

    // Check if we should give up
    if (cold->dup_count >= LLMQ_MAX_RETRIES) {
        // Max retries reached - give up
        release_msg_ref(b, hot->msg_idx);
        client_inflight_free_slot(sub, inf_slot);
        b->msgs_dropped++;
        return;
    }

    // Retry based on current state
    cold->dup_count++;
    cold->deadline = b->now + LLMQ_INFLIGHT_TIMEOUT_MS;

    if (hot->state == INFLIGHT_WAIT_PUBCOMP) {
        // QoS 2: Already received PUBREC, retransmit PUBREL (not PUBLISH)
        send_pubrel(b, sub, hot->packet_id);
        return;
    }

    // QoS 1 (WAIT_PUBACK) or QoS 2 (WAIT_PUBREC): retransmit PUBLISH with DUP=1
    i32 sd_idx = msg_prepare_send(b, hot->msg_idx, hot->packet_id, cold->qos, 1,
                                  client_slot_idx, sub->generation);
    if (unlikely(sd_idx < 0)) {
        // Can't retry - give up
        release_msg_ref(b, hot->msg_idx);
        client_inflight_free_slot(sub, inf_slot);
        b->msgs_dropped++;
        return;
    }

    // Link and submit
    cold->send_desc_idx = (u32)sd_idx;
    msg_submit(b, (u32)sd_idx, sub->fd);
}

// =============================================================================
// Client Disconnect
// =============================================================================

// Called when a client disconnects.
// Releases message references for all pending outgoing inflight entries.
// Note: caller (broker_free_slot) handles client_inflight_free_all
INLINE void fsm_client_disconnect(struct broker *b, struct client_slot *c) {
    if (c->inflight_count == 0) {
        return;
    }

    u16 found = 0;
    for (u16 i = 0; i < c->max_inflight && found < c->inflight_count; i++) {
        struct inflight_hot *hot = &c->inflight_hot[i];
        if (hot->state == INFLIGHT_FREE) {
            continue;
        }
        found++;

        // Only release refs for outgoing messages (direction=0)
        // Incoming QoS 2 tracking (direction=1) doesn't hold buffer refs
        if (hot->direction == 0 && hot->msg_idx != MSG_POOL_INVALID) {
            release_msg_ref(b, hot->msg_idx);
        }
    }
    // Note: inflight entries freed by broker_free_slot calling client_inflight_free_all
}

// =============================================================================
// Timeout Sweep
// =============================================================================

// Sweep a single client for expired inflight entries.
// Calls fsm_sub_timeout for retransmission or cleanup.
// Returns: number of entries processed
INLINE u32 fsm_sweep_client(struct broker *b, struct client_slot *c, u32 slot_idx) {
    if (c->state != CLIENT_ACTIVE || c->inflight_count == 0) {
        return 0;
    }

    u32 processed = 0;
    u16 found     = 0;

    for (u16 i = 0; i < c->max_inflight && found < c->inflight_count; i++) {
        struct inflight_hot *hot = &c->inflight_hot[i];
        struct inflight_cold *cold = &c->inflight_cold[i];
        if (hot->state == INFLIGHT_FREE) {
            continue;
        }
        found++;

        // Check deadline (0 = no timeout)
        if (cold->deadline != 0 && b->now > cold->deadline) {
            // Only timeout outgoing messages (we sent PUBLISH, waiting for ACK)
            if (hot->direction == 0) {
                fsm_sub_timeout(b, c, i, slot_idx);
                processed++;
            }
        }
    }

    return processed;
}

// Sweep all clients for expired inflight entries.
// Called periodically from event loop.
INLINE u32 fsm_sweep_all_timeouts(struct broker *b) {
    u32 total = 0;

    // Only sweep active clients with inflight messages (avoids function call overhead)
    for (u32 i = 0; i < b->max_clients; i++) {
        struct client_slot *c = &b->clients[i];
        if (c->state == CLIENT_ACTIVE && c->inflight_count > 0) {
            total += fsm_sweep_client(b, c, i);
        }
    }

    return total;
}

#endif // BROKER_MQTT_FSM_H
