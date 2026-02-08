// broker/mqtt_fsm.h - MQTT Protocol State Machine
// Manages QoS ceremonies and owns the refcount lifecycle.
// All protocol state transitions happen here.
//
// NOTE: This file must be included AFTER msg_path.h and submit functions are defined.

#ifndef BROKER_MQTT_FSM_H
#define BROKER_MQTT_FSM_H

#include "broker/state.h"
#include "broker/client.h"
#include "broker/egress.h"
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

// Egress flush (defined in loop.h)
INLINE u8 egress_flush(struct broker *b, struct client_slot *c, u32 slot_idx);

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
    msg_ref(b, msg_idx);
    u8 retain = 0;
    struct canonical_msg *msg = msg_pool_get(&b->msg_pool, msg_idx);
    if (msg) {
        retain = msg->retain;
    }

    if (sub_qos == 0) {
        // QoS 0: enqueue segment, ref released on send completion
        if (egress_full(sub->egress_count, sub->egress_capacity)) {
            release_msg_ref(b, msg_idx);
            b->drops_egress_full++;
            return -1;
        }
        struct egress_segment *seg = egress_push(sub->egress, sub->egress_head,
                                                  sub->egress_count, sub->egress_mask);
        seg->msg_idx   = msg_idx;
        seg->packet_id = 0;
        seg->kind      = SEG_PUBLISH;
        seg->qos       = 0;
        seg->dup       = 0;
        seg->retain    = retain;
        seg->slot_gen  = sub->generation;
        seg->ctrl_len  = 0;
        seg->cursor    = 0;
        if (msg) seg->wire_len = msg->wire_len_qos0;
        sub->egress_count++;
        if (egress_flush(b, sub, slot_idx) == EGRESS_FLUSH_SQ_FULL) {
            egress_flush_enqueue(b, slot_idx);
        }
        return 0;
    }

    // QoS 1/2: enqueue in egress. Inflight allocated at flush time (deferred).
    // This prevents inflight saturation when egress queue is deep —
    // slots are only consumed for messages actually submitted to the kernel.
    if (egress_full(sub->egress_count, sub->egress_capacity)) {
        release_msg_ref(b, msg_idx);
        b->drops_egress_full++;
        return -1;
    }

    struct egress_segment *seg = egress_push(sub->egress, sub->egress_head,
                                              sub->egress_count, sub->egress_mask);
    seg->msg_idx   = msg_idx;
    seg->packet_id = 0;  // Assigned at flush time when inflight is allocated
    seg->kind      = SEG_PUBLISH;
    seg->qos       = sub_qos;
    seg->dup       = 0;
    seg->retain    = retain;
    seg->slot_gen  = sub->generation;
    seg->ctrl_len  = 0;
    seg->cursor    = 0;
    if (msg) seg->wire_len = sub_qos > 0 ? msg->wire_len_qos12 : msg->wire_len_qos0;
    sub->egress_count++;
    if (egress_flush(b, sub, slot_idx) == EGRESS_FLUSH_SQ_FULL) {
        egress_flush_enqueue(b, slot_idx);
    }
    return 0;
}

// Called when we receive PUBACK from subscriber (QoS 1 complete).
// Releases message reference and frees inflight entry.
INLINE void fsm_sub_puback_received(struct broker *b, struct client_slot *sub,
                                    u16 packet_id) {
    i32 slot = client_inflight_find_direct(sub, packet_id);
    if (slot < 0 || sub->inflight_hot[slot].state != INFLIGHT_WAIT_PUBACK) {
        return;
    }
    release_msg_ref(b, sub->inflight_hot[slot].msg_idx);
    client_inflight_free_slot(sub, (u16)slot);

    // Inflight slot freed — kick egress if it was stalled on inflight-full
    if (sub->egress_count > 0 && !sub->egress_inflight) {
        u32 slot_idx = (u32)(sub - b->clients);
        if (egress_flush(b, sub, slot_idx) == EGRESS_FLUSH_SQ_FULL) {
            egress_flush_enqueue(b, slot_idx);
        }
    }
}

// Enqueue PUBREL as SEG_CTRL in the egress queue (batched into writev).
// Falls back to send_resp if egress is full.
INLINE void enqueue_pubrel(struct broker *b, struct client_slot *sub, u16 packet_id) {
    if (!egress_full(sub->egress_count, sub->egress_capacity)) {
        struct egress_segment *seg = egress_push(sub->egress, sub->egress_head,
                                                  sub->egress_count, sub->egress_mask);
        seg->kind      = SEG_CTRL;
        seg->msg_idx   = MSG_POOL_INVALID;
        seg->packet_id = 0;
        seg->qos       = 0;
        seg->slot_gen  = sub->generation;
        seg->ctrl_len  = 4;
        seg->ctrl[0]   = 0x62;  // PUBREL fixed header
        seg->ctrl[1]   = 0x02;
        seg->ctrl[2]   = (u8)(packet_id >> 8);
        seg->ctrl[3]   = (u8)(packet_id & 0xFF);
        seg->cursor    = 0;
        sub->egress_count++;
    } else {
        send_pubrel(b, sub, packet_id);  // Fallback to resp_buf
    }
}

// Called when we receive PUBREC from subscriber (QoS 2 step 2).
// Frees inflight immediately — subscriber has the data, no need to hold the
// slot until PUBCOMP. Enqueues PUBREL via egress (batched into writev).
// Duplicate PUBRECs also get PUBREL (idempotent).
INLINE void fsm_sub_pubrec_received(struct broker *b, struct client_slot *sub,
                                    u16 packet_id) {
    i32 slot = client_inflight_find_direct(sub, packet_id);
    if (slot < 0 || sub->inflight_hot[slot].state != INFLIGHT_WAIT_PUBREC) {
        // Duplicate PUBREC (inflight already freed) — resend PUBREL
        u32 slot_idx = (u32)(sub - b->clients);
        enqueue_pubrel(b, sub, packet_id);
        if (!sub->egress_inflight) {
            if (egress_flush(b, sub, slot_idx) == EGRESS_FLUSH_SQ_FULL)
                egress_flush_enqueue(b, slot_idx);
        }
        return;
    }

    // Release message buffer — subscriber already has the data
    release_msg_ref(b, sub->inflight_hot[slot].msg_idx);

    // Free inflight slot immediately — no need to hold until PUBCOMP
    client_inflight_free_slot(sub, (u16)slot);

    // Enqueue PUBREL via egress (batched into writev, no extra SQE/syscall)
    enqueue_pubrel(b, sub, packet_id);

    // Inflight slot freed + PUBREL enqueued — kick egress
    if (sub->egress_count > 0 && !sub->egress_inflight) {
        u32 slot_idx = (u32)(sub - b->clients);
        if (egress_flush(b, sub, slot_idx) == EGRESS_FLUSH_SQ_FULL) {
            egress_flush_enqueue(b, slot_idx);
        }
    }
}

// Called when we receive PUBCOMP from subscriber (QoS 2 complete).
// No-op: inflight already freed at PUBREC. PUBCOMP just confirms subscriber
// received our PUBREL — no state to update.
INLINE void fsm_sub_pubcomp_received(struct broker *b, struct client_slot *sub,
                                     u16 packet_id) {
    (void)b; (void)sub; (void)packet_id;
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
        release_msg_ref(b, hot->msg_idx);
        client_inflight_free_slot(sub, inf_slot);
        b->msgs_dropped++;
        return;
    }

    // Retry based on current state
    cold->dup_count++;
    cold->deadline = b->now + LLMQ_INFLIGHT_TIMEOUT_MS;

    // QoS 1 (WAIT_PUBACK) or QoS 2 (WAIT_PUBREC): retransmit PUBLISH with DUP=1
    // Note: WAIT_PUBCOMP no longer exists — inflight freed at PUBREC
    if (egress_full(sub->egress_count, sub->egress_capacity)) {
        // Can't retry - give up
        release_msg_ref(b, hot->msg_idx);
        client_inflight_free_slot(sub, inf_slot);
        b->msgs_dropped++;
        return;
    }

    struct egress_segment *seg = egress_push(sub->egress, sub->egress_head,
                                              sub->egress_count, sub->egress_mask);
    struct canonical_msg *msg = msg_pool_get(&b->msg_pool, hot->msg_idx);
    seg->msg_idx   = hot->msg_idx;
    seg->packet_id = hot->packet_id;
    seg->kind      = SEG_PUBLISH;
    seg->qos       = cold->qos;
    seg->dup       = 1;
    seg->retain    = msg ? msg->retain : 0;
    seg->slot_gen  = sub->generation;
    seg->ctrl_len  = 0;
    seg->cursor    = 0;
    if (msg) seg->wire_len = cold->qos > 0 ? msg->wire_len_qos12 : msg->wire_len_qos0;
    sub->egress_count++;

    if (egress_flush(b, sub, client_slot_idx) == EGRESS_FLUSH_SQ_FULL) {
        egress_flush_enqueue(b, client_slot_idx);
    }
}

// =============================================================================
// Client Disconnect
// =============================================================================

// Called when a client disconnects.
// Releases message references for all pending outgoing inflight entries.
// Also drains the egress queue, releasing QoS 0 refs.
// Note: caller (broker_free_slot) handles client_inflight_free_all
INLINE void fsm_client_disconnect(struct broker *b, struct client_slot *c) {
    // Drain egress queue — release refs for segments still owned by egress.
    // packet_id==0 means egress owns the ref (QoS 0, or QoS 1/2 not yet flushed).
    // packet_id>0 means inflight owns the ref (allocated at flush time).
    while (c->egress_count > 0) {
        struct egress_segment *seg = egress_head_seg(c->egress, c->egress_head, c->egress_mask);
        if (seg->kind == SEG_PUBLISH && seg->msg_idx != MSG_POOL_INVALID &&
            seg->packet_id == 0) {
            release_msg_ref(b, seg->msg_idx);
        }
        egress_pop(&c->egress_head, &c->egress_count, c->egress_mask);
    }

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
