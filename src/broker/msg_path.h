// broker/msg_path.h - Pure message data path
// Stateless functions for moving message bytes through the system.
// No protocol knowledge - just steal, store, prepare, submit.
//
// NOTE: This file must be included AFTER pool types are defined.

#ifndef BROKER_MSG_PATH_H
#define BROKER_MSG_PATH_H

#include "broker/state.h"
#include "mem/msgbuf.h"

// =============================================================================
// Buffer Stealing
// =============================================================================

// Steal the publisher's receive buffer for zero-copy fan-out.
// Gives the publisher a fresh buffer from the pool.
// Returns: stolen buffer index, or BUF_POOL_INVALID on failure
INLINE u32 msg_steal_buffer(struct broker *b, struct client_slot *pub) {
    u32 stolen = pub->recv_buf_idx;

    // Give publisher a new buffer
    pub->recv_buf_idx = buf_pool_alloc(&b->recv_pool);
    if (unlikely(pub->recv_buf_idx == BUF_POOL_INVALID)) {
        // Can't allocate new buffer - return stolen one and fail
        pub->recv_buf_idx = stolen;
        return BUF_POOL_INVALID;
    }

    return stolen;
}

// =============================================================================
// Message Storage
// =============================================================================

// Store a stolen buffer as a canonical message in the pool.
// Returns: msg_idx, or MSG_POOL_INVALID on failure
INLINE u32 msg_store(struct broker *b, u32 buf_idx,
                     u16 topic_off, u16 topic_len,
                     u32 payload_off, u32 payload_len,
                     u8 qos, u8 retain, u8 dup) {
    u32 msg_idx = msg_pool_alloc(&b->msg_pool);
    if (unlikely(msg_idx == MSG_POOL_INVALID)) {
        return MSG_POOL_INVALID;
    }

    struct canonical_msg *msg = msg_pool_get(&b->msg_pool, msg_idx);
    msg->buf_idx              = buf_idx;
    msg->topic_off            = topic_off;
    msg->topic_len            = topic_len;
    msg->payload_off          = payload_off;
    msg->payload_len          = payload_len;
    msg->qos                  = qos;
    msg->retain               = retain;
    msg->dup                  = dup;

    return msg_idx;
}

// =============================================================================
// Send Descriptor Preparation
// =============================================================================

// Forward declare - defined in loop.h
struct broker;

// Prepare a send descriptor for delivering a message to a subscriber.
// Materializes the PUBLISH header and builds scatter-gather iovecs.
//
// Parameters:
//   b         - broker state
//   msg_idx   - canonical message index
//   packet_id - MQTT packet ID (0 for QoS 0)
//   sub_qos   - subscriber's QoS level
//   dup       - DUP flag for retransmission
//   slot_idx  - subscriber's slot index
//   slot_gen  - subscriber's slot generation (for stale detection)
//
// Returns: send_desc index, or -1 on failure
INLINE i32 msg_prepare_send(struct broker *b, u32 msg_idx, u16 packet_id,
                            u8 sub_qos, u8 dup, u32 slot_idx, u8 slot_gen) {
    struct canonical_msg *msg = msg_pool_get(&b->msg_pool, msg_idx);
    if (unlikely(!msg)) {
        return -1;
    }

    i32 sd_idx_signed = (i32)send_desc_pool_alloc(&b->send_desc_pool);
    if (unlikely(sd_idx_signed < 0)) {
        return -1;
    }
    u32 sd_idx = (u32)sd_idx_signed;

    struct send_desc *sd = send_desc_pool_get(&b->send_desc_pool, sd_idx);
    if (unlikely(!sd)) {
        return -1;
    }

    // Get buffer pointer
    u8 *buf = buf_pool_get(&b->recv_pool, msg->buf_idx);
    if (unlikely(!buf)) {
        send_desc_pool_free(&b->send_desc_pool, sd_idx);
        return -1;
    }

    // Fill send_desc metadata
    sd->msg_idx   = msg_idx;
    sd->slot_idx  = slot_idx;
    sd->slot_gen  = slot_gen;
    sd->packet_id = packet_id;
    sd->sub_qos   = sub_qos;
    sd->state     = SEND_PENDING;
    sd->cursor    = 0;

    // Store big-endian packet ID for protocol
    if (sub_qos > 0) {
        sd->pkt_id_be[0] = (u8)(packet_id >> BITS_PER_BYTE);
        sd->pkt_id_be[1] = (u8)(packet_id & BYTE_MASK);
    }

    // Materialize PUBLISH header
    bool retain_flag = msg->retain != 0;
    bool dup_flag    = dup != 0;
    materialize_publish_header(sd, msg->topic_len, msg->payload_len, sub_qos, retain_flag, dup_flag);

    // Build scatter-gather iovecs
    // iov[0]: header (materialized)
    // iov[1]: topic (from stolen buffer)
    // iov[2]: packet_id (for QoS > 0, from sd->pkt_id_be)
    // iov[3]: payload (from stolen buffer)
    sd->iov[0].iov_base = sd->header;
    sd->iov[0].iov_len  = sd->header_len;

    sd->iov[1].iov_base = buf + msg->topic_off;
    sd->iov[1].iov_len  = msg->topic_len;

    if (sub_qos > 0) {
        sd->iov[2].iov_base = sd->pkt_id_be;
        sd->iov[2].iov_len  = 2;
    } else {
        sd->iov[2].iov_base = NULL;
        sd->iov[2].iov_len  = 0;
    }

    sd->iov[3].iov_base = buf + msg->payload_off;
    sd->iov[3].iov_len  = msg->payload_len;

    return (i32)sd_idx;
}

// =============================================================================
// Send Submission
// =============================================================================

// Forward declare submit function (defined in loop.h)
INLINE void submit_send_zc(struct broker *b, i32 fd, u32 send_desc_idx);

// Submit a prepared send descriptor to io_uring.
// Marks the descriptor as INFLIGHT.
INLINE void msg_submit(struct broker *b, u32 sd_idx, i32 fd) {
    struct send_desc *sd = send_desc_pool_get(&b->send_desc_pool, sd_idx);
    if (likely(sd)) {
        sd->state = SEND_INFLIGHT;
    }
    submit_send_zc(b, fd, sd_idx);
}

// =============================================================================
// Message Reference Helpers
// =============================================================================

// Increment reference count for a message (before fan-out to subscriber)
INLINE void msg_ref(struct broker *b, u32 msg_idx) {
    msg_pool_ref(&b->msg_pool, msg_idx);
}

// Release a message reference (decrement refcount, free if last)
// Forward declaration - actual implementation in loop.h which has pool access
INLINE void release_msg_ref(struct broker *b, u32 msg_idx);

#endif // BROKER_MSG_PATH_H
