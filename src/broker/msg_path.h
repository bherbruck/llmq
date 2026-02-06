// broker/msg_path.h - Pure message data path
// Stateless functions for moving message bytes through the system.
// No protocol knowledge - just steal, store, ref/unref.
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
