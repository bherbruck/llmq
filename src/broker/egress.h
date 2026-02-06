// broker/egress.h - Per-client segmented egress queue
// Decouples transport submission from QoS ceremony ownership.
// Each client has a ring buffer of egress segments; one writev in-flight at a time.

#ifndef BROKER_EGRESS_H
#define BROKER_EGRESS_H

#include "sys/types.h"
#include "mem/msgbuf.h"

// Segment kinds
#define SEG_PUBLISH 0
#define SEG_CTRL    1

// egress_flush() return codes
#define EGRESS_FLUSH_NOOP      0
#define EGRESS_FLUSH_SUBMITTED 1
#define EGRESS_FLUSH_SQ_FULL   2

// Max PUBLISH messages batched into a single writev SQE
#define EGRESS_BATCH_MAX 8

// Compound egress segment (24 bytes) - one per logical message
struct egress_segment {
    u32 msg_idx;        // Canonical msg index (SEG_PUBLISH only, MSG_POOL_INVALID for CTRL)
    u16 packet_id;      // Per-subscriber packet ID (0 for QoS 0)
    u8 kind;            // SEG_PUBLISH or SEG_CTRL
    u8 qos;             // Subscriber's delivery QoS
    u8 dup;             // DUP flag (set on retransmit)
    u8 retain;          // Retain flag
    u8 slot_gen;        // Client generation at enqueue time (stale protection)
    u8 ctrl_len;        // SEG_CTRL: actual byte count (0 for PUBLISH)
    u8 ctrl[8];         // SEG_CTRL: inline packet data (PUBREL etc)
    u32 cursor;         // Bytes already consumed (partial send resume)
};

STATIC_ASSERT(sizeof(struct egress_segment) == 24, "egress_segment should be 24 bytes");

// Queue operations (power-of-two ring, bitmask indexing)
INLINE bool egress_full(u16 count, u16 capacity) { return count >= capacity; }
INLINE bool egress_empty(u16 count) { return count == 0; }

INLINE struct egress_segment *egress_head_seg(struct egress_segment *ring,
                                               u16 head, u16 mask) {
    return &ring[head & mask];
}

INLINE struct egress_segment *egress_push(struct egress_segment *ring,
                                           u16 head, u16 count, u16 mask) {
    return &ring[(head + count) & mask];
}

INLINE void egress_pop(u16 *head, u16 *count, u16 mask) {
    *head = (*head + 1) & mask;
    (*count)--;
}

// Compute total wire length of a PUBLISH packet from canonical message metadata
INLINE u32 egress_publish_wire_len(struct canonical_msg *msg, u8 qos) {
    u32 remaining = 2 + msg->topic_len + msg->payload_len;
    if (qos > 0) remaining += 2;
    u32 total = 1; // Fixed header byte
    u32 x = remaining;
    do { total++; x >>= 7; } while (x > 0);
    return total + remaining;
}

// For SEG_PUBLISH segments: store total wire length in ctrl[0..3]
// (ctrl[] bytes are unused for PUBLISH, only meaningful for SEG_CTRL)
INLINE void egress_seg_store_wire_len(struct egress_segment *seg, u32 len) {
    seg->ctrl[0] = (u8)(len);
    seg->ctrl[1] = (u8)(len >> 8);
    seg->ctrl[2] = (u8)(len >> 16);
    seg->ctrl[3] = (u8)(len >> 24);
}

INLINE u32 egress_seg_load_wire_len(struct egress_segment *seg) {
    return (u32)seg->ctrl[0] | ((u32)seg->ctrl[1] << 8) |
           ((u32)seg->ctrl[2] << 16) | ((u32)seg->ctrl[3] << 24);
}

#endif // BROKER_EGRESS_H
