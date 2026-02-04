// broker/handler.h - MQTT packet processing
// Handles incoming MQTT packets and generates responses
//
// NOTE: This file must be included AFTER submit_send() is defined

#ifndef BROKER_HANDLER_H
#define BROKER_HANDLER_H

#include "broker/state.h"
#include "broker/subs.h"
#include "mqtt/packet.h"
#include "mqtt/connect.h"
#include "mqtt/subscribe.h"
#include "mqtt/publish.h"
#include "util/log.h"

// Forward declare functions (defined in loop.h which includes this file)
INLINE void submit_send(struct broker *b, i32 fd, const u8 *buf, u32 len);
INLINE void submit_send_zc(struct broker *b, i32 fd, u32 send_desc_idx);
INLINE bool send_static(struct broker *b, struct client_slot *c, const void *buf, u32 len);
INLINE bool send_resp(struct broker *b, struct client_slot *c, const u8 *hdr, u16 packet_id);
INLINE bool send_buf(struct broker *b, struct client_slot *c, const void *buf, u32 len);
INLINE bool submit_send_inf(struct broker *b, struct client_slot *c,
                            struct inflight_msg *inf, const u8 *hdr);
INLINE void release_msg_ref(struct broker *b, u32 msg_idx);

// Static headers for zero-copy protocol responses (type<<4 | flags, remaining_len)
static const u8 HDR_PUBACK[2]   = {0x40, 0x02};
static const u8 HDR_PUBREC[2]   = {0x50, 0x02};
static const u8 HDR_PUBREL[2]   = {0x62, 0x02};
static const u8 HDR_PUBCOMP[2]  = {0x70, 0x02};
static const u8 HDR_UNSUBACK[2] = {0xB0, 0x02};
static const u8 HDR_PINGRESP[2] = {0xD0, 0x00};

// =============================================================================
// Protocol Response Senders (used by FSM)
// =============================================================================

INLINE void send_puback(struct broker *b, struct client_slot *c, u16 packet_id) {
    send_resp(b, c, HDR_PUBACK, packet_id);
}

INLINE void send_pubrec(struct broker *b, struct client_slot *c, u16 packet_id) {
    send_resp(b, c, HDR_PUBREC, packet_id);
}

INLINE void send_pubrel(struct broker *b, struct client_slot *c, u16 packet_id) {
    send_resp(b, c, HDR_PUBREL, packet_id);
}

INLINE void send_pubcomp(struct broker *b, struct client_slot *c, u16 packet_id) {
    send_resp(b, c, HDR_PUBCOMP, packet_id);
}

INLINE void send_unsuback(struct broker *b, struct client_slot *c, u16 packet_id) {
    send_resp(b, c, HDR_UNSUBACK, packet_id);
}

// Include FSM after protocol senders are defined
#include "broker/msg_path.h"
#include "broker/mqtt_fsm.h"

// =============================================================================
// Zero-Copy Publish Context
// =============================================================================

// Context for publishing - tracks stolen buffer and metadata
struct publish_ctx {
    struct broker *broker;
    u32 msg_idx;                    // Index into canonical_msg pool
    u32 stolen_buf_idx;             // Index of stolen recv buffer
    u8 *stolen_buf;                 // Pointer to stolen buffer data
    u64 sent_bitmap[TRIE_FD_SLOTS]; // Track which slots we've sent to
    u32 subscriber_count;           // Number of subscribers we're sending to
};

// =============================================================================
// Buffer Stealing
// =============================================================================

// Steal the publisher's recv buffer and give them a fresh one
// packet_len: size of the PUBLISH packet being stolen (remaining data is copied to new buffer)
// Returns: new recv_buf_idx for publisher, or BUF_POOL_INVALID on failure
INLINE u32 steal_recv_buffer(struct broker *b, struct client_slot *pub, u32 packet_len,
                              u32 *out_stolen_idx) {
    // Try to allocate a fresh buffer for the publisher
    u32 new_buf_idx = buf_pool_alloc(&b->recv_pool);
    if (new_buf_idx == BUF_POOL_INVALID) {
        return BUF_POOL_INVALID; // No buffers available, can't steal
    }

    // Steal the publisher's buffer
    *out_stolen_idx = pub->recv_buf_idx;

    // Copy remaining pipelined data to new buffer (starts at recv_start + packet_len)
    // This prevents loss of subsequent packets that arrived in the same recv
    u32 remaining = (pub->recv_len > packet_len) ? (pub->recv_len - packet_len) : 0;
    if (remaining > 0) {
        u8 *old_buf = buf_pool_get(&b->recv_pool, pub->recv_buf_idx);
        u8 *new_buf = buf_pool_get(&b->recv_pool, new_buf_idx);
        // Data is at recv_start offset, packet_len bytes consumed, copy rest
        memcpy(new_buf, old_buf + pub->recv_start + packet_len, remaining);
    }

    // Give the publisher the fresh buffer with remaining data at offset 0
    pub->recv_buf_idx = new_buf_idx;
    pub->recv_start   = 0;         // New buffer starts at 0
    pub->recv_len     = remaining; // Preserve pipelined data

    b->stolen_buffers++;
    return new_buf_idx;
}

// =============================================================================
// Late Materialization - Build PUBLISH at Send Time
// =============================================================================

// Forward publish to a single subscriber using zero-copy via FSM
// msg is pre-fetched by caller to avoid repeated lookups in fan-out loop
static i32 forward_publish_zc(struct broker *b, u32 slot_idx, struct client_slot *sub,
                               struct publish_ctx *pctx, struct canonical_msg *msg, u8 sub_qos) {
    // Downgrade QoS to minimum of publisher and subscriber
    u8 effective_qos = (msg->qos < sub_qos) ? msg->qos : sub_qos;

    // Delegate to FSM - it handles refcount, inflight, send_desc, and submission
    i32 rc = fsm_sub_start_delivery(b, sub, pctx->msg_idx, effective_qos, slot_idx);
    if (rc == 0) {
        pctx->subscriber_count++;
        log_debug("  -> fd=%d qos=%d (zc)", sub->fd, effective_qos);
    }
    return rc;
}

// Process collected trie matches - no callback overhead, fully inlined
INLINE void process_publish_matches(struct broker *b, struct publish_ctx *pctx,
                                    struct trie_match_result *matches) {
    // Pre-fetch message once for all nodes (not per-callback)
    struct canonical_msg *msg = msg_pool_get(&b->msg_pool, pctx->msg_idx);
    if (unlikely(!msg)) {
        return;
    }
    u8 pub_qos = msg->qos; // Cache for all loops

    // Process collected nodes (flat loop, no function pointer calls)
    for (u32 m = 0; m < matches->count; m++) {
        struct trie_node *node = matches->nodes[m];

        // Iterate slots in this node
        for (u32 slot = 0; slot < TRIE_FD_SLOTS; slot++) {
            u64 bits = node->fd_bitmap[slot] & ~pctx->sent_bitmap[slot];
            if (likely(bits == 0)) continue; // Skip empty slots quickly

            while (bits) {
                u32 bit_idx  = (u32)__builtin_ctzll(bits);
                u32 slot_idx = slot * BITS_PER_SLOT + bit_idx;

                // Mark as sent (avoid duplicates from multiple matching wildcards)
                pctx->sent_bitmap[slot] |= (1ULL << bit_idx);

                // Get client slot and validate generation to prevent slot-reuse race:
                // If slot was reused for a new client, generation won't match the
                // one stored when subscription was made → skip (stale subscription)
                struct client_slot *c = broker_get_client_unchecked(b, slot_idx);
                if (likely(c->state == CLIENT_ACTIVE && c->generation == node->fd_gen[slot_idx])) {
                    forward_publish_zc(b, slot_idx, c, pctx, msg, pub_qos);
                }

                bits &= bits - 1; // Clear lowest bit
            }
        }
    }
}

// =============================================================================
// MQTT Packet Processing
// =============================================================================

// Process a complete MQTT packet
// Returns: bytes consumed, or -1 on error (close connection)
INLINE i32 process_mqtt_packet(struct broker *b, struct client_slot *c, const u8 *buf, u32 len) {
    struct mqtt_fixed_header hdr;
    i32 hdr_len = mqtt_parse_fixed_header(buf, len, &hdr);
    if (hdr_len < 0) {
        return hdr_len;
    }

    u32 total_len     = (u32)hdr_len + hdr.remaining_len;
    const u8 *var_hdr = buf + hdr_len;
    u32 var_len       = hdr.remaining_len;

    // Get slot index for this client
    i32 slot_idx = b->fd_to_slot[c->fd];

    switch (hdr.type) {
    case MQTT_CONNECT: {
        if (c->state != CLIENT_ACTIVE || c->protocol_version != 0) {
            return -1;
        }

        struct mqtt_connect conn_pkt;
        i32 rc = mqtt_parse_connect(var_hdr, var_len, &conn_pkt);
        if (rc < 0) {
            // Send CONNACK with error (4 bytes: header + session_present + return_code)
            u8 connack_err[4] = {
                0x20, 0x02, 0x00, // Header + remaining_len + session_present=0
                (rc == MQTT_ERR_PROTOCOL) ? MQTT_CONNACK_PROTO_VERSION : MQTT_CONNACK_ID_REJECTED
            };
            send_buf(b, c, connack_err, 4);
            return -1;
        }

        broker_set_client_identity(b, (u32)slot_idx, conn_pkt.client_id.ptr, (u8)conn_pkt.client_id.len);
        c->protocol_version = conn_pkt.protocol_version;
        c->keepalive        = conn_pkt.keepalive;
        c->clean_session    = conn_pkt.clean_session;

        log_debug("CONNECT fd=%d slot=%d client='%.*s' keepalive=%d", c->fd, slot_idx,
                  (i32)c->client_id_len, c->client_id, c->keepalive);

        // Send CONNACK success (4 bytes)
        u8 connack_ok[4] = {0x20, 0x02, 0x00, MQTT_CONNACK_ACCEPTED};
        send_buf(b, c, connack_ok, 4);
        break;
    }

    case MQTT_PINGREQ: {
        if (c->protocol_version == 0) {
            return -1;
        }
        log_trace("PINGREQ fd=%d", c->fd);
        send_static(b, c, HDR_PINGRESP, 2);
        break;
    }

    case MQTT_DISCONNECT: {
        log_debug("DISCONNECT fd=%d slot=%d", c->fd, slot_idx);
        return -1;
    }

    case MQTT_PUBACK: {
        if (var_len < 2) {
            return -1;
        }
        u16 packet_id = mqtt_read_u16(var_hdr);
        log_debug("PUBACK fd=%d packet_id=%d", c->fd, packet_id);
        fsm_sub_puback_received(b, c, packet_id);
        break;
    }

    case MQTT_PUBREC: {
        if (var_len < 2) {
            return -1;
        }
        u16 packet_id = mqtt_read_u16(var_hdr);
        log_debug("PUBREC fd=%d packet_id=%d", c->fd, packet_id);
        fsm_sub_pubrec_received(b, c, packet_id);
        break;
    }

    case MQTT_PUBREL: {
        if (var_len < 2) {
            return -1;
        }
        u16 packet_id = mqtt_read_u16(var_hdr);
        log_debug("PUBREL fd=%d packet_id=%d", c->fd, packet_id);
        fsm_pub_pubrel_received(b, c, packet_id);
        break;
    }

    case MQTT_PUBCOMP: {
        if (var_len < 2) {
            return -1;
        }
        u16 packet_id = mqtt_read_u16(var_hdr);
        log_debug("PUBCOMP fd=%d packet_id=%d", c->fd, packet_id);
        fsm_sub_pubcomp_received(b, c, packet_id);
        break;
    }

    case MQTT_SUBSCRIBE: {
        if (c->protocol_version == 0) {
            return -1;
        }

        struct mqtt_subscribe sub_pkt;
        i32 rc = mqtt_parse_subscribe(var_hdr, var_len, &sub_pkt);
        if (rc < 0) {
            return -1;
        }

        u8 return_codes[MQTT_MAX_FILTERS];
        for (u16 i = 0; i < sub_pkt.filter_count; i++) {
            i32 sub_rc      = sub_add(b, (u32)slot_idx, c->generation, sub_pkt.filters[i].topic.ptr,
                                      sub_pkt.filters[i].topic.len, sub_pkt.filters[i].qos);
            return_codes[i] = (sub_rc == 0) ? sub_pkt.filters[i].qos : MQTT_SUBACK_FAILURE;

            if (sub_rc != 0) {
                log_warn("SUBSCRIBE failed slot=%d topic='%.*s'", slot_idx,
                         (i32)sub_pkt.filters[i].topic.len,
                         (const char *)sub_pkt.filters[i].topic.ptr);
            }

            log_debug("SUBSCRIBE fd=%d slot=%d topic='%.*s' qos=%d granted=%d", c->fd, slot_idx,
                      (i32)sub_pkt.filters[i].topic.len, (const char *)sub_pkt.filters[i].topic.ptr,
                      sub_pkt.filters[i].qos, return_codes[i]);
        }

        // Encode SUBACK into temp buffer and send via async io_uring
        u8 suback_buf[16]; // Max: 2 header + 2 pkt_id + 10 return codes
        u32 suback_len = mqtt_encode_suback(suback_buf, sub_pkt.packet_id, return_codes, sub_pkt.filter_count);
        send_buf(b, c, suback_buf, suback_len);
        break;
    }

    case MQTT_UNSUBSCRIBE: {
        if (c->protocol_version == 0) {
            return -1;
        }

        struct mqtt_unsubscribe unsub_pkt;
        i32 rc = mqtt_parse_unsubscribe(var_hdr, var_len, &unsub_pkt);
        if (rc < 0) {
            return -1;
        }

        for (u16 i = 0; i < unsub_pkt.filter_count; i++) {
            sub_remove(b, (u32)slot_idx, unsub_pkt.filters[i].ptr, unsub_pkt.filters[i].len);

            log_debug("UNSUBSCRIBE fd=%d slot=%d topic='%.*s'", c->fd, slot_idx,
                      (i32)unsub_pkt.filters[i].len, (const char *)unsub_pkt.filters[i].ptr);
        }

        // Send UNSUBACK (4 bytes: header + packet_id)
        send_unsuback(b, c, unsub_pkt.packet_id);
        break;
    }

    case MQTT_PUBLISH: {
        if (c->protocol_version == 0) {
            return -1;
        }

        struct mqtt_publish pub_pkt;
        i32 rc = mqtt_parse_publish(hdr.flags, var_hdr, var_len, &pub_pkt);
        if (rc < 0) {
            return -1;
        }

        log_debug("PUBLISH topic='%.*s' qos=%d", (i32)pub_pkt.topic.len,
                  (const char *)pub_pkt.topic.ptr, pub_pkt.qos);

        // Publisher ceremony - handles PUBACK/PUBREC and duplicate detection
        // Returns false for QoS 2 duplicates (don't fan-out again)
        if (!fsm_pub_received(b, c, pub_pkt.qos, pub_pkt.packet_id)) {
            break; // Duplicate QoS 2 or error - don't fan-out
        }

        // === Zero-Copy Fan-Out ===

        // Backpressure: if send_desc pool is <10% free, drop message
        // This prevents cascading failure when sends back up
        u32 sd_free = b->send_desc_pool.free_count;
        u32 sd_threshold = b->send_desc_pool.capacity / 10; // 10%
        if (sd_free < sd_threshold) {
            b->msgs_dropped++;
            b->drops_send_desc_empty++;
            break; // Drop silently - backpressure
        }

        // Allocate canonical message metadata
        u32 msg_idx = msg_pool_alloc(&b->msg_pool);
        if (msg_idx == MSG_POOL_INVALID) {
            b->msgs_dropped++;
            b->drops_msg_pool_empty++;
            log_warn("PUBLISH dropped - msg_pool exhausted");
            break; // Don't fail connection, just drop message
        }

        // Steal the publisher's recv buffer (pass total_len so remaining data is preserved)
        u32 stolen_buf_idx;
        u32 new_buf_idx = steal_recv_buffer(b, c, total_len, &stolen_buf_idx);
        if (new_buf_idx == BUF_POOL_INVALID) {
            // Can't steal - no free buffers. Free msg and drop.
            msg_pool_free(&b->msg_pool, msg_idx);
            b->msgs_dropped++;
            b->drops_msg_pool_empty++;
            log_warn("PUBLISH dropped - no buffers for steal");
            break;
        }

        // Get pointer to stolen buffer
        u8 *stolen_buf = buf_pool_get(&b->recv_pool, stolen_buf_idx);

        // Setup canonical message metadata (zero-copy - offsets into stolen buffer)
        struct canonical_msg *msg = msg_pool_get(&b->msg_pool, msg_idx);
        msg->buf_idx              = stolen_buf_idx;
        // Calculate offsets: topic.ptr and payload are pointers into original buffer
        // We need to convert them to offsets from buffer start
        u8 *recv_buf       = stolen_buf; // This is the original buffer
        msg->topic_off     = (u16)(pub_pkt.topic.ptr - recv_buf);
        msg->topic_len     = pub_pkt.topic.len;
        msg->payload_off   = (u32)(pub_pkt.payload - recv_buf);
        msg->payload_len   = pub_pkt.payload_len;
        msg->qos           = pub_pkt.qos;
        msg->retain        = pub_pkt.retain ? 1 : 0;
        msg->dup           = pub_pkt.dup ? 1 : 0;

        // Take base reference BEFORE fan-out to prevent premature freeing.
        // This ensures the buffer survives even if submit_send_zc fails mid-iteration.
        // Without this, if submit fails and decrements refcount to 0, subsequent
        // subscribers in the same fan-out would access freed memory.
        msg_pool_ref(&b->msg_pool, msg_idx);

        // Setup publish context
        struct publish_ctx pctx;
        pctx.broker           = b;
        pctx.msg_idx          = msg_idx;
        pctx.stolen_buf_idx   = stolen_buf_idx;
        pctx.stolen_buf       = stolen_buf;
        pctx.subscriber_count = 0;
        memset(pctx.sent_bitmap, 0, sizeof(pctx.sent_bitmap));

        // Fan-out to matching subscribers (collect mode - no callback overhead)
        struct trie_match_result matches;
        trie_match_collect(&b->trie, pub_pkt.topic.ptr, pub_pkt.topic.len, &matches);
        process_publish_matches(b, &pctx, &matches);

        // Flush all prepared SQEs with single atomic (batched submission)
        ring_flush_sqes(&b->ring);

        // Release base reference. If this brings refcount to 0, all sends either
        // failed or there were no subscribers - free the stolen buffer and message.
        u32 final_ref = msg_pool_unref(&b->msg_pool, msg_idx);
        if (final_ref == 0) {
            buf_pool_free(&b->recv_pool, stolen_buf_idx);
            msg_pool_free(&b->msg_pool, msg_idx);
        }
        // Otherwise, buffer is freed when last send completes (refcount -> 0)

        b->msgs_published++;
        break;
    }

    default:
        log_warn("Unknown packet type=%d", hdr.type);
        return -1;
    }

    return (i32)total_len;
}

#endif // BROKER_HANDLER_H
