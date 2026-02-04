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

// Forward declare submit functions (defined in loop.h which includes this file)
INLINE void submit_send(struct broker *b, i32 fd, const u8 *buf, u32 len);
INLINE void submit_send_zc(struct broker *b, i32 fd, u32 send_desc_idx);
INLINE void submit_send_writev_simple(struct broker *b, i32 fd, struct iovec *iov, u32 nr_vecs);

// Static headers for zero-copy protocol responses (type<<4 | flags, remaining_len)
static const u8 HDR_CONNACK[4]  = {0x20, 0x02, 0x00, 0x00}; // Session=0, accepted
static const u8 HDR_PUBACK[2]   = {0x40, 0x02};
static const u8 HDR_PUBREC[2]   = {0x50, 0x02};
static const u8 HDR_PUBREL[2]   = {0x62, 0x02};
static const u8 HDR_PUBCOMP[2]  = {0x70, 0x02};
static const u8 HDR_UNSUBACK[2] = {0xB0, 0x02};
static const u8 HDR_PINGRESP[2] = {0xD0, 0x00};

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

    // Copy remaining pipelined data to new buffer
    // This prevents loss of subsequent packets that arrived in the same recv
    u32 remaining = (pub->recv_len > packet_len) ? (pub->recv_len - packet_len) : 0;
    if (remaining > 0) {
        u8 *old_buf = buf_pool_get(&b->recv_pool, pub->recv_buf_idx);
        u8 *new_buf = buf_pool_get(&b->recv_pool, new_buf_idx);
        memcpy(new_buf, old_buf + packet_len, remaining);
    }

    // Give the publisher the fresh buffer with remaining data
    pub->recv_buf_idx = new_buf_idx;
    pub->recv_len     = remaining; // Preserve pipelined data

    b->stolen_buffers++;
    return new_buf_idx;
}

// =============================================================================
// Late Materialization - Build PUBLISH at Send Time
// =============================================================================

// Forward publish to a single subscriber using zero-copy
// Materializes PUBLISH header on-the-fly, uses scatter-gather for topic/payload
static i32 forward_publish_zc(struct broker *b, u32 slot_idx, struct client_slot *sub,
                               struct publish_ctx *pctx, u8 sub_qos) {
    struct canonical_msg *msg = msg_pool_get(&b->msg_pool, pctx->msg_idx);
    if (!msg) {
        return -1;
    }

    // Downgrade QoS to minimum of publisher and subscriber
    u8 effective_qos = (msg->qos < sub_qos) ? msg->qos : sub_qos;

    // Allocate send descriptor
    u32 sd_idx = send_desc_pool_alloc(&b->send_desc_pool);
    if (sd_idx == SEND_DESC_INVALID) {
        b->msgs_dropped++;
        b->drops_send_desc_empty++;
        return -1;
    }

    struct send_desc *sd = send_desc_pool_get(&b->send_desc_pool, sd_idx);
    sd->msg_idx          = pctx->msg_idx;
    sd->slot_idx         = slot_idx;
    sd->slot_gen         = sub->generation; // For stale detection
    sd->sub_qos          = effective_qos;

    // For QoS 1/2, allocate packet ID and track in inflight
    if (effective_qos > 0) {
        u16 packet_id = 0;
        i32 inf_idx   = client_inflight_alloc(sub, effective_qos, pctx->msg_idx, &packet_id);
        if (inf_idx < 0) {
            send_desc_pool_free(&b->send_desc_pool, sd_idx);
            b->msgs_dropped++;
            b->drops_inflight_full++;
            return -1;
        }

        sd->packet_id     = packet_id;
        sd->pkt_id_be[0]  = (u8)(packet_id >> 8);
        sd->pkt_id_be[1]  = (u8)(packet_id & 0xFF);

        // Link send_desc to inflight entry
        sub->inflight[inf_idx].send_desc_idx = sd_idx;
    } else {
        sd->packet_id = 0;
    }

    // Late materialization: build PUBLISH header now
    materialize_publish_header(sd, msg->topic_len, msg->payload_len, effective_qos,
                               msg->retain != 0, msg->dup != 0);

    // Setup scatter-gather iovecs pointing into stolen buffer
    const u8 *topic_ptr   = pctx->stolen_buf + msg->topic_off;
    const u8 *payload_ptr = pctx->stolen_buf + msg->payload_off;
    setup_send_iovec(sd, topic_ptr, msg->topic_len, payload_ptr, msg->payload_len);

    // Increment refcount before submitting send
    msg_pool_ref(&b->msg_pool, pctx->msg_idx);
    pctx->subscriber_count++;

    // Submit zero-copy send
    sd->state = SEND_INFLIGHT;
    submit_send_zc(b, sub->fd, sd_idx);

    log_debug("  -> fd=%d qos=%d pkt_id=%d (zc)", sub->fd, effective_qos, sd->packet_id);
    return 0;
}

// Callback for trie node match - send to matching slots
static void publish_node_cb(void *ctx, struct trie_node *node) {
    struct publish_ctx *pctx = (struct publish_ctx *)ctx;
    struct broker *b         = pctx->broker;

    // Iterate slots in this node
    for (u32 slot = 0; slot < TRIE_FD_SLOTS; slot++) {
        u64 bits = node->fd_bitmap[slot] & ~pctx->sent_bitmap[slot];

        while (bits) {
            u32 bit_idx  = (u32)__builtin_ctzll(bits);
            u32 slot_idx = slot * BITS_PER_SLOT + bit_idx;

            // Mark as sent (avoid duplicates from multiple matching wildcards)
            pctx->sent_bitmap[slot] |= (1ULL << bit_idx);

            // Get client slot
            struct client_slot *c = broker_get_client(b, slot_idx);
            if (!c || c->state != CLIENT_ACTIVE) {
                // TODO: Queue for DORMANT clients
                bits &= bits - 1;
                continue;
            }

            // Forward the publish using zero-copy
            // TODO: Get per-subscription QoS from trie node
            struct canonical_msg *msg = msg_pool_get(&b->msg_pool, pctx->msg_idx);
            u8 sub_qos = msg ? msg->qos : 0; // For now, use publisher's QoS
            forward_publish_zc(b, slot_idx, c, pctx, sub_qos);

            bits &= bits - 1; // Clear lowest bit
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
            // Send CONNACK with error
            u8 slot = client_get_resp_slot(c);
            c->resp_buf[slot][0] = 0; // session present = false
            c->resp_buf[slot][1] =
                (rc == MQTT_ERR_PROTOCOL) ? MQTT_CONNACK_PROTO_VERSION : MQTT_CONNACK_ID_REJECTED;
            c->resp_iov[slot][0].iov_base = (void *)HDR_CONNACK;
            c->resp_iov[slot][0].iov_len  = 2;
            c->resp_iov[slot][1].iov_base = c->resp_buf[slot];
            c->resp_iov[slot][1].iov_len  = 2;
            submit_send_writev_simple(b, c->fd, c->resp_iov[slot], 2);
            return -1;
        }

        client_set_identity(c, conn_pkt.client_id.ptr, (u8)conn_pkt.client_id.len);
        c->protocol_version = conn_pkt.protocol_version;
        c->keepalive        = conn_pkt.keepalive;
        c->clean_session    = conn_pkt.clean_session;

        log_debug("CONNECT fd=%d slot=%d client='%.*s' keepalive=%d", c->fd, slot_idx,
                  (i32)c->client_id_len, c->client_id, c->keepalive);

        // Send CONNACK success
        u8 slot2 = client_get_resp_slot(c);
        c->resp_buf[slot2][0]          = 0; // session present = false
        c->resp_buf[slot2][1]          = MQTT_CONNACK_ACCEPTED;
        c->resp_iov[slot2][0].iov_base = (void *)HDR_CONNACK;
        c->resp_iov[slot2][0].iov_len  = 2;
        c->resp_iov[slot2][1].iov_base = c->resp_buf[slot2];
        c->resp_iov[slot2][1].iov_len  = 2;
        submit_send_writev_simple(b, c->fd, c->resp_iov[slot2], 2);
        break;
    }

    case MQTT_PINGREQ: {
        if (c->protocol_version == 0) {
            return -1;
        }
        log_trace("PINGREQ fd=%d", c->fd);
        // PINGRESP is just 2 bytes, no payload
        u8 ping_slot = client_get_resp_slot(c);
        c->resp_iov[ping_slot][0].iov_base = (void *)HDR_PINGRESP;
        c->resp_iov[ping_slot][0].iov_len  = 2;
        submit_send_writev_simple(b, c->fd, c->resp_iov[ping_slot], 1);
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
        u16 packet_id            = mqtt_read_u16(var_hdr);
        struct inflight_msg *inf = client_inflight_find(c, packet_id);
        if (inf && inf->state == INFLIGHT_WAIT_PUBACK) {
            log_debug("PUBACK fd=%d packet_id=%d - delivery complete", c->fd, packet_id);
            // Note: msg_pool refcount is decremented during send completion
            client_inflight_free(c, inf);
        } else {
            log_debug("PUBACK fd=%d packet_id=%d - unexpected", c->fd, packet_id);
        }
        break;
    }

    case MQTT_PUBREC: {
        if (var_len < 2) {
            return -1;
        }
        u16 packet_id            = mqtt_read_u16(var_hdr);
        struct inflight_msg *inf = client_inflight_find(c, packet_id);
        if (inf && inf->state == INFLIGHT_WAIT_PUBREC) {
            log_debug("PUBREC fd=%d packet_id=%d - sending PUBREL", c->fd, packet_id);
            inf->state = INFLIGHT_WAIT_PUBCOMP;

            // Send PUBREL using client scratch area
            struct iovec *pubrel_iov = client_setup_resp_pkt(c, HDR_PUBREL, packet_id);
            submit_send_writev_simple(b, c->fd, pubrel_iov, 2);
        } else {
            log_debug("PUBREC fd=%d packet_id=%d - unexpected", c->fd, packet_id);
        }
        break;
    }

    case MQTT_PUBREL: {
        if (var_len < 2) {
            return -1;
        }
        u16 packet_id            = mqtt_read_u16(var_hdr);
        struct inflight_msg *inf = client_inflight_find(c, packet_id);
        if (inf && inf->state == INFLIGHT_WAIT_PUBREL && inf->direction == 1) {
            log_debug("PUBREL fd=%d packet_id=%d - sending PUBCOMP", c->fd, packet_id);
            client_inflight_free(c, inf);
        } else {
            log_debug("PUBREL fd=%d packet_id=%d - sending PUBCOMP (no state)", c->fd, packet_id);
        }
        // Send PUBCOMP using client scratch area
        struct iovec *pubcomp_iov = client_setup_resp_pkt(c, HDR_PUBCOMP, packet_id);
        submit_send_writev_simple(b, c->fd, pubcomp_iov, 2);
        break;
    }

    case MQTT_PUBCOMP: {
        if (var_len < 2) {
            return -1;
        }
        u16 packet_id            = mqtt_read_u16(var_hdr);
        struct inflight_msg *inf = client_inflight_find(c, packet_id);
        if (inf && inf->state == INFLIGHT_WAIT_PUBCOMP) {
            log_debug("PUBCOMP fd=%d packet_id=%d - delivery complete", c->fd, packet_id);
            client_inflight_free(c, inf);
        } else {
            log_debug("PUBCOMP fd=%d packet_id=%d - unexpected", c->fd, packet_id);
        }
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
            i32 sub_rc      = sub_add(b, (u32)slot_idx, sub_pkt.filters[i].topic.ptr,
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

        // Encode SUBACK into resp_buf: [0x90, remaining_len, pkt_id_hi, pkt_id_lo, return_codes...]
        u8 suback_slot = client_get_resp_slot(c);
        u32 suback_len =
            mqtt_encode_suback(c->resp_buf[suback_slot], sub_pkt.packet_id, return_codes, sub_pkt.filter_count);
        c->resp_iov[suback_slot][0].iov_base = c->resp_buf[suback_slot];
        c->resp_iov[suback_slot][0].iov_len  = suback_len;
        submit_send_writev_simple(b, c->fd, c->resp_iov[suback_slot], 1);
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

        // Send UNSUBACK using writev
        struct iovec *unsuback_iov = client_setup_resp_pkt(c, HDR_UNSUBACK, unsub_pkt.packet_id);
        submit_send_writev_simple(b, c->fd, unsuback_iov, 2);
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

        // Handle publisher QoS acknowledgments BEFORE fan-out
        if (pub_pkt.qos == 1) {
            // Send PUBACK to publisher
            struct iovec *puback_iov = client_setup_resp_pkt(c, HDR_PUBACK, pub_pkt.packet_id);
            submit_send_writev_simple(b, c->fd, puback_iov, 2);
        } else if (pub_pkt.qos == 2) {
            struct inflight_msg *existing = client_inflight_find(c, pub_pkt.packet_id);
            if (existing && existing->direction == 1) {
                log_debug("PUBLISH fd=%d qos=2 pkt_id=%d DUP - resending PUBREC", c->fd,
                          pub_pkt.packet_id);
                // Resend PUBREC
                struct iovec *pubrec_dup_iov = client_setup_resp_pkt(c, HDR_PUBREC, pub_pkt.packet_id);
                submit_send_writev_simple(b, c->fd, pubrec_dup_iov, 2);
                break; // Don't re-fan-out duplicate
            }

            i32 inf_idx = client_inflight_track_incoming(c, pub_pkt.packet_id);
            if (inf_idx < 0) {
                log_debug("PUBLISH fd=%d qos=2 - inflight full, dropping", c->fd);
                return -1;
            }

            // Send PUBREC
            struct iovec *pubrec_iov = client_setup_resp_pkt(c, HDR_PUBREC, pub_pkt.packet_id);
            submit_send_writev_simple(b, c->fd, pubrec_iov, 2);
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

        // Fan-out to matching subscribers
        trie_match(&b->trie, pub_pkt.topic.ptr, pub_pkt.topic.len, publish_node_cb, &pctx);

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
