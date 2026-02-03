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

// Forward declare submit_send (defined in loop.h which includes this file)
INLINE void submit_send(struct broker *b, i32 fd, const u8 *buf, u32 len);

// =============================================================================
// Publish Fan-out Context
// =============================================================================

struct publish_ctx {
    struct broker *broker;
    struct mqtt_publish *pub;
    u64 sent_bitmap[TRIE_FD_SLOTS]; // Track which slots we've sent to
};

// Forward declare submit_send_ctx (defined in loop.h which includes this file)
INLINE void submit_send_ctx(struct broker *b, i32 fd, const u8 *buf, u32 len, u32 ctx);

// Forward a publish to a single subscriber using inflight tracking
// Returns: 0 on success, -1 on failure
static i32 forward_publish_to_client(struct broker *b, u32 slot_idx, struct client_slot *c,
                                     struct mqtt_publish *pub, u8 sub_qos) {
    // Downgrade QoS to minimum of publisher and subscriber
    u8 effective_qos = (pub->qos < sub_qos) ? pub->qos : sub_qos;

    // ALL sends use inflight buffer to avoid use-after-free with async io_uring
    u16 packet_id = 0;
    i32 inf_idx;

    if (effective_qos == 0) {
        // QoS 0: Allocate inflight slot for buffer only (no state tracking)
        inf_idx = client_inflight_alloc(c, 0, &packet_id, b->max_inflight);
    } else {
        // QoS 1/2: Allocate with state tracking
        inf_idx = client_inflight_alloc(c, effective_qos, &packet_id, b->max_inflight);
    }

    if (inf_idx < 0) {
        b->msgs_dropped++;
        return -1;
    }

    struct inflight_msg *inf = &c->inflight[inf_idx];

    // Encode into inflight buffer (persists until send CQE for QoS 0, ACK for QoS 1/2)
    struct mqtt_publish fwd = *pub;
    fwd.qos                 = effective_qos;
    fwd.dup                 = false;

    u32 fwd_len = mqtt_encode_publish(inf->send_buf, &fwd, packet_id);
    if (fwd_len > SEND_BUF_SIZE) {
        client_inflight_free(c, inf);
        log_debug("Message too large for inflight buffer fd=%d", c->fd);
        return -1;
    }
    inf->data_len = (u16)fwd_len;

    // Submit with context so handle_send can free buffer
    u32 ctx = make_send_ctx(slot_idx, (u8)inf_idx, effective_qos);
    submit_send_ctx(b, c->fd, inf->send_buf, fwd_len, ctx);

    if (effective_qos > 0) {
        log_debug("  -> fd=%d qos=%d pkt_id=%d", c->fd, effective_qos, packet_id);
    }

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

            // Forward the publish (uses inflight buffer for ALL QoS levels)
            // TODO: Get per-subscription QoS from trie node
            u8 sub_qos = pctx->pub->qos; // For now, use publisher's QoS
            forward_publish_to_client(b, slot_idx, c, pctx->pub, sub_qos);

            log_debug("  -> slot=%d fd=%d", slot_idx, c->fd);

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
            // Already connected or not in right state
            return -1;
        }

        struct mqtt_connect conn_pkt;
        i32 rc = mqtt_parse_connect(var_hdr, var_len, &conn_pkt);
        if (rc < 0) {
            u8 reason =
                (rc == MQTT_ERR_PROTOCOL) ? MQTT_CONNACK_PROTO_VERSION : MQTT_CONNACK_ID_REJECTED;
            u8 *pbuf = client_get_proto_buf(c, b->proto_buf_count);
            mqtt_encode_connack(pbuf, false, reason);
            submit_send(b, c->fd, pbuf, MQTT_CONNACK_SIZE);
            return -1;
        }

        // Set client identity and mark as connected
        client_set_identity(c, conn_pkt.client_id.ptr, (u8)conn_pkt.client_id.len);
        c->protocol_version = conn_pkt.protocol_version; // 4 = MQTT 3.1.1
        c->keepalive        = conn_pkt.keepalive;
        c->clean_session    = conn_pkt.clean_session;

        // TODO: Check for existing session with same client_id (session takeover)

        log_debug("CONNECT fd=%d slot=%d client='%.*s' keepalive=%d", c->fd, slot_idx,
                  (i32)c->client_id_len, c->client_id, c->keepalive);

        u8 *pbuf = client_get_proto_buf(c, b->proto_buf_count);
        mqtt_encode_connack(pbuf, false, MQTT_CONNACK_ACCEPTED);
        submit_send(b, c->fd, pbuf, MQTT_CONNACK_SIZE);
        break;
    }

    case MQTT_PINGREQ: {
        if (c->protocol_version == 0) {
            return -1; // Not connected
        }
        log_trace("PINGREQ fd=%d", c->fd);
        u8 *pbuf = client_get_proto_buf(c, b->proto_buf_count);
        pbuf[0]  = MQTT_PINGRESP << MQTT_TYPE_SHIFT;
        pbuf[1]  = 0;
        submit_send(b, c->fd, pbuf, 2);
        break;
    }

    case MQTT_DISCONNECT: {
        log_debug("DISCONNECT fd=%d slot=%d", c->fd, slot_idx);
        return -1;
    }

    case MQTT_PUBACK: {
        // Subscriber acknowledged a QoS 1 message we sent
        if (var_len < 2) {
            return -1;
        }
        u16 packet_id            = mqtt_read_u16(var_hdr);
        struct inflight_msg *inf = client_inflight_find(c, packet_id);
        if (inf && inf->state == INFLIGHT_WAIT_PUBACK) {
            log_debug("PUBACK fd=%d packet_id=%d - delivery complete", c->fd, packet_id);
            client_inflight_free(c, inf);
        } else {
            log_debug("PUBACK fd=%d packet_id=%d - unexpected", c->fd, packet_id);
        }
        break;
    }

    case MQTT_PUBREC: {
        // QoS 2: We sent PUBLISH, client sends PUBREC, we send PUBREL
        if (var_len < 2) {
            return -1;
        }
        u16 packet_id            = mqtt_read_u16(var_hdr);
        struct inflight_msg *inf = client_inflight_find(c, packet_id);
        if (inf && inf->state == INFLIGHT_WAIT_PUBREC) {
            log_debug("PUBREC fd=%d packet_id=%d - sending PUBREL", c->fd, packet_id);
            // Transition to WAIT_PUBCOMP, send PUBREL
            inf->state = INFLIGHT_WAIT_PUBCOMP;
            u8 pubrel[MQTT_PUBREL_SIZE];
            mqtt_encode_pubrel(pubrel, packet_id);
            // Encode into inflight buffer for persistence
            for (u32 i = 0; i < MQTT_PUBREL_SIZE; i++) {
                inf->send_buf[i] = pubrel[i];
            }
            inf->data_len = MQTT_PUBREL_SIZE;
            submit_send(b, c->fd, inf->send_buf, MQTT_PUBREL_SIZE);
        } else {
            log_debug("PUBREC fd=%d packet_id=%d - unexpected", c->fd, packet_id);
        }
        break;
    }

    case MQTT_PUBREL: {
        // QoS 2: Client sent PUBLISH, we sent PUBREC, client sends PUBREL
        // Now we deliver and send PUBCOMP
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
        // Send PUBCOMP
        u8 *pbuf = client_get_proto_buf(c, b->proto_buf_count);
        mqtt_encode_pubcomp(pbuf, packet_id);
        submit_send(b, c->fd, pbuf, MQTT_PUBCOMP_SIZE);
        break;
    }

    case MQTT_PUBCOMP: {
        // QoS 2: We sent PUBLISH → got PUBREC → sent PUBREL → got PUBCOMP
        // Delivery complete
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
            return -1; // Not connected
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
                         (i32)sub_pkt.filters[i].topic.len, (const char *)sub_pkt.filters[i].topic.ptr);
            }

            log_debug("SUBSCRIBE fd=%d slot=%d topic='%.*s' qos=%d granted=%d", c->fd, slot_idx,
                      (i32)sub_pkt.filters[i].topic.len, (const char *)sub_pkt.filters[i].topic.ptr,
                      sub_pkt.filters[i].qos, return_codes[i]);
        }

        u8 *pbuf       = client_get_proto_buf(c, b->proto_buf_count);
        u32 suback_len = mqtt_encode_suback(pbuf, sub_pkt.packet_id, return_codes, sub_pkt.filter_count);
        submit_send(b, c->fd, pbuf, suback_len);
        break;
    }

    case MQTT_UNSUBSCRIBE: {
        if (c->protocol_version == 0) {
            return -1; // Not connected
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

        u8 *pbuf = client_get_proto_buf(c, b->proto_buf_count);
        mqtt_encode_unsuback(pbuf, unsub_pkt.packet_id);
        submit_send(b, c->fd, pbuf, MQTT_UNSUBACK_SIZE);
        break;
    }

    case MQTT_PUBLISH: {
        if (c->protocol_version == 0) {
            return -1; // Not connected
        }

        struct mqtt_publish pub_pkt;
        i32 rc = mqtt_parse_publish(hdr.flags, var_hdr, var_len, &pub_pkt);
        if (rc < 0) {
            return -1;
        }

        log_debug("PUBLISH topic='%.*s' qos=%d", (i32)pub_pkt.topic.len,
                  (const char *)pub_pkt.topic.ptr, pub_pkt.qos);

        // Handle QoS acknowledgment to publisher
        if (pub_pkt.qos == 1) {
            // QoS 1: Send PUBACK immediately
            u8 *pbuf = client_get_proto_buf(c, b->proto_buf_count);
            mqtt_encode_puback(pbuf, pub_pkt.packet_id);
            submit_send(b, c->fd, pbuf, MQTT_PUBACK_SIZE);
        } else if (pub_pkt.qos == 2) {
            // QoS 2: Check if this is a duplicate (DUP=1)
            struct inflight_msg *existing = client_inflight_find(c, pub_pkt.packet_id);
            if (existing && existing->direction == 1) {
                // Duplicate - resend PUBREC but don't re-deliver
                log_debug("PUBLISH fd=%d qos=2 pkt_id=%d DUP - resending PUBREC", c->fd,
                          pub_pkt.packet_id);
                u8 *pbuf = client_get_proto_buf(c, b->proto_buf_count);
                mqtt_encode_pubrec(pbuf, pub_pkt.packet_id);
                submit_send(b, c->fd, pbuf, MQTT_PUBREC_SIZE);
                break; // Don't fan out again
            }

            // Track this message for QoS 2 flow
            i32 inf_idx = client_inflight_track_incoming(c, pub_pkt.packet_id);
            if (inf_idx < 0) {
                log_debug("PUBLISH fd=%d qos=2 - inflight full, dropping", c->fd);
                return -1;
            }

            // Send PUBREC to publisher
            u8 *pbuf = client_get_proto_buf(c, b->proto_buf_count);
            mqtt_encode_pubrec(pbuf, pub_pkt.packet_id);
            submit_send(b, c->fd, pbuf, MQTT_PUBREC_SIZE);
        }

        // Fan out to matching subscribers via trie
        struct publish_ctx pctx;
        pctx.broker = b;
        pctx.pub    = &pub_pkt;
        memset(pctx.sent_bitmap, 0, sizeof(pctx.sent_bitmap));

        trie_match(&b->trie, pub_pkt.topic.ptr, pub_pkt.topic.len, publish_node_cb, &pctx);

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
