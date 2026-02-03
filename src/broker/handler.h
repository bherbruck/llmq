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

            // Forward the publish
            struct mqtt_publish fwd = *pctx->pub;
            fwd.qos                 = 0; // TODO: track per-subscription QoS
            fwd.dup                 = false;

            u8 fwd_buf[RECV_BUF_SIZE];
            u32 fwd_len = mqtt_encode_publish(fwd_buf, &fwd, 0);
            submit_send(b, c->fd, fwd_buf, fwd_len);

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
            u8 connack[MQTT_CONNACK_SIZE];
            u8 reason =
                (rc == MQTT_ERR_PROTOCOL) ? MQTT_CONNACK_PROTO_VERSION : MQTT_CONNACK_ID_REJECTED;
            mqtt_encode_connack(connack, false, reason);
            submit_send(b, c->fd, connack, MQTT_CONNACK_SIZE);
            return -1;
        }

        // Set client identity and mark as connected
        client_set_identity(c, conn_pkt.client_id.ptr, (u8)conn_pkt.client_id.len);
        c->protocol_version = conn_pkt.protocol_version; // 4 = MQTT 3.1.1
        c->keepalive        = conn_pkt.keepalive;
        c->clean_session    = conn_pkt.clean_session;

        // TODO: Check for existing session with same client_id (session takeover)

        log_info("CONNECT fd=%d slot=%d client='%.*s' keepalive=%d", c->fd, slot_idx,
                 (i32)c->client_id_len, c->client_id, c->keepalive);

        u8 connack[MQTT_CONNACK_SIZE];
        mqtt_encode_connack(connack, false, MQTT_CONNACK_ACCEPTED);
        submit_send(b, c->fd, connack, MQTT_CONNACK_SIZE);
        break;
    }

    case MQTT_PINGREQ: {
        if (c->protocol_version == 0) {
            return -1; // Not connected
        }
        log_trace("PINGREQ fd=%d", c->fd);
        u8 pingresp[2] = {MQTT_PINGRESP << 4, 0};
        submit_send(b, c->fd, pingresp, 2);
        break;
    }

    case MQTT_DISCONNECT: {
        log_info("DISCONNECT fd=%d slot=%d", c->fd, slot_idx);
        return -1;
    }

    case MQTT_PUBACK: {
        // Subscriber acknowledged a QoS 1 message we sent
        if (var_len >= 2) {
            u16 packet_id = mqtt_read_u16(var_hdr);
            log_debug("PUBACK fd=%d packet_id=%d", c->fd, packet_id);
        }
        break;
    }

    case MQTT_PUBREC:
    case MQTT_PUBREL:
    case MQTT_PUBCOMP: {
        // QoS 2 flow - not fully implemented yet
        log_debug("QoS2 packet type=%d fd=%d", hdr.type, c->fd);
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

            log_info("SUBSCRIBE fd=%d slot=%d topic='%.*s' qos=%d granted=%d", c->fd, slot_idx,
                     (i32)sub_pkt.filters[i].topic.len, (const char *)sub_pkt.filters[i].topic.ptr,
                     sub_pkt.filters[i].qos, return_codes[i]);
        }

        u8 suback[MQTT_SUBACK_MAX];
        u32 suback_len =
            mqtt_encode_suback(suback, sub_pkt.packet_id, return_codes, sub_pkt.filter_count);
        submit_send(b, c->fd, suback, suback_len);
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

            log_info("UNSUBSCRIBE fd=%d slot=%d topic='%.*s'", c->fd, slot_idx,
                     (i32)unsub_pkt.filters[i].len, (const char *)unsub_pkt.filters[i].ptr);
        }

        u8 unsuback[MQTT_UNSUBACK_SIZE];
        mqtt_encode_unsuback(unsuback, unsub_pkt.packet_id);
        submit_send(b, c->fd, unsuback, MQTT_UNSUBACK_SIZE);
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

        log_info("PUBLISH fd=%d slot=%d topic='%.*s' qos=%d len=%u", c->fd, slot_idx,
                 (i32)pub_pkt.topic.len, (const char *)pub_pkt.topic.ptr, pub_pkt.qos,
                 pub_pkt.payload_len);

        if (pub_pkt.qos == 1) {
            u8 puback[MQTT_PUBACK_SIZE];
            mqtt_encode_puback(puback, pub_pkt.packet_id);
            submit_send(b, c->fd, puback, MQTT_PUBACK_SIZE);
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
