// mqtt/subscribe.h - SUBSCRIBE/SUBACK packet handling
// Zero-copy: topic filters point into receive buffer

#ifndef MQTT_SUBSCRIBE_H
#define MQTT_SUBSCRIBE_H

#include "mqtt/packet.h"

// =============================================================================
// SUBSCRIBE Packet Parsing
// =============================================================================

// Maximum topic filters per SUBSCRIBE packet
#define MQTT_MAX_FILTERS 8
#define MQTT_SUBACK_MAX  16 // Max SUBACK size: 5 (header) + 2 (id) + 8 (codes)

struct mqtt_subscribe {
    u16 packet_id;
    u16 filter_count;

    // Topic filters (pointers into receive buffer)
    struct {
        struct mqtt_str topic;
        u8 qos;
    } filters[MQTT_MAX_FILTERS];
};

// Parse SUBSCRIBE packet (variable header + payload)
// Returns: MQTT_OK or error
INLINE i32 mqtt_parse_subscribe(const u8 *buf, u32 len, struct mqtt_subscribe *sub) {
    if (len < 2) {
        return MQTT_INCOMPLETE;
    }

    // Packet ID
    sub->packet_id = mqtt_read_u16(buf);
    u32 pos        = 2;

    // Parse topic filters
    sub->filter_count = 0;

    while (pos < len && sub->filter_count < MQTT_MAX_FILTERS) {
        // Topic filter string
        i32 rc = mqtt_parse_string(buf + pos, len - pos, &sub->filters[sub->filter_count].topic);
        if (rc < 0) {
            return rc;
        }
        pos += (u32)rc;

        // QoS byte
        if (pos >= len) {
            return MQTT_ERR_MALFORMED;
        }
        u8 qos = buf[pos++];
        if (qos > 2) {
            return MQTT_ERR_MALFORMED;
        }
        sub->filters[sub->filter_count].qos = qos;

        sub->filter_count++;
    }

    if (sub->filter_count == 0) {
        return MQTT_ERR_MALFORMED; // Must have at least one filter
    }

    return MQTT_OK;
}

// =============================================================================
// SUBACK Encoding
// =============================================================================

// Encode SUBACK response
// return_codes: array of QoS granted or 0x80 for failure
// Returns: total bytes written
INLINE u32 mqtt_encode_suback(u8 *buf, u16 packet_id, const u8 *return_codes, u16 count) {
    // Fixed header
    buf[0] = MQTT_SUBACK << MQTT_TYPE_SHIFT;

    // Remaining length = packet_id (2) + return_codes (count)
    u32 remaining = MQTT_PACKET_ID_SIZE + count;
    u32 hdr_len   = 1 + mqtt_encode_remaining_len(buf + 1, remaining);

    // Variable header: packet ID
    buf[hdr_len]     = (u8)(packet_id >> MQTT_U16_HIGH_SHIFT);
    buf[hdr_len + 1] = (u8)(packet_id & MQTT_U16_LOW_MASK);

    // Payload: return codes
    for (u16 i = 0; i < count; i++) {
        buf[hdr_len + MQTT_PACKET_ID_SIZE + i] = return_codes[i];
    }

    return hdr_len + MQTT_PACKET_ID_SIZE + count;
}

// =============================================================================
// UNSUBSCRIBE Packet Parsing
// =============================================================================

struct mqtt_unsubscribe {
    u16 packet_id;
    u16 filter_count;

    // Topic filters (pointers into receive buffer)
    struct mqtt_str filters[MQTT_MAX_FILTERS];
};

// Parse UNSUBSCRIBE packet
INLINE i32 mqtt_parse_unsubscribe(const u8 *buf, u32 len, struct mqtt_unsubscribe *unsub) {
    if (len < 2) {
        return MQTT_INCOMPLETE;
    }

    unsub->packet_id = mqtt_read_u16(buf);
    u32 pos          = 2;

    unsub->filter_count = 0;

    while (pos < len && unsub->filter_count < MQTT_MAX_FILTERS) {
        i32 rc = mqtt_parse_string(buf + pos, len - pos, &unsub->filters[unsub->filter_count]);
        if (rc < 0) {
            return rc;
        }
        pos += (u32)rc;
        unsub->filter_count++;
    }

    if (unsub->filter_count == 0) {
        return MQTT_ERR_MALFORMED;
    }

    return MQTT_OK;
}

// =============================================================================
// UNSUBACK Encoding
// =============================================================================

// UNSUBACK is always 4 bytes (like CONNACK)
#define MQTT_UNSUBACK_SIZE 4

INLINE u32 mqtt_encode_unsuback(u8 *buf, u16 packet_id) {
    buf[0] = MQTT_UNSUBACK << MQTT_TYPE_SHIFT;
    buf[1] = MQTT_PACKET_ID_SIZE; // Remaining length
    buf[2] = (u8)(packet_id >> MQTT_U16_HIGH_SHIFT);
    buf[3] = (u8)(packet_id & MQTT_U16_LOW_MASK);
    return MQTT_UNSUBACK_SIZE;
}

#endif // MQTT_SUBSCRIBE_H
