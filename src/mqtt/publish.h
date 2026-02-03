// mqtt/publish.h - PUBLISH/PUBACK packet handling
// Zero-copy: topic and payload point into receive buffer

#ifndef MQTT_PUBLISH_H
#define MQTT_PUBLISH_H

#include "mqtt/packet.h"

// =============================================================================
// PUBLISH Flags (from fixed header lower nibble)
// =============================================================================

#define MQTT_PUBLISH_FLAG_RETAIN    0x01
#define MQTT_PUBLISH_FLAG_QOS_MASK  0x06
#define MQTT_PUBLISH_FLAG_QOS_SHIFT 1
#define MQTT_PUBLISH_FLAG_DUP       0x08

// =============================================================================
// Parsed PUBLISH Packet
// =============================================================================

struct mqtt_publish {
    // From fixed header flags
    bool retain;
    u8 qos;
    bool dup;

    // Variable header
    struct mqtt_str topic;
    u16 packet_id; // Only present for QoS > 0

    // Payload (points into receive buffer)
    const u8 *payload;
    u32 payload_len;
};

// =============================================================================
// PUBLISH Parsing
// =============================================================================

// Parse PUBLISH packet (variable header + payload)
// flags: lower 4 bits from fixed header
// buf: points to variable header (after fixed header)
// len: remaining_len from fixed header
// Returns: MQTT_OK or error
INLINE i32 mqtt_parse_publish(u8 flags, const u8 *buf, u32 len, struct mqtt_publish *pub) {
    // Extract flags
    pub->retain = (flags & MQTT_PUBLISH_FLAG_RETAIN) != 0;
    pub->qos    = (flags & MQTT_PUBLISH_FLAG_QOS_MASK) >> MQTT_PUBLISH_FLAG_QOS_SHIFT;
    pub->dup    = (flags & MQTT_PUBLISH_FLAG_DUP) != 0;

    // Validate QoS
    if (pub->qos > 2) {
        return MQTT_ERR_MALFORMED;
    }

    // DUP must be 0 for QoS 0
    if (pub->qos == 0 && pub->dup) {
        return MQTT_ERR_MALFORMED;
    }

    u32 pos = 0;

    // Topic name
    i32 rc = mqtt_parse_string(buf, len, &pub->topic);
    if (rc < 0) {
        return rc;
    }
    pos += (u32)rc;

    // Topic must not be empty
    if (pub->topic.len == 0) {
        return MQTT_ERR_MALFORMED;
    }

    // Packet ID (only for QoS 1 and 2)
    if (pub->qos > 0) {
        if (len < pos + 2) {
            return MQTT_INCOMPLETE;
        }
        pub->packet_id = mqtt_read_u16(buf + pos);
        pos += 2;

        // Packet ID must not be 0
        if (pub->packet_id == 0) {
            return MQTT_ERR_MALFORMED;
        }
    } else {
        pub->packet_id = 0;
    }

    // Payload is everything remaining
    pub->payload     = buf + pos;
    pub->payload_len = len - pos;

    return MQTT_OK;
}

// =============================================================================
// PUBLISH Encoding (for fan-out to subscribers)
// =============================================================================

// Calculate encoded PUBLISH size
INLINE u32 mqtt_publish_size(u16 topic_len, u32 payload_len, u8 qos) {
    // Fixed header (1) + remaining length (1-4) + topic (2 + len) + packet_id (0 or 2) + payload
    u32 remaining = 2 + topic_len + payload_len;
    if (qos > 0) {
        remaining += 2;
    }

    // Calculate variable byte integer size
    u32 vbi_size = 1;
    if (remaining > MQTT_VBI_1BYTE_MAX)
        vbi_size = 2;
    if (remaining > MQTT_VBI_2BYTE_MAX)
        vbi_size = 3;
    if (remaining > MQTT_VBI_3BYTE_MAX)
        vbi_size = 4;

    return 1 + vbi_size + remaining;
}

// Encode PUBLISH packet
// Returns: bytes written
INLINE u32 mqtt_encode_publish(u8 *buf, const struct mqtt_publish *pub, u16 packet_id_override) {
    // Fixed header flags
    u8 flags = 0;
    if (pub->retain)
        flags |= MQTT_PUBLISH_FLAG_RETAIN;
    flags |= (pub->qos << MQTT_PUBLISH_FLAG_QOS_SHIFT) & MQTT_PUBLISH_FLAG_QOS_MASK;
    if (pub->dup)
        flags |= MQTT_PUBLISH_FLAG_DUP;

    // Remaining length
    u32 remaining = 2 + pub->topic.len + pub->payload_len;
    if (pub->qos > 0) {
        remaining += 2;
    }

    // Fixed header
    u32 pos = mqtt_encode_fixed_header(buf, MQTT_PUBLISH, flags, remaining);

    // Topic
    buf[pos++] = (u8)(pub->topic.len >> MQTT_U16_HIGH_SHIFT);
    buf[pos++] = (u8)(pub->topic.len & MQTT_U16_LOW_MASK);
    for (u16 i = 0; i < pub->topic.len; i++) {
        buf[pos++] = pub->topic.ptr[i];
    }

    // Packet ID (if QoS > 0)
    if (pub->qos > 0) {
        u16 pid    = packet_id_override ? packet_id_override : pub->packet_id;
        buf[pos++] = (u8)(pid >> MQTT_U16_HIGH_SHIFT);
        buf[pos++] = (u8)(pid & MQTT_U16_LOW_MASK);
    }

    // Payload
    for (u32 i = 0; i < pub->payload_len; i++) {
        buf[pos++] = pub->payload[i];
    }

    return pos;
}

// =============================================================================
// PUBLISH Scatter-Gather Encoding (for QoS 1/2 zero-copy fan-out)
// =============================================================================

// Encode PUBLISH packet in scatter-gather format for QoS 1/2
// Layout: [header][remaining_len][topic_len][topic][payload]
// The packet_id will be inserted via writev between topic and payload
// Returns: topic_end offset (where to split for iovec)
// out_total is set to total length (header + topic + payload, WITHOUT packet_id)
INLINE u32 mqtt_encode_publish_scatter(u8 *buf, const struct mqtt_publish *pub, u32 *out_total,
                                       u32 *out_payload_len) {
    // Fixed header flags - preserve actual QoS for correct subscriber behavior
    u8 flags = 0;
    if (pub->retain)
        flags |= MQTT_PUBLISH_FLAG_RETAIN;
    flags |= (pub->qos << MQTT_PUBLISH_FLAG_QOS_SHIFT) & MQTT_PUBLISH_FLAG_QOS_MASK;
    if (pub->dup)
        flags |= MQTT_PUBLISH_FLAG_DUP;

    // Remaining length INCLUDES packet_id (2 bytes) even though we don't encode it here
    u32 remaining = 2 + pub->topic.len + 2 + pub->payload_len;

    // Fixed header
    u32 pos = mqtt_encode_fixed_header(buf, MQTT_PUBLISH, flags, remaining);

    // Topic
    buf[pos++] = (u8)(pub->topic.len >> MQTT_U16_HIGH_SHIFT);
    buf[pos++] = (u8)(pub->topic.len & MQTT_U16_LOW_MASK);
    for (u16 i = 0; i < pub->topic.len; i++) {
        buf[pos++] = pub->topic.ptr[i];
    }

    // Record topic_end (this is where packet_id would be inserted)
    u32 topic_end = pos;

    // Payload (follows directly, packet_id will be inserted via writev)
    for (u32 i = 0; i < pub->payload_len; i++) {
        buf[pos++] = pub->payload[i];
    }

    *out_total       = pos;
    *out_payload_len = pub->payload_len;
    return topic_end;
}

// =============================================================================
// PUBACK Encoding (QoS 1 acknowledgment)
// =============================================================================

#define MQTT_PUBACK_SIZE 4

INLINE u32 mqtt_encode_puback(u8 *buf, u16 packet_id) {
    buf[0] = MQTT_PUBACK << MQTT_TYPE_SHIFT;
    buf[1] = MQTT_PACKET_ID_SIZE; // Remaining length
    buf[2] = (u8)(packet_id >> MQTT_U16_HIGH_SHIFT);
    buf[3] = (u8)(packet_id & MQTT_U16_LOW_MASK);
    return MQTT_PUBACK_SIZE;
}

// =============================================================================
// PUBREC/PUBREL/PUBCOMP (QoS 2 - minimal support)
// =============================================================================

#define MQTT_PUBREC_SIZE  4
#define MQTT_PUBREL_SIZE  4
#define MQTT_PUBCOMP_SIZE 4

INLINE u32 mqtt_encode_pubrec(u8 *buf, u16 packet_id) {
    buf[0] = MQTT_PUBREC << MQTT_TYPE_SHIFT;
    buf[1] = MQTT_PACKET_ID_SIZE;
    buf[2] = (u8)(packet_id >> MQTT_U16_HIGH_SHIFT);
    buf[3] = (u8)(packet_id & MQTT_U16_LOW_MASK);
    return MQTT_PUBREC_SIZE;
}

// PUBREL reserved flags per MQTT 3.1.1 spec
#define MQTT_PUBREL_FLAGS 0x02

INLINE u32 mqtt_encode_pubrel(u8 *buf, u16 packet_id) {
    buf[0] = (MQTT_PUBREL << MQTT_TYPE_SHIFT) | MQTT_PUBREL_FLAGS;
    buf[1] = MQTT_PACKET_ID_SIZE;
    buf[2] = (u8)(packet_id >> MQTT_U16_HIGH_SHIFT);
    buf[3] = (u8)(packet_id & MQTT_U16_LOW_MASK);
    return MQTT_PUBREL_SIZE;
}

INLINE u32 mqtt_encode_pubcomp(u8 *buf, u16 packet_id) {
    buf[0] = MQTT_PUBCOMP << MQTT_TYPE_SHIFT;
    buf[1] = MQTT_PACKET_ID_SIZE;
    buf[2] = (u8)(packet_id >> MQTT_U16_HIGH_SHIFT);
    buf[3] = (u8)(packet_id & MQTT_U16_LOW_MASK);
    return MQTT_PUBCOMP_SIZE;
}

#endif // MQTT_PUBLISH_H
