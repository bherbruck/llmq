// mqtt/packet.h - MQTT packet types and zero-copy parsing primitives
// All parsing returns pointers into receive buffers - no copying

#ifndef MQTT_PACKET_H
#define MQTT_PACKET_H

#include "sys/types.h"

// =============================================================================
// Packet Types (MQTT 3.1.1)
// =============================================================================

enum mqtt_packet_type {
    MQTT_CONNECT     = 1,
    MQTT_CONNACK     = 2,
    MQTT_PUBLISH     = 3,
    MQTT_PUBACK      = 4,
    MQTT_PUBREC      = 5,
    MQTT_PUBREL      = 6,
    MQTT_PUBCOMP     = 7,
    MQTT_SUBSCRIBE   = 8,
    MQTT_SUBACK      = 9,
    MQTT_UNSUBSCRIBE = 10,
    MQTT_UNSUBACK    = 11,
    MQTT_PINGREQ     = 12,
    MQTT_PINGRESP    = 13,
    MQTT_DISCONNECT  = 14,
};

// =============================================================================
// Parse Results
// =============================================================================

enum mqtt_parse_result {
    MQTT_OK            = 0,
    MQTT_INCOMPLETE    = -1, // Need more data
    MQTT_ERR_MALFORMED = -2, // Protocol violation
    MQTT_ERR_PROTOCOL  = -3, // Wrong protocol name/version
    MQTT_ERR_TOO_LARGE = -4, // Packet exceeds max size
};

// =============================================================================
// Zero-Copy String Slice
// =============================================================================

// Points into receive buffer - no allocation, no copy
struct mqtt_str {
    const u8 *ptr;
    u16 len;
};

// =============================================================================
// Fixed Header
// =============================================================================

struct mqtt_fixed_header {
    u8 type;           // Packet type (upper 4 bits)
    u8 flags;          // Type-specific flags (lower 4 bits)
    u32 remaining_len; // Variable byte integer (0 to 268,435,455)
    u8 header_len;     // Total fixed header size (2-5 bytes)
};

// Parse fixed header from buffer
// Returns: header_len on success, or mqtt_parse_result on error
INLINE i32 mqtt_parse_fixed_header(const u8 *buf, u32 len, struct mqtt_fixed_header *hdr) {
    if (len < 2) {
        return MQTT_INCOMPLETE;
    }

    hdr->type  = buf[0] >> 4;
    hdr->flags = buf[0] & 0x0F;

    // Parse variable byte integer (remaining length)
    u32 value      = 0;
    u32 multiplier = 1;
    u32 pos        = 1;

    for (;;) {
        if (pos >= len) {
            return MQTT_INCOMPLETE;
        }
        if (pos > 4) {
            return MQTT_ERR_MALFORMED; // VBI too long
        }

        u8 byte = buf[pos++];
        value += (byte & 0x7F) * multiplier;
        multiplier *= 128;

        if ((byte & 0x80) == 0) {
            break;
        }
    }

    hdr->remaining_len = value;
    hdr->header_len    = (u8)pos;

    return (i32)pos;
}

// Check if we have a complete packet
INLINE i32 mqtt_packet_complete(const u8 *buf, u32 len) {
    struct mqtt_fixed_header hdr;
    i32 rc = mqtt_parse_fixed_header(buf, len, &hdr);
    if (rc < 0) {
        return rc;
    }

    u32 total = hdr.header_len + hdr.remaining_len;
    if (len < total) {
        return MQTT_INCOMPLETE;
    }

    return (i32)total;
}

// =============================================================================
// String Parsing (length-prefixed UTF-8)
// =============================================================================

// Parse MQTT string: 2-byte big-endian length + data
// Returns: bytes consumed on success, or mqtt_parse_result on error
INLINE i32 mqtt_parse_string(const u8 *buf, u32 remaining, struct mqtt_str *str) {
    if (remaining < 2) {
        return MQTT_INCOMPLETE;
    }

    u16 slen = ((u16)buf[0] << 8) | buf[1];

    if (remaining < 2u + slen) {
        return MQTT_INCOMPLETE;
    }

    str->ptr = buf + 2;
    str->len = slen;

    return 2 + slen;
}

// =============================================================================
// Binary Data Parsing (length-prefixed)
// =============================================================================

// Same as string but for binary data (passwords, will payload)
INLINE i32 mqtt_parse_binary(const u8 *buf, u32 remaining, struct mqtt_str *data) {
    return mqtt_parse_string(buf, remaining, data);
}

// =============================================================================
// Integer Parsing
// =============================================================================

INLINE u16 mqtt_read_u16(const u8 *buf) {
    return ((u16)buf[0] << 8) | buf[1];
}

// =============================================================================
// Encoding Helpers
// =============================================================================

// Encode remaining length as variable byte integer
// Returns: bytes written (1-4)
INLINE u32 mqtt_encode_remaining_len(u8 *buf, u32 len) {
    u32 pos = 0;
    do {
        u8 byte = len & 0x7F;
        len >>= 7;
        if (len > 0) {
            byte |= 0x80;
        }
        buf[pos++] = byte;
    } while (len > 0);
    return pos;
}

// Encode fixed header
// Returns: bytes written
INLINE u32 mqtt_encode_fixed_header(u8 *buf, u8 type, u8 flags, u32 remaining) {
    buf[0] = (type << 4) | (flags & 0x0F);
    return 1 + mqtt_encode_remaining_len(buf + 1, remaining);
}

#endif // MQTT_PACKET_H
