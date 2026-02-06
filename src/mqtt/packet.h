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
// Protocol Constants
// =============================================================================

// Fixed header masks
#define MQTT_TYPE_SHIFT 4    // Packet type in upper nibble
#define MQTT_FLAGS_MASK 0x0F // Flags in lower nibble

// Variable Byte Integer (VBI) encoding
#define MQTT_VBI_CONTINUE  0x80 // MSB set = more bytes follow
#define MQTT_VBI_MASK      0x7F // Lower 7 bits = value
#define MQTT_VBI_SHIFT     7    // Bits per VBI byte
#define MQTT_VBI_MAX_BYTES 4    // Max VBI length

// VBI max values per byte count (for size calculation)
#define MQTT_VBI_1BYTE_MAX 127     // 2^7 - 1
#define MQTT_VBI_2BYTE_MAX 16383   // 2^14 - 1
#define MQTT_VBI_3BYTE_MAX 2097151 // 2^21 - 1

// Big-endian u16 encoding
#define MQTT_U16_HIGH_SHIFT 8    // Shift for high byte
#define MQTT_U16_LOW_MASK   0xFF // Mask for low byte

// Packet ID constants
#define MQTT_PACKET_ID_SIZE 2 // Packet ID is always 2 bytes

// String encoding
#define MQTT_STR_LEN_BYTES 2 // 2-byte big-endian length prefix
#define MQTT_STR_LEN_SHIFT 8 // Shift for high byte

// =============================================================================
// Zero-Copy String Slice
// =============================================================================

// Points into receive buffer - no allocation, no copy
struct mqtt_str {
    const u8 *ptr;
    u16 len;
};

// =============================================================================
// Topic Segment Iterator
// =============================================================================

// Iterates over '/'-delimited segments in a topic string
// Usage:
//   struct topic_iter it = topic_iter_init(topic, len);
//   struct mqtt_str seg;
//   while (topic_iter_next(&it, &seg)) {
//       // use seg.ptr, seg.len
//   }

struct topic_iter {
    const u8 *data;
    u16 len;
    u16 pos;
};

INLINE struct topic_iter topic_iter_init(const u8 *data, u16 len) {
    return (struct topic_iter){.data = data, .len = len, .pos = 0};
}

// Returns true if segment found, false if exhausted
INLINE bool topic_iter_next(struct topic_iter *it, struct mqtt_str *seg) {
    if (it->pos >= it->len) {
        return false;
    }

    u16 start = it->pos;
    while (it->pos < it->len && it->data[it->pos] != '/') {
        it->pos++;
    }

    seg->ptr = it->data + start;
    seg->len = it->pos - start;

    // Skip delimiter
    if (it->pos < it->len) {
        it->pos++;
    }

    return true;
}

// Extract segment at position, return next position (for recursive use)
INLINE u16 topic_extract_segment(const u8 *data, u16 len, u16 pos, struct mqtt_str *seg) {
    u16 start = pos;
    while (pos < len && data[pos] != '/') {
        pos++;
    }

    seg->ptr = data + start;
    seg->len = pos - start;

    // Return position after delimiter
    return (pos < len) ? pos + 1 : pos;
}

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
    if (unlikely(len < 2)) {
        return MQTT_INCOMPLETE;
    }

    hdr->type  = buf[0] >> MQTT_TYPE_SHIFT;
    hdr->flags = buf[0] & MQTT_FLAGS_MASK;

    // Parse variable byte integer (remaining length)
    u32 value      = 0;
    u32 multiplier = 1;
    u32 pos        = 1;

    for (;;) {
        if (unlikely(pos >= len)) {
            return MQTT_INCOMPLETE;
        }
        if (unlikely(pos > MQTT_VBI_MAX_BYTES)) {
            return MQTT_ERR_MALFORMED; // VBI too long
        }

        u8 byte = buf[pos++];
        value += (byte & MQTT_VBI_MASK) * multiplier;
        multiplier <<= MQTT_VBI_SHIFT;

        // Most packets have 1-2 byte remaining length
        if (likely((byte & MQTT_VBI_CONTINUE) == 0)) {
            break;
        }
    }

    hdr->remaining_len = value;
    hdr->header_len    = (u8)pos;

    return (i32)pos;
}

// Check if we have a complete packet, optionally returning parsed header
// If out_hdr is non-NULL, fills it with the parsed header (avoids re-parsing later)
INLINE i32 mqtt_packet_complete_ex(const u8 *buf, u32 len, struct mqtt_fixed_header *out_hdr) {
    struct mqtt_fixed_header hdr;
    i32 rc = mqtt_parse_fixed_header(buf, len, &hdr);
    if (unlikely(rc < 0)) {
        return rc;
    }

    u32 total = hdr.header_len + hdr.remaining_len;
    if (unlikely(len < total)) {
        return MQTT_INCOMPLETE;
    }

    if (out_hdr) {
        *out_hdr = hdr;
    }
    return (i32)total;
}

// Check if we have a complete packet (convenience wrapper)
INLINE i32 mqtt_packet_complete(const u8 *buf, u32 len) {
    return mqtt_packet_complete_ex(buf, len, NULL);
}

// =============================================================================
// String Parsing (length-prefixed UTF-8)
// =============================================================================

// Parse MQTT string: 2-byte big-endian length + data
// Returns: bytes consumed on success, or mqtt_parse_result on error
INLINE i32 mqtt_parse_string(const u8 *buf, u32 remaining, struct mqtt_str *str) {
    if (remaining < MQTT_STR_LEN_BYTES) {
        return MQTT_INCOMPLETE;
    }

    u16 slen = (u16)((buf[0] << MQTT_STR_LEN_SHIFT) | buf[1]);

    if (remaining < MQTT_STR_LEN_BYTES + slen) {
        return MQTT_INCOMPLETE;
    }

    str->ptr = buf + MQTT_STR_LEN_BYTES;
    str->len = slen;

    return MQTT_STR_LEN_BYTES + slen;
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
    return (u16)((buf[0] << MQTT_STR_LEN_SHIFT) | buf[1]);
}

// =============================================================================
// Encoding Helpers
// =============================================================================

// Encode remaining length as variable byte integer
// Returns: bytes written (1-4)
INLINE u32 mqtt_encode_remaining_len(u8 *buf, u32 len) {
    u32 pos = 0;
    do {
        u8 byte = len & MQTT_VBI_MASK;
        len >>= MQTT_VBI_SHIFT;
        if (len > 0) {
            byte |= MQTT_VBI_CONTINUE;
        }
        buf[pos++] = byte;
    } while (len > 0);
    return pos;
}

// Encode fixed header
// Returns: bytes written
INLINE u32 mqtt_encode_fixed_header(u8 *buf, u8 type, u8 flags, u32 remaining) {
    buf[0] = (u8)((type << MQTT_TYPE_SHIFT) | (flags & MQTT_FLAGS_MASK));
    return 1 + mqtt_encode_remaining_len(buf + 1, remaining);
}

#endif // MQTT_PACKET_H
