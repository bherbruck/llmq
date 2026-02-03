// mqtt/connect.h - CONNECT packet parsing and CONNACK encoding
// Zero-copy: all strings point into receive buffer

#ifndef MQTT_CONNECT_H
#define MQTT_CONNECT_H

#include "mqtt/packet.h"

// =============================================================================
// Connect Flags
// =============================================================================

#define MQTT_CONNECT_FLAG_CLEAN_SESSION  0x02
#define MQTT_CONNECT_FLAG_WILL           0x04
#define MQTT_CONNECT_FLAG_WILL_QOS_MASK  0x18
#define MQTT_CONNECT_FLAG_WILL_QOS_SHIFT 3
#define MQTT_CONNECT_FLAG_WILL_RETAIN    0x20
#define MQTT_CONNECT_FLAG_PASSWORD       0x40
#define MQTT_CONNECT_FLAG_USERNAME       0x80

// =============================================================================
// CONNACK Reason Codes (MQTT 3.1.1)
// =============================================================================

enum mqtt_connack_code {
    MQTT_CONNACK_ACCEPTED        = 0x00,
    MQTT_CONNACK_PROTO_VERSION   = 0x01, // Unacceptable protocol version
    MQTT_CONNACK_ID_REJECTED     = 0x02, // Client ID rejected
    MQTT_CONNACK_SERVER_UNAVAIL  = 0x03, // Server unavailable
    MQTT_CONNACK_BAD_CREDENTIALS = 0x04, // Bad username/password
    MQTT_CONNACK_NOT_AUTHORIZED  = 0x05, // Not authorized
};

// =============================================================================
// Parsed CONNECT Packet
// =============================================================================

struct mqtt_connect {
    // Protocol info
    u8 protocol_version; // 4 = MQTT 3.1.1

    // Flags (unpacked for convenience)
    bool clean_session;
    bool has_will;
    u8 will_qos;
    bool will_retain;
    bool has_username;
    bool has_password;

    // Keepalive in seconds (0 = disabled)
    u16 keepalive;

    // Payload strings (point into receive buffer)
    struct mqtt_str client_id;
    struct mqtt_str will_topic;
    struct mqtt_str will_payload;
    struct mqtt_str username;
    struct mqtt_str password;
};

// =============================================================================
// CONNECT Parsing
// =============================================================================

// Parse CONNECT packet after fixed header
// buf points to start of variable header (after fixed header)
// Returns: MQTT_OK or error code
INLINE i32 mqtt_parse_connect(const u8 *buf, u32 len, struct mqtt_connect *conn) {
    u32 pos = 0;

    // --- Variable Header ---

    // Protocol name length (must be 4 for "MQTT")
    if (len < pos + 2) {
        return MQTT_INCOMPLETE;
    }
    u16 proto_len = mqtt_read_u16(buf + pos);
    pos += 2;

    if (proto_len != 4) {
        return MQTT_ERR_PROTOCOL;
    }

    // Protocol name "MQTT"
    if (len < pos + 4) {
        return MQTT_INCOMPLETE;
    }
    if (buf[pos] != 'M' || buf[pos + 1] != 'Q' || buf[pos + 2] != 'T' || buf[pos + 3] != 'T') {
        return MQTT_ERR_PROTOCOL;
    }
    pos += 4;

    // Protocol version
    if (len < pos + 1) {
        return MQTT_INCOMPLETE;
    }
    conn->protocol_version = buf[pos++];
    if (conn->protocol_version != 4) {
        return MQTT_ERR_PROTOCOL; // Only 3.1.1 for now
    }

    // Connect flags
    if (len < pos + 1) {
        return MQTT_INCOMPLETE;
    }
    u8 flags = buf[pos++];

    // Reserved bit must be 0
    if (flags & 0x01) {
        return MQTT_ERR_MALFORMED;
    }

    conn->clean_session = (flags & MQTT_CONNECT_FLAG_CLEAN_SESSION) != 0;
    conn->has_will      = (flags & MQTT_CONNECT_FLAG_WILL) != 0;
    conn->will_qos = (flags & MQTT_CONNECT_FLAG_WILL_QOS_MASK) >> MQTT_CONNECT_FLAG_WILL_QOS_SHIFT;
    conn->will_retain  = (flags & MQTT_CONNECT_FLAG_WILL_RETAIN) != 0;
    conn->has_username = (flags & MQTT_CONNECT_FLAG_USERNAME) != 0;
    conn->has_password = (flags & MQTT_CONNECT_FLAG_PASSWORD) != 0;

    // Validate will QoS
    if (conn->will_qos > 2) {
        return MQTT_ERR_MALFORMED;
    }

    // Will flags without will flag is invalid
    if (!conn->has_will && (conn->will_qos != 0 || conn->will_retain)) {
        return MQTT_ERR_MALFORMED;
    }

    // Password without username is invalid
    if (conn->has_password && !conn->has_username) {
        return MQTT_ERR_MALFORMED;
    }

    // Keepalive
    if (len < pos + 2) {
        return MQTT_INCOMPLETE;
    }
    conn->keepalive = mqtt_read_u16(buf + pos);
    pos += 2;

    // --- Payload ---

    // Client ID (required)
    i32 rc = mqtt_parse_string(buf + pos, len - pos, &conn->client_id);
    if (rc < 0) {
        return rc;
    }
    pos += (u32)rc;

    // Will topic and message (if has_will)
    if (conn->has_will) {
        rc = mqtt_parse_string(buf + pos, len - pos, &conn->will_topic);
        if (rc < 0) {
            return rc;
        }
        pos += (u32)rc;

        rc = mqtt_parse_binary(buf + pos, len - pos, &conn->will_payload);
        if (rc < 0) {
            return rc;
        }
        pos += (u32)rc;
    } else {
        conn->will_topic.ptr   = NULL;
        conn->will_topic.len   = 0;
        conn->will_payload.ptr = NULL;
        conn->will_payload.len = 0;
    }

    // Username (if has_username)
    if (conn->has_username) {
        rc = mqtt_parse_string(buf + pos, len - pos, &conn->username);
        if (rc < 0) {
            return rc;
        }
        pos += (u32)rc;
    } else {
        conn->username.ptr = NULL;
        conn->username.len = 0;
    }

    // Password (if has_password)
    if (conn->has_password) {
        rc = mqtt_parse_binary(buf + pos, len - pos, &conn->password);
        if (rc < 0) {
            return rc;
        }
        // pos not updated - password is the last field
    } else {
        conn->password.ptr = NULL;
        conn->password.len = 0;
    }

    (void)pos; // Silence unused warning
    return MQTT_OK;
}

// =============================================================================
// CONNACK Encoding
// =============================================================================

// CONNACK is always 4 bytes
#define MQTT_CONNACK_SIZE 4

// Encode CONNACK response
// Returns: bytes written (always 4)
INLINE u32 mqtt_encode_connack(u8 *buf, bool session_present, u8 reason_code) {
    buf[0] = MQTT_CONNACK << 4; // Fixed header: type + flags (0)
    buf[1] = 2;                 // Remaining length: always 2
    buf[2] = session_present ? 1 : 0;
    buf[3] = reason_code;
    return MQTT_CONNACK_SIZE;
}

#endif // MQTT_CONNECT_H
