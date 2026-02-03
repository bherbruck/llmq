# Section 7: MQTT Protocol Handling

## 7.1 Overview

The broker targets **MQTT 3.1.1** (protocol level 4) as the primary implementation. MQTT 5.0 (protocol level 5) support is optional and may be added later. All parsing is performed in-place on receive buffers without copying packet data.

## 7.2 Supported Features

### 7.2.1 MQTT 3.1.1 Features (Primary Target)

| Feature | Support | Notes |
|---------|---------|-------|
| QoS 0 | REQUIRED | At most once delivery |
| QoS 1 | REQUIRED | At least once delivery |
| QoS 2 | OPTIONAL | Exactly once delivery (may defer) |
| Retained messages | REQUIRED | Stored per-topic |
| Will messages | REQUIRED | Published immediately on ungraceful disconnect |
| Clean session | REQUIRED | Session state management |
| Keepalive | REQUIRED | Connection health monitoring |

### 7.2.2 MQTT 5.0 Features (Future/Optional)

| Feature | Support | Notes |
|---------|---------|-------|
| All 3.1.1 features | REQUIRED | Backwards compatible |
| Session expiry | OPTIONAL | Configurable per-client |
| Will delay | OPTIONAL | Delay will message publication |
| Message expiry | OPTIONAL | TTL on messages |
| Topic aliases | OPTIONAL | Reduces bandwidth |
| User properties | OPTIONAL | Key-value metadata |
| Shared subscriptions | OPTIONAL | Load balancing |
| Request/response | OPTIONAL | Correlation data |
| Server DISCONNECT | OPTIONAL | Reason codes before close |

## 7.3 Packet Parsing

### 7.3.1 Fixed Header

All MQTT packets begin with a fixed header:

```
Bit     7   6   5   4   3   2   1   0
       ├───────────────┼───────────────┤
Byte 1 │  Packet Type  │    Flags      │
       ├───────────────────────────────┤
Byte 2+│     Remaining Length (VBI)    │
       └───────────────────────────────┘
```

Parsing:

```c
struct mqtt_header {
    u8  packet_type;             // Upper 4 bits of byte 0
    u8  flags;                   // Lower 4 bits of byte 0
    u32 remaining_length;        // Variable byte integer
    u8  header_len;              // Total fixed header length (1-5)
};

// Returns header length or -1 if incomplete
int mqtt_parse_header(const u8 *buf, u32 len, struct mqtt_header *hdr) {
    if (len < 2) return -1;
    
    hdr->packet_type = buf[0] >> 4;
    hdr->flags = buf[0] & 0x0F;
    
    // Parse variable byte integer
    u32 multiplier = 1;
    u32 value = 0;
    u32 pos = 1;
    
    do {
        if (pos >= len) return -1;
        if (pos > 4) return -2;  // Malformed
        
        u8 byte = buf[pos++];
        value += (byte & 0x7F) * multiplier;
        multiplier *= 128;
        
        if ((byte & 0x80) == 0) break;
    } while (1);
    
    hdr->remaining_length = value;
    hdr->header_len = pos;
    
    return pos;
}
```

### 7.3.2 Packet Types

```c
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
    MQTT_AUTH        = 15,        // MQTT 5.0 only
};
```

### 7.3.3 String Parsing

MQTT strings are length-prefixed:

```c
struct mqtt_string {
    const u8 *data;              // Pointer into buffer (no copy)
    u16 len;
};

// Returns bytes consumed or -1 on error
int mqtt_parse_string(const u8 *buf, u32 remaining, struct mqtt_string *str) {
    if (remaining < 2) return -1;
    
    u16 len = ((u16)buf[0] << 8) | buf[1];
    
    if (remaining < 2 + len) return -1;
    
    str->data = buf + 2;
    str->len = len;
    
    return 2 + len;
}
```

## 7.4 CONNECT Handling

### 7.4.1 CONNECT Packet Structure

```c
struct mqtt_connect {
    u8  protocol_version;        // 4 = 3.1.1, 5 = 5.0
    u8  connect_flags;
    u16 keepalive;
    
    struct mqtt_string client_id;
    struct mqtt_string will_topic;
    struct mqtt_string will_message;
    struct mqtt_string username;
    struct mqtt_string password;
    
    // MQTT 5.0 properties
    u32 session_expiry;
    u16 receive_max;
    u32 max_packet_size;
    u16 topic_alias_max;
};
```

### 7.4.2 Connect Flags

```
Bit   7        6        5        4   3      2          1         0
    ┌────────┬────────┬────────┬────────┬──────────┬─────────────┐
    │Username│Password│Will    │Will QoS│Will Flag │Clean Session│
    │  Flag  │  Flag  │Retain  │        │          │   / Start   │
    └────────┴────────┴────────┴────────┴──────────┴─────────────┘
```

### 7.4.3 Validation Rules

The broker MUST validate:

1. Protocol name is "MQTT" (or "MQIsdp" for 3.1 legacy)
2. Protocol version is 4 or 5
3. Reserved flag bit 0 is zero
4. Client ID is valid UTF-8 and within length limits
5. If will flag set, will QoS is 0, 1, or 2
6. Username present if password present

### 7.4.4 CONNACK Response

```c
void mqtt_encode_connack(u8 *buf, u8 session_present, u8 reason_code) {
    buf[0] = MQTT_CONNACK << 4;  // Fixed header
    buf[1] = 2;                   // Remaining length
    buf[2] = session_present & 0x01;
    buf[3] = reason_code;
}
```

## 7.5 PUBLISH Handling

### 7.5.1 PUBLISH Packet Structure

The PUBLISH packet is parsed **surgically** - we extract offsets/pointers into the receive buffer rather than copying data. This enables zero-copy fan-out where the same buffer serves all subscribers.

```
┌──────────────────────────────────────────────────────────────────┐
│                      RECEIVE BUFFER                               │
├──────────┬──────────────┬───────────┬───────────────────────────┤
│  Fixed   │   Topic      │ Packet ID │         Payload            │
│  Header  │ (2B len+str) │ (QoS>0)   │   (rest of packet)         │
├──────────┼──────────────┼───────────┼───────────────────────────┤
│ offset 0 │   hdr_len    │  + 2 +    │   + 2 (if QoS>0)          │
│          │              │ topic_len │                            │
└──────────┴──────────────┴───────────┴───────────────────────────┘
      │            │             │              │
      │            ▼             │              ▼
      │      pub->topic ─────────┘        pub->payload
      │      (pointer)                    (pointer)
      ▼
 pub->packet_offset = 0
 pub->packet_len = total
```

The parsed struct contains **pointers** into the buffer, not copies:

```c
struct mqtt_publish {
    // Parsed from fixed header
    u8  qos;
    u8  retain;
    u8  dup;
    
    // Pointers into receive buffer (zero-copy)
    const u8 *topic;
    u16 topic_len;
    
    u16 packet_id;               // Only for QoS > 0
    
    const u8 *payload;
    u32 payload_len;
    
    // For fan-out: offset from buffer start
    u32 packet_offset;
    u32 packet_len;
};

int mqtt_parse_publish(const u8 *buf, u32 len, struct mqtt_publish *pub) {
    struct mqtt_header hdr;
    int hdr_len = mqtt_parse_header(buf, len, &hdr);
    if (hdr_len < 0) return hdr_len;
    
    pub->qos = (hdr.flags >> 1) & 0x03;
    pub->retain = hdr.flags & 0x01;
    pub->dup = (hdr.flags >> 3) & 0x01;
    
    u32 pos = hdr_len;
    
    // Topic
    if (len - pos < 2) return -1;
    pub->topic_len = ((u16)buf[pos] << 8) | buf[pos + 1];
    pos += 2;
    
    if (len - pos < pub->topic_len) return -1;
    pub->topic = buf + pos;
    pos += pub->topic_len;
    
    // Packet ID (QoS > 0)
    if (pub->qos > 0) {
        if (len - pos < 2) return -1;
        pub->packet_id = ((u16)buf[pos] << 8) | buf[pos + 1];
        pos += 2;
    }
    
    // Payload (rest of packet)
    pub->payload = buf + pos;
    pub->payload_len = hdr_len + hdr.remaining_length - pos;
    
    // Store full packet location for fan-out
    pub->packet_offset = 0;
    pub->packet_len = hdr_len + hdr.remaining_length;

    return 0;
}
```

**Important:** The `mqtt_publish` struct contains pointers into the receive buffer. These pointers are only valid while the buffer is alive (refcount > 0). After fan-out completes and the buffer is freed, the `mqtt_publish` struct MUST NOT be dereferenced.

For QoS 0, this is straightforward - parse, fan-out, done.

For QoS 1, we extract the parts we need (topic offset, payload offset) into a `mqtt_publish_parts` struct that stores **offsets** rather than pointers. Offsets remain valid as long as we track which buffer they refer to. See [Section 9.6.2: QoS 1 Scatter-Gather](./09-fanout.md#962-qos-1-at-least-once).

```c
// Safe for storage (offsets, not pointers)
struct mqtt_publish_parts {
    u32 topic_offset;
    u16 topic_len;
    u32 payload_offset;
    u32 payload_len;
    u32 remaining_length;
    u8  type_flags;
};
```

### 7.5.2 Topic Validation

Topics MUST be validated:

```c
int mqtt_validate_topic(const u8 *topic, u16 len) {
    if (len == 0) return -1;
    
    // Check for wildcards (not allowed in PUBLISH)
    for (u16 i = 0; i < len; i++) {
        if (topic[i] == '+' || topic[i] == '#') {
            return -1;
        }
        if (topic[i] == 0) {  // Null character
            return -1;
        }
    }
    
    return 0;
}
```

### 7.5.3 QoS Handling

| QoS | Publisher Flow | Subscriber Flow |
|-----|----------------|-----------------|
| 0 | PUBLISH → | → PUBLISH |
| 1 | PUBLISH → PUBACK | PUBLISH → PUBACK |
| 2 | PUBLISH → PUBREC → PUBREL → PUBCOMP | PUBLISH → PUBREC → PUBREL → PUBCOMP |

For QoS 1/2, the broker tracks packet IDs per connection to match acknowledgments.

### 7.5.4 QoS 1 State Machine

QoS 1 requires tracking inflight messages until acknowledged.

#### Broker as Receiver (Client → Broker)

```
                     ┌─────────────────┐
                     │                 │
   PUBLISH received  │      IDLE       │
  ┌─────────────────►│                 │
  │                  └────────┬────────┘
  │                           │
  │                           │ Store packet ID
  │                           │ Deliver to subscribers
  │                           │ Send PUBACK
  │                           ▼
  │                  ┌─────────────────┐
  │                  │                 │
  └──────────────────│   COMPLETED    │
      (immediate)    │                 │
                     └─────────────────┘
```

The broker processes QoS 1 PUBLISHes synchronously - deliver then PUBACK. No state retained.

#### Broker as Sender (Broker → Client)

When delivering to a QoS 1 subscriber:

```
                     ┌─────────────────┐
                     │                 │
                     │      IDLE       │
                     │                 │
                     └────────┬────────┘
                              │
                              │ Allocate packet ID
                              │ Store inflight entry
                              │ Send PUBLISH
                              ▼
                     ┌─────────────────┐
    Timeout          │                 │
   ┌────────────────►│ WAIT_PUBACK     │◄───────────────┐
   │  Resend with    │  (inflight)     │                │
   │  DUP=1          └────────┬────────┘   Duplicate    │
   │                          │            PUBACK       │
   │                          │ PUBACK                  │
   │                          │ received                │
   │                          ▼                         │
   │                 ┌─────────────────┐                │
   │                 │                 │                │
   └─────────────────│   COMPLETED    │────────────────┘
                     │  Free packet ID │
                     └─────────────────┘
```

### 7.5.5 QoS 2 State Machine

QoS 2 requires a four-way handshake with explicit state tracking.

#### Broker as Receiver (Client → Broker)

```
                     ┌─────────────────┐
                     │                 │
   PUBLISH received  │      IDLE       │
        ┌───────────►│                 │
        │            └────────┬────────┘
        │                     │
        │                     │ Store packet ID + message
        │                     │ Send PUBREC
        │                     ▼
        │            ┌─────────────────┐
        │            │                 │
        │ Duplicate  │  WAIT_PUBREL    │◄───────────────┐
        │ PUBLISH    │                 │                │
        │ (resend    └────────┬────────┘   Timeout:     │
        │  PUBREC)            │            Resend       │
        │                     │ PUBREL     PUBREC       │
        └─────────────────────│ received                │
                              │                         │
                              │ Deliver to subscribers  │
                              │ Send PUBCOMP            │
                              │ Free packet ID          │
                              ▼                         │
                     ┌─────────────────┐                │
                     │                 │                │
                     │   COMPLETED    │────────────────┘
                     │                 │  Duplicate PUBREL
                     └─────────────────┘  (resend PUBCOMP)
```

#### Broker as Sender (Broker → Client)

```
                     ┌─────────────────┐
                     │                 │
                     │      IDLE       │
                     │                 │
                     └────────┬────────┘
                              │
                              │ Allocate packet ID
                              │ Store message
                              │ Send PUBLISH
                              ▼
                     ┌─────────────────┐
    Timeout:         │                 │
    Resend PUBLISH   │  WAIT_PUBREC    │◄───────────────┐
   ┌────────────────►│                 │                │
   │                 └────────┬────────┘                │
   │                          │                         │
   │                          │ PUBREC received         │
   │                          │ Discard message         │
   │                          │ Send PUBREL             │
   │                          ▼                         │
   │                 ┌─────────────────┐                │
   │   Timeout:      │                 │                │
   │   Resend PUBREL │  WAIT_PUBCOMP   │◄───┐           │
   │  ┌─────────────►│                 │    │           │
   │  │              └────────┬────────┘    │           │
   │  │                       │             │           │
   │  │                       │ PUBCOMP     │ Duplicate │
   │  │                       │ received    │ PUBREC    │
   │  │                       │             │ (resend   │
   │  │                       │ Free ID     │  PUBREL)  │
   │  │                       ▼             │           │
   │  │              ┌─────────────────┐    │           │
   │  │              │                 │    │           │
   │  └──────────────│   COMPLETED    │────┴───────────┘
   │                 │                 │
   └─────────────────└─────────────────┘
```

### 7.5.6 Inflight Message Tracking

Per-connection inflight state for QoS 1/2:

```c
enum inflight_state {
    INFLIGHT_FREE        = 0,
    INFLIGHT_WAIT_PUBACK = 1,    // QoS 1: awaiting PUBACK
    INFLIGHT_WAIT_PUBREC = 2,    // QoS 2: awaiting PUBREC
    INFLIGHT_WAIT_PUBREL = 3,    // QoS 2: awaiting PUBREL (receiver)
    INFLIGHT_WAIT_PUBCOMP = 4,   // QoS 2: awaiting PUBCOMP
};

struct inflight_msg {
    u16 packet_id;               // MQTT packet identifier
    u8  state;                   // enum inflight_state
    u8  qos;                     // Original QoS level
    u8  dup_count;               // Retransmission count
    u8  _pad[3];
    u32 buf_idx;                 // Buffer containing message (QoS 2)
    u32 timestamp;               // For timeout calculation
};

#define MAX_INFLIGHT_PER_CONN 16

struct conn_inflight {
    struct inflight_msg msgs[MAX_INFLIGHT_PER_CONN];
    u16 next_packet_id;          // Monotonic, wraps at 65535
    u8  count;                   // Active inflight messages
};
```

### 7.5.7 Packet ID Allocation

Packet IDs are allocated per-connection and MUST be unique among inflight messages:

```c
int alloc_packet_id(struct conn_inflight *inf, u16 *out_id) {
    if (inf->count >= MAX_INFLIGHT_PER_CONN) {
        return -1;  // Quota exceeded
    }

    // Find free slot and unused ID
    u16 start_id = inf->next_packet_id;
    do {
        inf->next_packet_id++;
        if (inf->next_packet_id == 0) inf->next_packet_id = 1;

        // Check if ID is in use
        int in_use = 0;
        for (int i = 0; i < MAX_INFLIGHT_PER_CONN; i++) {
            if (inf->msgs[i].state != INFLIGHT_FREE &&
                inf->msgs[i].packet_id == inf->next_packet_id) {
                in_use = 1;
                break;
            }
        }

        if (!in_use) {
            *out_id = inf->next_packet_id;
            return 0;
        }
    } while (inf->next_packet_id != start_id);

    return -1;  // All IDs in use (shouldn't happen with count check)
}
```

## 7.6 SUBSCRIBE Handling

### 7.6.1 SUBSCRIBE Packet Structure

```c
struct mqtt_subscription {
    struct mqtt_string topic_filter;
    u8 qos;
    // MQTT 5.0 options
    u8 no_local;
    u8 retain_as_published;
    u8 retain_handling;
};

struct mqtt_subscribe {
    u16 packet_id;
    u32 sub_count;
    struct mqtt_subscription subs[MAX_SUBS_PER_PACKET];
};
```

### 7.6.2 Topic Filter Validation

Topic filters may include wildcards:

```c
int mqtt_validate_topic_filter(const u8 *filter, u16 len) {
    if (len == 0) return -1;
    
    for (u16 i = 0; i < len; i++) {
        if (filter[i] == '+') {
            // Single-level wildcard must occupy entire level
            if ((i > 0 && filter[i-1] != '/') ||
                (i < len-1 && filter[i+1] != '/')) {
                return -1;
            }
        } else if (filter[i] == '#') {
            // Multi-level wildcard must be last character
            if (i != len - 1) return -1;
            // Must be at start or preceded by /
            if (i > 0 && filter[i-1] != '/') return -1;
        }
    }
    
    return 0;
}
```

### 7.6.3 SUBACK Response

```c
int mqtt_encode_suback(u8 *buf, u16 packet_id, const u8 *reason_codes, u32 count) {
    u32 pos = 0;
    
    // Fixed header
    buf[pos++] = MQTT_SUBACK << 4;
    
    // Remaining length (VBI) - 2 for packet ID + count for reason codes
    u32 remaining = 2 + count;
    pos += mqtt_encode_vbi(buf + pos, remaining);
    
    // Packet ID
    buf[pos++] = packet_id >> 8;
    buf[pos++] = packet_id & 0xFF;
    
    // Reason codes
    for (u32 i = 0; i < count; i++) {
        buf[pos++] = reason_codes[i];
    }
    
    return pos;
}
```

## 7.7 PINGREQ/PINGRESP

Minimal handling:

```c
void handle_pingreq(struct broker *b, struct conn_slot *slot) {
    static const u8 pingresp[2] = { MQTT_PINGRESP << 4, 0 };
    send_packet(b, slot, pingresp, 2);
}
```

## 7.8 DISCONNECT

### 7.8.1 Client-Initiated (MQTT 5.0)

```c
void handle_disconnect(struct broker *b, struct conn_slot *slot,
                       const u8 *buf, u32 len) {
    // Parse reason code (MQTT 5.0)
    u8 reason = 0;
    if (len > 2) {
        reason = buf[2];
    }
    
    // Normal disconnect - don't publish will message
    if (reason == 0) {
        slot->will_flag = 0;
    }
    
    conn_disconnect(b, slot);
}
```

### 7.8.2 Server-Initiated

The broker sends DISCONNECT before closing (MQTT 5.0 only):

```c
void send_disconnect(struct broker *b, struct conn_slot *slot, u8 reason) {
    if (slot->protocol_version < 5) {
        // MQTT 3.1.1 has no server-initiated DISCONNECT
        return;
    }
    
    u8 packet[4];
    packet[0] = MQTT_DISCONNECT << 4;
    packet[1] = 1;  // Remaining length
    packet[2] = reason;
    
    send_packet(b, slot, packet, 3);
}
```

## 7.9 Session Management

### 7.9.0 MQTT 3.1.1 Sessions

In MQTT 3.1.1, session persistence is controlled by the `clean_session` flag:

| clean_session | Behavior |
|---------------|----------|
| 1 | Discard any previous session. No state persisted after disconnect. |
| 0 | Resume previous session if exists. Persist subscriptions and inflight QoS 1/2 messages. |

For 3.1.1, sessions are either **active** (client connected) or **stored** (client disconnected with clean_session=0). There is no expiry - sessions persist until the client reconnects with clean_session=1.

```c
// MQTT 3.1.1 simplified session
struct session_v3 {
    u32  client_id_hash;
    u16  client_id_len;
    u8   exists;                 // 0 = no session, 1 = has stored session

    // Persisted state (only when clean_session=0)
    u32  sub_topics[64];         // Topic trie indices
    u8   sub_qos[64];
    u32  sub_count;

    struct conn_inflight inflight;  // Pending QoS 1/2 deliveries

    u8   client_id[MAX_CLIENT_ID_LEN];
};
```

### 7.9.1 Session State Machine (MQTT 5.0 - Optional)

MQTT 5.0 introduces session expiry, requiring explicit session lifecycle management. **This section is optional for the initial implementation.**

### 7.9.1 Session States

```c
enum session_state {
    SESSION_NONE     = 0,        // No session exists
    SESSION_ACTIVE   = 1,        // Client connected, session in use
    SESSION_DORMANT  = 2,        // Client disconnected, session retained
    SESSION_EXPIRED  = 3,        // Expiry elapsed, pending cleanup
};
```

### 7.9.2 Session State Machine

```
                     ┌─────────────────┐
                     │                 │
  Clean Start=1      │   SESSION_NONE  │◄──────────────────────────┐
  ┌─────────────────►│                 │                           │
  │                  └────────┬────────┘                           │
  │                           │                                    │
  │                           │ CONNECT with                       │
  │                           │ Clean Start=0                      │
  │                           │ (new session)                      │
  │                           ▼                                    │
  │                  ┌─────────────────┐                           │
  │                  │                 │                           │
  │  CONNECT with    │ SESSION_ACTIVE  │◄──────────────┐           │
  │  Clean Start=1   │  (connected)    │               │           │
  │  (discard)       └────────┬────────┘               │           │
  │                           │                        │           │
  └───────────────────────────┤                        │           │
                              │ DISCONNECT or          │           │
                              │ connection lost        │           │
                              │                        │           │
                              │ session_expiry > 0     │           │
                              ▼                        │           │
                     ┌─────────────────┐               │           │
                     │                 │   Reconnect   │           │
                     │ SESSION_DORMANT │   Clean       │           │
                     │  (waiting)      │───Start=0────►│           │
                     └────────┬────────┘               │           │
                              │                                    │
                              │ session_expiry                     │
                              │ elapsed                            │
                              ▼                                    │
                     ┌─────────────────┐                           │
                     │                 │   Cleanup                 │
                     │ SESSION_EXPIRED │   complete                │
                     │  (cleanup)      │───────────────────────────┘
                     └─────────────────┘
```

### 7.9.3 Session Expiry Handling

```c
struct session {
    u32  client_id_hash;         // For lookup
    u8   state;                  // enum session_state
    u32  expiry_interval;        // Seconds (0 = expire on disconnect)
    u32  disconnect_time;        // When client disconnected

    // Retained state
    u32  subscription_bitmap[32]; // Topic indices subscribed
    u16  inflight_count;         // Pending QoS 1/2 messages
    // ... inflight message storage
};

int session_is_expired(struct session *s, u32 now) {
    if (s->state != SESSION_DORMANT) return 0;
    if (s->expiry_interval == 0xFFFFFFFF) return 0;  // Never expires
    return (now - s->disconnect_time) >= s->expiry_interval;
}
```

### 7.9.4 Session Takeover

When a client connects with a Client ID that has an active session:

1. The existing connection MUST be closed (send DISCONNECT with reason 0x8E "Session taken over")
2. If Clean Start=0, the session state is transferred to the new connection
3. If Clean Start=1, the session state is discarded

### 7.9.5 Will Message Handling

#### MQTT 3.1.1

Will messages are published **immediately** upon ungraceful disconnect:

```c
void conn_disconnect(struct broker *b, struct conn_slot *slot) {
    // Publish will if ungraceful disconnect
    if (slot->will_flag) {
        publish_will_immediate(b, slot);
    }
    // ... cleanup
}
```

An ungraceful disconnect is any disconnect without a DISCONNECT packet (TCP close, timeout, protocol error).

#### MQTT 5.0 - Will Delay (Optional)

Will messages can be delayed using the Will Delay Interval property:

```
                                       Will Delay > 0
                                       ┌──────────────────────────────────┐
                                       │                                  │
                                       ▼                                  │
┌──────────────┐  Disconnect   ┌───────────────┐  Delay     ┌────────────┴───┐
│              │  (ungraceful) │               │  elapsed   │                │
│   CONNECTED  │──────────────►│ WILL_PENDING  │───────────►│ WILL_PUBLISHED │
│              │               │               │            │                │
└──────────────┘               └───────┬───────┘            └────────────────┘
                                       │
                                       │ Client reconnects
                                       │ within delay
                                       ▼
                               ┌───────────────┐
                               │               │
                               │ WILL_CANCELED │
                               │               │
                               └───────────────┘
```

```c
struct will_state {
    u32 topic_idx;               // Topic trie index
    u32 msg_buf_idx;             // Buffer holding message
    u16 msg_len;
    u8  qos;
    u8  retain;
    u32 delay_interval;          // Seconds (0 = immediate)
    u32 disconnect_time;         // When client disconnected
};

int will_should_publish(struct will_state *w, u32 now) {
    if (w->delay_interval == 0) return 1;  // Immediate
    return (now - w->disconnect_time) >= w->delay_interval;
}
```

If the client reconnects before the will delay elapses, the will message MUST NOT be published.

## 7.10 Error Handling

### 7.10.1 Protocol Errors

| Error | MQTT 3.1.1 Action | MQTT 5.0 Action |
|-------|-------------------|-----------------|
| Malformed packet | Close connection | DISCONNECT + close |
| Protocol violation | Close connection | DISCONNECT + close |
| Not authorized | CONNACK failure | CONNACK/DISCONNECT |
| Topic invalid | Ignore PUBLISH | DISCONNECT |

### 7.10.2 Reason Codes (MQTT 5.0)

```c
enum mqtt_reason_code {
    MQTT_RC_SUCCESS                 = 0x00,
    MQTT_RC_NORMAL_DISCONNECT       = 0x00,
    MQTT_RC_QOS_1                   = 0x01,
    MQTT_RC_QOS_2                   = 0x02,
    MQTT_RC_NO_MATCHING_SUBSCRIBERS = 0x10,
    MQTT_RC_UNSPECIFIED_ERROR       = 0x80,
    MQTT_RC_MALFORMED_PACKET        = 0x81,
    MQTT_RC_PROTOCOL_ERROR          = 0x82,
    MQTT_RC_NOT_AUTHORIZED          = 0x87,
    MQTT_RC_TOPIC_NAME_INVALID      = 0x90,
    MQTT_RC_PACKET_ID_IN_USE        = 0x91,
    MQTT_RC_QUOTA_EXCEEDED          = 0x97,
    MQTT_RC_SESSION_TAKEN_OVER      = 0x8E,
};
```

## 7.11 Compliance Notes

The implementation MUST pass the MQTT conformance test suite for claimed features. Key test categories:

1. **Connection** - CONNECT validation, CONNACK codes
2. **Publish/Subscribe** - All QoS levels, wildcards
3. **Session** - Clean session, session takeover
4. **Keep alive** - Timeout behavior
5. **Will message** - Delivery on ungraceful disconnect
