# Section 6: Connection Management

## 6.1 Overview

Connection management handles the lifecycle of TCP connections from accept through disconnect. Each connection is assigned a slot indexed by its direct descriptor number, providing O(1) access to connection state.

## 6.2 Connection Slot

### 6.2.1 Structure

```c
struct conn_slot {
    // === Cache line 1: Hot path data ===
    i32  fd;                     // Direct descriptor index (-1 = free)
    u8   state;                  // enum conn_state
    u8   protocol_version;       // 4 (MQTT 3.1.1) or 5 (MQTT 5.0)
    u16  keepalive;              // Keepalive interval in seconds
    u32  recv_buf_idx;           // Current receive buffer
    u16  recv_offset;            // Parse position in buffer
    u16  recv_len;               // Valid bytes in buffer
    u32  last_active;            // Monotonic timestamp (seconds)
    u32  pending_sends;          // Outstanding send operations
    u8   flags;                  // CONN_FLAG_* bits
    u8   _reserved[27];          // Pad to 64 bytes
    
    // === Cache line 2: Session data ===
    u32  client_id_hash;         // FNV-1a hash for fast comparison
    u16  client_id_len;          // Length of client ID
    u8   clean_session;          // Clean session flag
    u8   will_flag;              // Has will message
    u32  will_topic_idx;         // Topic trie index for will
    u32  will_msg_idx;           // Buffer index for will message
    u16  will_msg_len;
    u8   will_qos;
    u8   will_retain;
    u8   _reserved2[44];         // Pad to 64 bytes
    
    // === Beyond hot cache lines: Variable data ===
    // Client ID stored inline if <= 64 bytes, otherwise in separate buffer
    u8   client_id[64];
};

_Static_assert(sizeof(struct conn_slot) == 192, "conn_slot size check");

enum conn_state {
    CONN_FREE       = 0,         // Slot available
    CONN_ACCEPTING  = 1,         // TCP accepted, awaiting CONNECT
    CONN_CONNECTED  = 2,         // MQTT session established
    CONN_DRAINING   = 3,         // Graceful shutdown, flushing sends
};

enum conn_flags {
    CONN_FLAG_WILL_RETAIN  = 0x01,
    CONN_FLAG_CLEAN_START  = 0x02,
    CONN_FLAG_ASSIGNED_ID  = 0x04,  // Server assigned client ID
};
```

### 6.2.2 Slot Array

```c
struct conn_manager {
    struct conn_slot slots[MAX_CONNS];
    u32 active_count;            // Number of connected clients
    u32 high_water_mark;         // Peak connections observed
};
```

Slots are directly indexed by direct descriptor number. When using `IORING_FILE_INDEX_ALLOC`, the kernel assigns sequential indices, making slot lookup trivial.

## 6.3 Connection State Machine

This state machine tracks the TCP connection lifecycle. For MQTT session state (which persists across reconnects in MQTT 5.0), see [Section 7.9: Session State Machine](./07-mqtt.md#79-session-state-machine-mqtt-50).

```
                            ┌─────────────────┐
                            │                 │
              ACCEPT CQE    │      FREE       │
           ┌───────────────►│    (fd = -1)    │◄─────────────────┐
           │                │                 │                  │
           │                └─────────────────┘                  │
           │                        │                            │
           │                        │ Accept new connection      │
           │                        ▼                            │
           │                ┌─────────────────┐                  │
           │                │                 │                  │
           │  Timeout or    │   ACCEPTING     │                  │
           │  Invalid       │  (awaiting      │                  │
           │  CONNECT       │   CONNECT pkt)  │                  │
           │◄───────────────│                 │                  │
           │                └─────────────────┘                  │
           │                        │                            │
           │                        │ Valid CONNECT received     │
           │                        ▼                            │
           │                ┌─────────────────┐                  │
           │                │                 │                  │
           │  Protocol      │   CONNECTED     │   DISCONNECT or  │
           │  error         │  (normal ops)   │   client close   │
           │◄───────────────│                 │──────────────────┤
           │                └─────────────────┘                  │
           │                        │                            │
           │                        │ Server-initiated close     │
           │                        │ (keepalive timeout, etc)   │
           │                        ▼                            │
           │                ┌─────────────────┐                  │
           │                │                 │                  │
           │                │    DRAINING     │                  │
           │                │  (pending_sends │──────────────────┘
           │                │   draining)     │  pending_sends == 0
           │                └─────────────────┘
           │                        │
           │                        │ All sends complete
           └────────────────────────┘
```

## 6.4 Lifecycle Operations

### 6.4.1 Accept

When an ACCEPT CQE arrives:

```c
void conn_accept(struct broker *b, int direct_fd) {
    if (direct_fd >= MAX_CONNS) {
        // Exceeds capacity, close immediately
        submit_close(&b->ring, direct_fd);
        return;
    }
    
    struct conn_slot *slot = &b->conns.slots[direct_fd];
    
    // Initialize slot
    slot->fd = direct_fd;
    slot->state = CONN_ACCEPTING;
    slot->protocol_version = 0;
    slot->keepalive = 0;
    slot->recv_buf_idx = buf_alloc(&b->buffers);
    slot->recv_offset = 0;
    slot->recv_len = 0;
    slot->last_active = current_time_sec();
    slot->pending_sends = 0;
    slot->flags = 0;
    slot->client_id_len = 0;
    
    if (slot->recv_buf_idx < 0) {
        // No buffers available, reject connection
        submit_close(&b->ring, direct_fd);
        slot->fd = -1;
        slot->state = CONN_FREE;
        return;
    }
    
    b->conns.active_count++;
    
    // Arm receive for CONNECT packet
    submit_recv(&b->ring, direct_fd, slot->recv_buf_idx);
}
```

### 6.4.2 CONNECT Processing

When CONNECT packet received:

```c
int conn_handle_connect(struct broker *b, struct conn_slot *slot,
                        u8 *buf, u32 len) {
    struct mqtt_connect conn;
    int rc = mqtt_parse_connect(buf, len, &conn);
    
    if (rc != MQTT_OK) {
        conn_close(b, slot, MQTT_CONNACK_PROTOCOL_ERROR);
        return -1;
    }
    
    // Validate protocol version
    if (conn.protocol_version != 4 && conn.protocol_version != 5) {
        conn_close(b, slot, MQTT_CONNACK_UNSUPPORTED_PROTOCOL);
        return -1;
    }
    
    // Store session info
    slot->protocol_version = conn.protocol_version;
    slot->keepalive = conn.keepalive;
    slot->flags = conn.clean_session ? CONN_FLAG_CLEAN_START : 0;
    
    // Copy client ID
    if (conn.client_id_len == 0) {
        // Generate server-assigned client ID
        generate_client_id(slot);
        slot->flags |= CONN_FLAG_ASSIGNED_ID;
    } else if (conn.client_id_len <= 64) {
        memcpy(slot->client_id, conn.client_id, conn.client_id_len);
        slot->client_id_len = conn.client_id_len;
    } else {
        conn_close(b, slot, MQTT_CONNACK_CLIENT_ID_REJECTED);
        return -1;
    }
    
    slot->client_id_hash = fnv1a(slot->client_id, slot->client_id_len);
    
    // Handle will message if present
    if (conn.will_flag) {
        slot->will_flag = 1;
        slot->will_qos = conn.will_qos;
        slot->will_retain = conn.will_retain;
        // Store will topic and message...
    }
    
    // Transition to connected state
    slot->state = CONN_CONNECTED;
    
    // Send CONNACK
    u8 connack[4];  // Fixed header + variable header
    mqtt_encode_connack(connack, 0, slot->flags & CONN_FLAG_ASSIGNED_ID);
    send_packet(b, slot, connack, sizeof(connack));
    
    // Arm multishot receive for subsequent packets
    submit_recv_multishot(&b->ring, slot->fd, RECV_BUF_GROUP);
    
    return 0;
}
```

### 6.4.3 Disconnect

Normal disconnect (client-initiated or clean server shutdown):

```c
void conn_disconnect(struct broker *b, struct conn_slot *slot) {
    if (slot->state == CONN_FREE) return;
    
    // Remove from all subscriptions
    trie_remove_subscriber(&b->trie, slot->fd);
    
    // Handle will message if ungraceful disconnect
    if (slot->will_flag && slot->state == CONN_CONNECTED) {
        publish_will(b, slot);
    }
    
    // Check if we can close immediately
    if (slot->pending_sends == 0) {
        conn_free(b, slot);
    } else {
        // Wait for pending sends to complete
        slot->state = CONN_DRAINING;
    }
}
```

### 6.4.4 Free Slot

```c
void conn_free(struct broker *b, struct conn_slot *slot) {
    // Submit close operation
    submit_close(&b->ring, slot->fd);
    
    // Return receive buffer
    if (slot->recv_buf_idx >= 0) {
        buf_free(&b->buffers, slot->recv_buf_idx);
    }
    
    // Clear slot
    slot->fd = -1;
    slot->state = CONN_FREE;
    
    b->conns.active_count--;
}
```

## 6.5 Timeouts

### 6.5.1 CONNECT Timeout

Connections in `CONN_ACCEPTING` state MUST receive a valid CONNECT packet within `CONNECT_TIMEOUT` seconds:

```c
#define CONNECT_TIMEOUT 10  // Seconds

int conn_is_connect_timeout(struct conn_slot *slot, u32 now) {
    if (slot->state != CONN_ACCEPTING) return 0;
    return (now - slot->last_active) > CONNECT_TIMEOUT;
}
```

The timeout check integrates with the keepalive scan:

```c
void check_timeouts(struct broker *b) {
    u32 now = current_time_sec();

    for (u32 i = 0; i < MAX_CONNS; i++) {
        struct conn_slot *slot = &b->conns.slots[i];

        if (slot->state == CONN_FREE) continue;

        if (slot->state == CONN_ACCEPTING) {
            // CONNECT timeout
            if (conn_is_connect_timeout(slot, now)) {
                conn_free(b, slot);  // No DISCONNECT, just close
            }
        } else if (slot->state == CONN_CONNECTED) {
            // Keepalive timeout
            if (now > keepalive_deadline(slot)) {
                conn_disconnect(b, slot);
            }
        }
    }
}
```

### 6.5.3 Keepalive Calculation

Per MQTT spec, the broker MUST disconnect a client if no packet is received within 1.5× the keepalive interval:

```c
u32 keepalive_deadline(struct conn_slot *slot) {
    if (slot->keepalive == 0) {
        return UINT32_MAX;  // No keepalive
    }
    return slot->last_active + (slot->keepalive * 3 / 2);
}
```

### 6.5.4 Timeout Implementation Notes

For efficiency, the timeout scan SHOULD use one of:

1. **Timer wheel** - O(1) insert/expire, good for many connections with similar timeouts
2. **Sorted deadline list** - O(log n) insert, O(1) expire check
3. **Fractional scan** - Check 1/N connections per iteration, amortized O(1)

The fractional scan is simplest for initial implementation:

```c
void check_timeouts_incremental(struct broker *b, u32 *scan_idx) {
    u32 now = current_time_sec();
    u32 batch = MAX_CONNS / 64;  // Check ~1.5% per iteration

    for (u32 i = 0; i < batch; i++) {
        u32 idx = (*scan_idx)++ % MAX_CONNS;
        struct conn_slot *slot = &b->conns.slots[idx];
        // ... timeout checks as in check_timeouts()
    }
}
```

### 6.5.5 QoS Retransmission Timeout

For QoS 1/2 messages, the broker MUST retransmit if no acknowledgment is received:

```c
#define QOS_RETRY_INTERVAL 20   // Seconds between retransmissions
#define QOS_RETRY_MAX      3    // Maximum retry attempts

void check_qos_retransmits(struct broker *b, struct conn_slot *slot) {
    u32 now = current_time_sec();
    struct conn_inflight *inf = &slot->inflight;

    for (int i = 0; i < MAX_INFLIGHT_PER_CONN; i++) {
        struct inflight_msg *msg = &inf->msgs[i];
        if (msg->state == INFLIGHT_FREE) continue;

        if ((now - msg->timestamp) < QOS_RETRY_INTERVAL) continue;

        if (msg->dup_count >= QOS_RETRY_MAX) {
            // Exceeded retry limit, disconnect client
            conn_disconnect(b, slot);
            return;
        }

        // Retransmit based on state
        msg->dup_count++;
        msg->timestamp = now;

        switch (msg->state) {
        case INFLIGHT_WAIT_PUBACK:
        case INFLIGHT_WAIT_PUBREC:
            // Resend PUBLISH with DUP=1
            resend_publish(b, slot, msg);
            break;
        case INFLIGHT_WAIT_PUBCOMP:
            // Resend PUBREL
            resend_pubrel(b, slot, msg->packet_id);
            break;
        case INFLIGHT_WAIT_PUBREL:
            // Resend PUBREC (receiver side)
            resend_pubrec(b, slot, msg->packet_id);
            break;
        }
    }
}
```

## 6.6 Send Tracking

### 6.6.1 Pending Send Count

Each connection tracks outstanding sends:

```c
void conn_send_submitted(struct conn_slot *slot) {
    slot->pending_sends++;
}

void conn_send_completed(struct broker *b, struct conn_slot *slot) {
    slot->pending_sends--;
    
    if (slot->state == CONN_DRAINING && slot->pending_sends == 0) {
        conn_free(b, slot);
    }
}
```

### 6.6.2 Backpressure

If a connection's pending send count exceeds a threshold, the broker SHOULD stop delivering messages to it:

```c
#define MAX_PENDING_SENDS 256

int conn_can_receive(struct conn_slot *slot) {
    return slot->state == CONN_CONNECTED && 
           slot->pending_sends < MAX_PENDING_SENDS;
}
```

The topic fan-out logic checks this before including a subscriber in the send set.

## 6.7 Receive Buffer Management

### 6.7.1 Partial Packets

MQTT packets may arrive fragmented across multiple receives. The slot tracks parse state:

```c
struct conn_slot {
    // ...
    u32  recv_buf_idx;           // Buffer holding partial packet
    u16  recv_offset;            // Next byte to parse
    u16  recv_len;               // Total valid bytes
    u32  packet_remaining;       // Bytes still needed for current packet
};
```

### 6.7.2 Reassembly

```c
int conn_on_recv(struct broker *b, struct conn_slot *slot, u32 bytes) {
    slot->recv_len += bytes;
    slot->last_active = current_time_sec();
    
    while (slot->recv_len > slot->recv_offset) {
        u8 *buf = buf_ptr(&b->buffers, slot->recv_buf_idx);
        u32 available = slot->recv_len - slot->recv_offset;
        
        // Try to parse packet header
        struct mqtt_header hdr;
        int hdr_len = mqtt_parse_header(buf + slot->recv_offset, available, &hdr);
        
        if (hdr_len < 0) {
            // Need more data
            break;
        }
        
        u32 packet_len = hdr_len + hdr.remaining_length;
        
        if (available < packet_len) {
            // Partial packet, need more data
            if (packet_len > BUFFER_SIZE) {
                // Packet too large
                return -1;
            }
            break;
        }
        
        // Full packet available, process it
        int rc = process_packet(b, slot, buf + slot->recv_offset, packet_len);
        if (rc < 0) {
            return rc;
        }
        
        slot->recv_offset += packet_len;
    }
    
    // Compact buffer if needed
    if (slot->recv_offset > 0 && slot->recv_offset == slot->recv_len) {
        // All data consumed, reset
        slot->recv_offset = 0;
        slot->recv_len = 0;
    } else if (slot->recv_offset > BUFFER_SIZE / 2) {
        // Shift remaining data to start
        u8 *buf = buf_ptr(&b->buffers, slot->recv_buf_idx);
        u32 remaining = slot->recv_len - slot->recv_offset;
        memmove(buf, buf + slot->recv_offset, remaining);
        slot->recv_offset = 0;
        slot->recv_len = remaining;
    }
    
    return 0;
}
```

## 6.8 Error Handling

| Error | Detection | Action |
|-------|-----------|--------|
| Protocol violation | Invalid packet structure | Close connection |
| CONNECT timeout | No CONNECT within 10s | Close connection |
| Keepalive timeout | No packet within 1.5× keepalive | Close connection |
| QoS retry exhausted | 3 retransmits without ACK | Close connection |
| Send failure | CQE with error | Close connection |
| Receive failure | CQE with error | Close connection |
| Resource exhaustion | No buffers/slots | Reject connection |

**Note:** MQTT 3.1.1 has no server-initiated DISCONNECT packet. The broker simply closes the TCP connection. MQTT 5.0 adds DISCONNECT with reason codes before closing.
