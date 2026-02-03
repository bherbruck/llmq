# Section 12: Security Considerations

## 12.1 Threat Model

### 12.1.1 In Scope

| Threat | Description |
|--------|-------------|
| Malformed packets | Invalid MQTT packets designed to crash or exploit |
| Resource exhaustion | Attempts to exhaust memory, connections, or CPU |
| Unauthorized access | Clients without valid credentials |
| Topic hijacking | Subscribing or publishing to unauthorized topics |
| Denial of service | Overwhelming the broker with traffic |

### 12.1.2 Out of Scope

| Threat | Mitigation |
|--------|------------|
| Network eavesdropping | Use TLS termination proxy |
| Man-in-the-middle | Use TLS termination proxy |
| Physical access | Standard server security |
| Kernel exploits | OS hardening |

## 12.2 Input Validation

### 12.2.1 Packet Length Limits

All packet lengths MUST be validated before processing:

```c
#define MAX_PACKET_SIZE     (256 * 1024)  // 256 KiB
#define MAX_TOPIC_LEN       (32 * 1024)   // 32 KiB
#define MAX_CLIENT_ID_LEN   256
#define MAX_PAYLOAD_SIZE    (256 * 1024)

int validate_packet_length(u32 remaining_length) {
    if (remaining_length > MAX_PACKET_SIZE) {
        return -1;  // Reject oversized packet
    }
    return 0;
}
```

### 12.2.2 UTF-8 Validation

Topic names and client IDs MUST be valid UTF-8:

```c
int validate_utf8(const u8 *data, u32 len) {
    u32 i = 0;
    while (i < len) {
        u8 c = data[i];
        
        if (c == 0x00) {
            return -1;  // Null character not allowed
        }
        
        u32 char_len;
        if ((c & 0x80) == 0x00) {
            char_len = 1;
        } else if ((c & 0xE0) == 0xC0) {
            char_len = 2;
        } else if ((c & 0xF0) == 0xE0) {
            char_len = 3;
        } else if ((c & 0xF8) == 0xF0) {
            char_len = 4;
        } else {
            return -1;  // Invalid UTF-8 start byte
        }
        
        if (i + char_len > len) {
            return -1;  // Truncated character
        }
        
        // Validate continuation bytes
        for (u32 j = 1; j < char_len; j++) {
            if ((data[i + j] & 0xC0) != 0x80) {
                return -1;
            }
        }
        
        i += char_len;
    }
    return 0;
}
```

### 12.2.3 Topic Validation

Topics MUST be validated for:

1. Non-empty
2. Valid UTF-8
3. No null characters
4. Wildcards in valid positions (subscribe only)
5. Length within limits

```c
int validate_publish_topic(const u8 *topic, u16 len) {
    if (len == 0 || len > MAX_TOPIC_LEN) return -1;
    if (validate_utf8(topic, len) < 0) return -1;
    
    // No wildcards allowed in publish topics
    for (u16 i = 0; i < len; i++) {
        if (topic[i] == '+' || topic[i] == '#') {
            return -1;
        }
    }
    
    return 0;
}
```

## 12.3 Resource Limits

### 12.3.1 Connection Limits

```c
#define MAX_CONNS           65536
#define MAX_CONNS_PER_IP    1000

struct ip_tracker {
    u32 ip_addr;
    u32 conn_count;
};

int check_connection_limit(struct broker *b, u32 client_ip) {
    // Global limit
    if (b->conns.active_count >= MAX_CONNS) {
        return -1;
    }
    
    // Per-IP limit
    struct ip_tracker *tracker = find_ip_tracker(b, client_ip);
    if (tracker && tracker->conn_count >= MAX_CONNS_PER_IP) {
        return -1;
    }
    
    return 0;
}
```

### 12.3.2 Subscription Limits

```c
#define MAX_SUBS_PER_CLIENT     1000
#define MAX_TOPIC_DEPTH         32

int check_subscription_limit(struct conn_slot *conn) {
    if (conn->sub_count >= MAX_SUBS_PER_CLIENT) {
        return -1;
    }
    return 0;
}

int check_topic_depth(const u8 *topic, u16 len) {
    u32 depth = 1;
    for (u16 i = 0; i < len; i++) {
        if (topic[i] == '/') {
            depth++;
            if (depth > MAX_TOPIC_DEPTH) {
                return -1;
            }
        }
    }
    return 0;
}
```

### 12.3.3 Message Rate Limits

```c
#define MAX_MSG_PER_SEC         10000
#define RATE_WINDOW_MS          1000

struct rate_limiter {
    u32 msg_count;
    u64 window_start;
};

int check_rate_limit(struct conn_slot *conn, u64 now_ms) {
    struct rate_limiter *rl = &conn->rate_limiter;
    
    if (now_ms - rl->window_start > RATE_WINDOW_MS) {
        // New window
        rl->window_start = now_ms;
        rl->msg_count = 0;
    }
    
    if (rl->msg_count >= MAX_MSG_PER_SEC) {
        return -1;  // Rate limited
    }
    
    rl->msg_count++;
    return 0;
}
```

## 12.4 Authentication

### 12.4.1 Username/Password

Basic authentication via CONNECT packet:

```c
struct auth_entry {
    u32 username_hash;
    u8  password_hash[32];  // SHA-256
};

int authenticate(struct broker *b, const u8 *username, u16 username_len,
                 const u8 *password, u16 password_len) {
    u32 hash = fnv1a(username, username_len);
    
    struct auth_entry *entry = auth_lookup(b, hash);
    if (entry == NULL) {
        return -1;  // User not found
    }
    
    u8 pwd_hash[32];
    sha256(password, password_len, pwd_hash);
    
    if (memcmp(pwd_hash, entry->password_hash, 32) != 0) {
        return -1;  // Password mismatch
    }
    
    return 0;
}
```

### 12.4.2 Authentication File Format

```
# /etc/broker/passwd
# username:sha256(password)
admin:5e884898da28047d6103...
sensor1:6cf615d5bcaac778...
```

### 12.4.3 Anonymous Access

Anonymous access MAY be allowed if configured:

```c
int authenticate(struct broker *b, ...) {
    if (username_len == 0 && b->config.allow_anonymous) {
        return 0;  // Anonymous allowed
    }
    
    // Normal authentication...
}
```

## 12.5 Authorization

### 12.5.1 ACL Structure

```c
enum acl_permission {
    ACL_DENY     = 0,
    ACL_READ     = 1,  // Subscribe
    ACL_WRITE    = 2,  // Publish
    ACL_READWRITE = 3,
};

struct acl_entry {
    u32 username_hash;
    u8  topic_pattern[256];
    u16 topic_pattern_len;
    u8  permission;
};
```

### 12.5.2 ACL Checking

```c
int check_acl(struct broker *b, struct conn_slot *conn,
              const u8 *topic, u16 topic_len, enum acl_permission required) {
    // Find ACL entries for this user
    struct acl_entry *entries;
    int count = acl_find_entries(b, conn->username_hash, &entries);
    
    for (int i = 0; i < count; i++) {
        if (topic_matches_pattern(topic, topic_len,
                                   entries[i].topic_pattern,
                                   entries[i].topic_pattern_len)) {
            if (entries[i].permission & required) {
                return 0;  // Allowed
            }
        }
    }
    
    return -1;  // Denied
}
```

### 12.5.3 ACL File Format

```
# /etc/broker/acl
# user topic permission
admin # rw
sensor1 sensors/+/data w
sensor1 commands/sensor1 r
```

## 12.6 Memory Safety

### 12.6.1 Bounds Checking

All buffer accesses MUST be bounds-checked:

```c
// UNSAFE
u16 len = *(u16*)(buf + offset);
memcpy(dest, buf + offset + 2, len);  // Could overflow

// SAFE
if (offset + 2 > buf_len) return -1;
u16 len = read_u16(buf + offset);
if (offset + 2 + len > buf_len) return -1;
memcpy(dest, buf + offset + 2, len);
```

### 12.6.2 Integer Overflow

Variable-length integer decoding MUST check for overflow:

```c
int decode_varint(const u8 *buf, u32 buf_len, u32 *value, u32 *bytes_read) {
    u32 multiplier = 1;
    u32 result = 0;
    u32 pos = 0;
    
    do {
        if (pos >= buf_len || pos >= 4) {
            return -1;  // Malformed or overflow
        }
        
        u8 byte = buf[pos++];
        
        // Check for overflow before adding
        u32 contribution = (byte & 0x7F) * multiplier;
        if (result > UINT32_MAX - contribution) {
            return -1;  // Would overflow
        }
        
        result += contribution;
        multiplier *= 128;
        
        if ((byte & 0x80) == 0) break;
    } while (1);
    
    *value = result;
    *bytes_read = pos;
    return 0;
}
```

## 12.7 TLS Termination

The broker itself does not implement TLS. For encrypted connections, use a terminating proxy:

```
┌────────────┐     TLS      ┌─────────────┐    TCP     ┌────────────┐
│   Client   │◄────────────►│   HAProxy   │◄──────────►│   Broker   │
│            │   :8883      │   / nginx   │   :1883    │            │
└────────────┘              └─────────────┘            └────────────┘
```

### 12.7.1 HAProxy Configuration

```
frontend mqtt_tls
    bind *:8883 ssl crt /etc/ssl/mqtt.pem
    mode tcp
    default_backend mqtt_backend

backend mqtt_backend
    mode tcp
    server broker1 127.0.0.1:1883
```

### 12.7.2 Client IP Preservation

Use PROXY protocol to preserve client IP:

```
frontend mqtt_tls
    bind *:8883 ssl crt /etc/ssl/mqtt.pem
    mode tcp
    default_backend mqtt_backend

backend mqtt_backend
    mode tcp
    server broker1 127.0.0.1:1883 send-proxy-v2
```

The broker can parse PROXY protocol header to get real client IP.

## 12.8 Logging

### 12.8.1 Security Events

Log security-relevant events:

```c
enum log_event {
    LOG_CONNECT,
    LOG_CONNECT_REJECT,
    LOG_AUTH_FAIL,
    LOG_ACL_DENY,
    LOG_DISCONNECT,
    LOG_RATE_LIMIT,
    LOG_PROTOCOL_ERROR,
};

void log_security(enum log_event event, struct conn_slot *conn, const char *detail) {
    // Format: timestamp event client_id client_ip detail
    log_write("%lu %s %s %s %s\n",
              current_time_ms(),
              event_name(event),
              conn->client_id,
              format_ip(conn->client_ip),
              detail);
}
```

### 12.8.2 Audit Trail

For compliance, maintain an audit log of:

1. All authentication attempts (success and failure)
2. All ACL denials
3. Connection establishment and termination
4. Configuration changes
