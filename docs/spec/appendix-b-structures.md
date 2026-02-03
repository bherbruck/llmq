# Appendix B: Data Structures

## B.1 Overview

This appendix provides complete definitions of all data structures used by the broker.

## B.2 Type Definitions

```c
// sys/types.h

typedef unsigned char      u8;
typedef unsigned short     u16;
typedef unsigned int       u32;
typedef unsigned long long u64;

typedef signed char        i8;
typedef signed short       i16;
typedef signed int         i32;
typedef signed long long   i64;

typedef u64 usize;
typedef i64 isize;

#define NULL  ((void*)0)
#define true  1
#define false 0
```

## B.3 Configuration Constants

```c
// broker/config.h

#define MAX_CONNS           65536       // Maximum concurrent connections
#define MAX_BUFS            4096        // Buffer pool size
#define BUFFER_SIZE         65536       // 64 KiB per buffer
#define MAX_TOPICS          65536       // Maximum topic trie nodes
#define MAX_INFLIGHT        65536       // Maximum in-flight messages
#define MAX_PROCESSES       64          // Maximum worker processes

#define SQ_ENTRIES          4096        // io_uring submission queue size
#define CQ_ENTRIES          16384       // io_uring completion queue size

#define MAX_PACKET_SIZE     (256*1024)  // 256 KiB max MQTT packet
#define MAX_TOPIC_LEN       (32*1024)   // 32 KiB max topic length
#define MAX_CLIENT_ID_LEN   256         // Max client ID length
#define MAX_SEGMENT_LEN     128         // Max topic segment length
#define MAX_CHILDREN        64          // Max children per trie node

#define DEFAULT_KEEPALIVE   60          // Default keepalive in seconds
#define CONNECT_TIMEOUT     10          // Seconds to wait for CONNECT
#define MAX_PENDING_SENDS   256         // Backpressure threshold
```

## B.4 io_uring Structures

```c
// sys/io_uring.h

struct io_uring_sqe {
    u8  opcode;
    u8  flags;
    u16 ioprio;
    i32 fd;
    union {
        u64 off;
        u64 addr2;
    };
    union {
        u64 addr;
        u64 splice_off_in;
    };
    u32 len;
    union {
        u32 rw_flags;
        u32 fsync_flags;
        u32 poll_events;
        u32 msg_flags;
        u32 accept_flags;
        u32 buf_index;
    };
    u64 user_data;
    union {
        u16 buf_group;
        u16 personality;
    };
    union {
        i32 splice_fd_in;
        u32 file_index;
    };
    u64 __pad2[2];
};

struct io_uring_cqe {
    u64 user_data;
    i32 res;
    u32 flags;
};

struct io_uring_params {
    u32 sq_entries;
    u32 cq_entries;
    u32 flags;
    u32 sq_thread_cpu;
    u32 sq_thread_idle;
    u32 features;
    u32 wq_fd;
    u32 resv[3];
    struct io_sqring_offsets sq_off;
    struct io_cqring_offsets cq_off;
};

struct io_sqring_offsets {
    u32 head;
    u32 tail;
    u32 ring_mask;
    u32 ring_entries;
    u32 flags;
    u32 dropped;
    u32 array;
    u32 resv1;
    u64 resv2;
};

struct io_cqring_offsets {
    u32 head;
    u32 tail;
    u32 ring_mask;
    u32 ring_entries;
    u32 overflow;
    u32 cqes;
    u32 flags;
    u32 resv1;
    u64 resv2;
};
```

## B.5 Ring Wrapper

```c
// sys/ring.h

struct ring {
    int fd;                          // io_uring file descriptor
    
    // Submission queue
    u32 *sq_head;
    u32 *sq_tail;
    u32 *sq_array;
    u32  sq_mask;
    struct io_uring_sqe *sqes;
    
    // Completion queue
    u32 *cq_head;
    u32 *cq_tail;
    u32  cq_mask;
    struct io_uring_cqe *cqes;
    
    // Pending operations
    u32 sq_pending;
};
```

## B.6 Buffer Pool

```c
// mem/pool.h

struct buffer_pool {
    u8 *base;                        // mmap'd buffer memory
    u32 capacity;                    // Number of buffers
    u32 free_head;                   // Head of freelist
    u32 free_count;                  // Available buffers
    u32 next[MAX_BUFS];              // Freelist links
};
```

## B.7 Message Reference

```c
// broker/ref.h

struct msg_ref {
    u32 buf_idx;                     // Buffer pool index
    u32 offset;                      // Offset within buffer
    u32 len;                         // Message length
    _Atomic u32 refcount;            // Outstanding sends
};

struct msg_ref_pool {
    struct msg_ref refs[MAX_INFLIGHT];
    u32 free_head;
    u32 free_count;
    u32 next[MAX_INFLIGHT];
};
```

## B.8 Connection Slot

```c
// broker/conn.h

enum conn_state {
    CONN_FREE       = 0,
    CONN_ACCEPTING  = 1,
    CONN_CONNECTED  = 2,
    CONN_DRAINING   = 3,
};

enum conn_flags {
    CONN_FLAG_WILL_RETAIN  = 0x01,
    CONN_FLAG_CLEAN_START  = 0x02,
    CONN_FLAG_ASSIGNED_ID  = 0x04,
};

struct conn_slot {
    // === Hot path data (cache line 1) ===
    i32  fd;                         // Direct descriptor (-1 = free)
    u8   state;                      // enum conn_state
    u8   protocol_version;           // 4 or 5
    u16  keepalive;                  // Keepalive interval (seconds)
    u32  recv_buf_idx;               // Receive buffer index
    u16  recv_offset;                // Parse position
    u16  recv_len;                   // Valid bytes in buffer
    u32  last_active;                // Last activity timestamp
    u32  pending_sends;              // Outstanding sends
    u8   flags;                      // enum conn_flags
    u8   _pad1[27];                  // Pad to 64 bytes
    
    // === Session data (cache line 2) ===
    u32  client_id_hash;             // FNV-1a hash
    u16  client_id_len;
    u8   will_flag;
    u8   will_qos;
    u32  will_topic_idx;             // Topic trie index
    u32  will_msg_idx;               // Buffer index
    u16  will_msg_len;
    u8   will_retain;
    u8   _pad2[45];                  // Pad to 64 bytes
    
    // === Variable data ===
    u8   client_id[64];              // Inline client ID
};

_Static_assert(sizeof(struct conn_slot) == 192, "conn_slot size");

struct conn_manager {
    struct conn_slot slots[MAX_CONNS];
    u32 active_count;
    u32 high_water_mark;
};
```

## B.9 Topic Trie

```c
// broker/trie.h

struct topic_node {
    // === Subscriber tracking ===
    _Atomic u32 sub_count;           // Number of subscribers
    u64 fd_bitmap[16];               // 1024 subscriber bits
    
    // === Trie structure ===
    u32 parent;                      // Parent node index
    u32 children[MAX_CHILDREN];      // Child indices
    u8  child_keys[MAX_CHILDREN];    // First byte of child names
    u16 child_count;
    
    // === Topic segment ===
    u16 name_len;
    u8  name[MAX_SEGMENT_LEN];
    
    // === Flags ===
    u8  is_wildcard_plus;            // '+' wildcard
    u8  is_wildcard_hash;            // '#' wildcard
    u8  _pad[2];
};

struct topic_trie {
    struct topic_node nodes[MAX_TOPICS];
    u32 free_head;
    u32 node_count;
    u32 root;                        // Always 0
};
```

## B.10 Inflight Message Tracking

```c
// broker/inflight.h

enum inflight_state {
    INFLIGHT_FREE        = 0,    // Slot available
    INFLIGHT_WAIT_PUBACK = 1,    // QoS 1: awaiting PUBACK
    INFLIGHT_WAIT_PUBREC = 2,    // QoS 2: awaiting PUBREC
    INFLIGHT_WAIT_PUBREL = 3,    // QoS 2: awaiting PUBREL (receiver)
    INFLIGHT_WAIT_PUBCOMP = 4,   // QoS 2: awaiting PUBCOMP
};

struct inflight_msg {
    u16 packet_id;               // MQTT packet identifier (1-65535)
    u8  state;                   // enum inflight_state
    u8  qos;                     // QoS level (1 or 2)
    u8  dup_count;               // Retransmission attempts
    u8  direction;               // 0 = outgoing, 1 = incoming
    u8  _pad[2];
    u32 buf_idx;                 // Buffer index (for stored message)
    u32 msg_offset;              // Offset within buffer
    u32 msg_len;                 // Message length
    u32 timestamp;               // Creation time (for timeout)
};

_Static_assert(sizeof(struct inflight_msg) == 24, "inflight_msg size");

#define MAX_INFLIGHT_PER_CONN 16

struct conn_inflight {
    struct inflight_msg msgs[MAX_INFLIGHT_PER_CONN];
    u16 next_packet_id;          // Next ID to try (wraps 1-65535)
    u8  count;                   // Active entries
    u8  _pad;
};

_Static_assert(sizeof(struct conn_inflight) == 388, "conn_inflight size");
```

## B.11 Session Storage

### B.11.1 MQTT 3.1.1 Sessions

```c
// broker/session.h

struct session {
    // === Identification ===
    u32 client_id_hash;          // FNV-1a hash for fast lookup
    u16 client_id_len;
    u8  exists;                  // 0 = no stored session, 1 = has session

    // === Subscriptions (persisted when clean_session=0) ===
    u32 sub_count;
    u32 sub_topics[64];          // Topic trie indices
    u8  sub_qos[64];             // Max QoS per subscription

    // === Inflight state (QoS 1/2 pending delivery) ===
    struct conn_inflight inflight;

    // === Client ID ===
    u8  client_id[MAX_CLIENT_ID_LEN];
};

#define MAX_SESSIONS 4096

struct session_store {
    struct session sessions[MAX_SESSIONS];
    u32 free_head;
    u32 count;
    u32 next[MAX_SESSIONS];      // Freelist
};
```

### B.11.2 MQTT 5.0 Session Extensions (Optional)

```c
// Additional fields for MQTT 5.0 session expiry

enum session_state {
    SESSION_NONE     = 0,        // No session
    SESSION_ACTIVE   = 1,        // Connected
    SESSION_DORMANT  = 2,        // Disconnected, retained
    SESSION_EXPIRED  = 3,        // Pending cleanup
};

struct session_v5 {
    struct session base;         // Inherit 3.1.1 fields

    u8  state;                   // enum session_state
    u32 expiry_interval;         // 0 = immediate, 0xFFFFFFFF = never
    u32 disconnect_time;         // Timestamp of disconnect
};
```

## B.12 MQTT Structures

```c
// mqtt/proto.h

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
    MQTT_AUTH        = 15,
};

struct mqtt_header {
    u8  packet_type;
    u8  flags;
    u32 remaining_length;
    u8  header_len;
};

struct mqtt_string {
    const u8 *data;                  // Pointer into buffer (no copy)
    u16 len;
};

struct mqtt_connect {
    u8  protocol_version;
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

struct mqtt_publish {
    u8  qos;
    u8  retain;
    u8  dup;
    const u8 *topic;                 // Pointer into buffer
    u16 topic_len;
    u16 packet_id;
    const u8 *payload;               // Pointer into buffer
    u32 payload_len;
    u32 packet_offset;               // Offset in buffer
    u32 packet_len;                  // Total packet length
};

struct mqtt_subscription {
    struct mqtt_string topic_filter;
    u8 qos;
    u8 no_local;
    u8 retain_as_published;
    u8 retain_handling;
};

struct mqtt_subscribe {
    u16 packet_id;
    u32 sub_count;
    struct mqtt_subscription subs[32];
};
```

## B.13 Broker State

```c
// broker/broker.h

struct broker {
    // Event loop
    struct ring ring;
    int listen_fd;
    volatile int running;

    // Memory pools
    struct buffer_pool buffers;
    struct msg_ref_pool refs;

    // Connection management
    struct conn_manager conns;

    // Session storage (for clean_session=0 persistence)
    struct session_store sessions;

    // Subscription index
    struct topic_trie trie;

    // Configuration
    struct {
        u16 port;
        u32 max_conns;
        u32 keepalive_default;
        u8  allow_anonymous;
    } config;

    // Statistics
    struct {
        u64 msgs_received;
        u64 msgs_sent;
        u64 bytes_received;
        u64 bytes_sent;
        u64 connects;
        u64 disconnects;
    } stats;
};
```

## B.14 user_data Encoding

```c
// broker/userdata.h

// Layout: [8-bit op][24-bit fd][32-bit context]
#define MAKE_USER_DATA(op, fd, ctx) \
    (((u64)(op) << 56) | ((u64)((fd) & 0xFFFFFF) << 32) | (u64)(ctx))

#define USER_DATA_OP(ud)  ((u8)((ud) >> 56))
#define USER_DATA_FD(ud)  ((u32)(((ud) >> 32) & 0xFFFFFF))
#define USER_DATA_CTX(ud) ((u32)((ud) & 0xFFFFFFFF))

enum op_type {
    OP_ACCEPT = 1,
    OP_RECV   = 2,
    OP_SEND   = 3,
    OP_CLOSE  = 4,
};
```
