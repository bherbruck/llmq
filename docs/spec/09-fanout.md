# Section 9: Message Fan-out

## 9.1 Overview

Fan-out is the process of distributing a published message to all matching subscribers. This is the hot path that MUST be zero-copy and zero-allocation.

## 9.2 Fan-out Architecture

```
        PUBLISH received in buffer[N]
                    │
                    ▼
        ┌───────────────────────┐
        │   Parse in-place      │  No copy: just compute offsets
        │   (topic, payload)    │
        └───────────────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │   Match topic in      │  Walk trie, count matching subs
        │   subscription trie   │
        └───────────────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │   Allocate msg_ref    │  From fixed pool, set refcount = N
        │   refcount = N subs   │
        └───────────────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │   Submit SEND_ZC to   │  All reference same buffer[N]
        │   each subscriber     │
        └───────────────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │   io_uring_enter()    │  Kernel handles all sends
        └───────────────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │   CQEs arrive         │  Each decrements refcount
        │   (possibly async)    │
        └───────────────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │   refcount == 0       │  Return buffer to pool
        │   Free buffer & ref   │
        └───────────────────────┘
```

## 9.3 Implementation

### 9.3.1 Fan-out Entry Point

```c
void fanout_publish(struct broker *b, u32 src_fd, u32 buf_idx,
                    struct mqtt_publish *pub) {
    // Phase 1: Count subscribers
    u32 sub_count = 0;
    struct fanout_ctx ctx = {
        .broker = b,
        .total = &sub_count,
        .phase = PHASE_COUNT,
    };
    
    trie_match(&b->trie, pub->topic, pub->topic_len, fanout_callback, &ctx);
    
    if (sub_count == 0) {
        // No subscribers, nothing to do
        // Buffer stays with connection for next receive
        return;
    }
    
    // Phase 2: Allocate message reference
    u32 ref_idx = ref_alloc(&b->refs);
    if (ref_idx == 0) {
        // Out of refs, drop message (apply backpressure)
        return;
    }
    
    struct msg_ref *ref = &b->refs.refs[ref_idx];
    ref->buf_idx = buf_idx;
    ref->offset = pub->packet_offset;
    ref->len = pub->packet_len;
    atomic_store(&ref->refcount, sub_count);
    
    // Phase 3: Submit sends
    ctx.phase = PHASE_SEND;
    ctx.ref_idx = ref_idx;
    ctx.sent = 0;
    
    trie_match(&b->trie, pub->topic, pub->topic_len, fanout_callback, &ctx);
    
    // Verify we sent to expected count
    // (May differ if subscriber disconnected between phases)
    if (ctx.sent < sub_count) {
        // Adjust refcount down
        u32 diff = sub_count - ctx.sent;
        if (atomic_sub(&ref->refcount, diff) == diff) {
            // Refcount hit zero, free immediately
            ref_free(&b->refs, ref_idx);
            buf_free(&b->buffers, buf_idx);
        }
    }
}
```

### 9.3.2 Fan-out Callback

```c
enum fanout_phase {
    PHASE_COUNT,
    PHASE_SEND,
};

struct fanout_ctx {
    struct broker *broker;
    u32 *total;
    enum fanout_phase phase;
    u32 ref_idx;
    u32 sent;
};

static void fanout_callback(void *user, struct topic_node *node) {
    struct fanout_ctx *ctx = user;
    
    if (ctx->phase == PHASE_COUNT) {
        *ctx->total += atomic_load(&node->sub_count);
        return;
    }
    
    // PHASE_SEND: submit sends to each subscriber
    struct broker *b = ctx->broker;
    struct msg_ref *ref = &b->refs.refs[ctx->ref_idx];
    
    for (u32 slot = 0; slot < 16; slot++) {
        u64 bits = node->fd_bitmap[slot];
        
        while (bits) {
            u32 bit_idx = __builtin_ctzll(bits);
            u32 fd = slot * 64 + bit_idx;
            bits &= bits - 1;
            
            struct conn_slot *conn = &b->conns.slots[fd];
            
            // Skip if connection can't receive (backpressure)
            if (!conn_can_receive(conn)) {
                continue;
            }
            
            // Submit zero-copy send
            struct io_uring_sqe *sqe = ring_get_sqe(&b->ring);
            if (sqe == NULL) {
                // SQ full, will need to flush and retry
                continue;
            }
            
            sqe->opcode = IORING_OP_SEND_ZC;
            sqe->fd = fd;
            sqe->flags = IOSQE_FIXED_FILE;
            sqe->addr = (u64)(buf_ptr(&b->buffers, ref->buf_idx) + ref->offset);
            sqe->len = ref->len;
            sqe->user_data = MAKE_USER_DATA(OP_SEND, fd, ctx->ref_idx);
            
            conn->pending_sends++;
            ctx->sent++;
        }
    }
}
```

### 9.3.3 Send Completion Handler

```c
void handle_send_complete(struct broker *b, struct io_uring_cqe *cqe) {
    u32 fd = USER_DATA_FD(cqe->user_data);
    u32 ref_idx = USER_DATA_CTX(cqe->user_data);
    
    // Handle connection bookkeeping
    struct conn_slot *conn = &b->conns.slots[fd];
    conn->pending_sends--;
    
    if (cqe->res < 0) {
        // Send failed, connection is dead
        conn_disconnect(b, conn);
    }
    
    if (conn->state == CONN_DRAINING && conn->pending_sends == 0) {
        conn_free(b, conn);
    }
    
    // Handle zero-copy notification
    // SEND_ZC may produce two CQEs - only decrement on final one
    if (cqe->flags & IORING_CQE_F_MORE) {
        // More CQEs coming for this operation
        return;
    }
    
    // Decrement refcount
    struct msg_ref *ref = &b->refs.refs[ref_idx];
    u32 old = atomic_sub(&ref->refcount, 1);
    
    if (old == 1) {
        // We were the last reference
        buf_free(&b->buffers, ref->buf_idx);
        ref_free(&b->refs, ref_idx);
    }
}
```

## 9.4 Zero-Copy Mechanics

### 9.4.1 How SEND_ZC Works

1. Userspace submits SEND_ZC with buffer address
2. Kernel pins the page containing the buffer
3. Kernel DMAs directly from the page to NIC
4. Kernel posts CQE when transmission complete
5. Userspace can now reuse the buffer

The buffer MUST NOT be modified or freed until the CQE arrives.

### 9.4.2 Notification Modes

SEND_ZC may operate in two modes:

**Async notification (default):**
- First CQE: Send submitted (may have `IORING_CQE_F_MORE`)
- Second CQE: Buffer released (no `IORING_CQE_F_MORE`)

**Sync notification (IORING_SEND_ZC_REPORT_USAGE):**
- Single CQE after buffer released
- Simpler but higher latency

The broker uses async mode and tracks the `IORING_CQE_F_MORE` flag.

### 9.4.3 Fallback to Regular Send

If zero-copy is not available (older kernel, unsupported NIC), fall back to regular send:

```c
void submit_send(struct ring *r, int fd, u32 buf_idx, u32 offset, u32 len, u32 ctx) {
    struct io_uring_sqe *sqe = ring_get_sqe(r);
    
    #ifdef HAVE_SEND_ZC
    sqe->opcode = IORING_OP_SEND_ZC;
    #else
    sqe->opcode = IORING_OP_SEND;
    #endif
    
    sqe->fd = fd;
    sqe->flags = IOSQE_FIXED_FILE;
    sqe->addr = (u64)(buf_ptr(&pool, buf_idx) + offset);
    sqe->len = len;
    sqe->user_data = MAKE_USER_DATA(OP_SEND, fd, ctx);
}
```

## 9.5 Backpressure

### 9.5.1 Per-Connection Limits

Each connection has a maximum pending sends limit:

```c
#define MAX_PENDING_SENDS 256

int conn_can_receive(struct conn_slot *conn) {
    return conn->state == CONN_CONNECTED &&
           conn->pending_sends < MAX_PENDING_SENDS;
}
```

Slow subscribers are skipped during fan-out, effectively dropping messages for them.

### 9.5.2 Global Limits

If the message ref pool is exhausted, new publishes are dropped:

```c
if (ref_alloc(&b->refs) == 0) {
    // Apply backpressure - drop message or disconnect publisher
    return;
}
```

### 9.5.3 Buffer Pool Exhaustion

If all buffers are in use, new receives cannot be submitted. The broker continues processing completions until buffers are freed.

## 9.6 QoS Considerations

### 9.6.1 QoS 0 (At Most Once)

- Fire and forget
- No ACK tracking
- Message may be lost if subscriber disconnects

### 9.6.2 QoS 1 (At Least Once)

QoS 1 requires per-subscriber packet IDs, but we can still achieve **partial zero-copy** using scatter-gather I/O.

#### 9.6.2.1 Surgical Packet Mapping

When parsing an inbound QoS 1 PUBLISH, extract offsets to enable surgical reconstruction:

```c
struct mqtt_publish_parts {
    // Fixed header (needs rewrite for packet ID)
    u8  type_flags;              // Byte 0: type + DUP/QoS/RETAIN
    u8  remaining_len_bytes;     // 1-4 bytes for remaining length
    u32 remaining_length;        // Decoded value

    // Variable header - topic (zero-copy reference)
    u32 topic_offset;            // Offset to topic length MSB
    u16 topic_len;               // Topic string length

    // Variable header - packet ID location (2 bytes after topic)
    u32 packet_id_offset;        // Where to insert/modify packet ID

    // Payload (zero-copy reference)
    u32 payload_offset;          // Offset to first payload byte
    u32 payload_len;
};
```

#### 9.6.2.2 Scatter-Gather Send

Use `IORING_OP_WRITEV` or `IORING_OP_SEND_ZC` with `msg_hdr` for scatter-gather:

```
┌─────────────────────────────────────────────────────────────────┐
│                    INBOUND BUFFER (original)                     │
├─────────┬───────────────┬───────────┬───────────────────────────┤
│ Fixed   │    Topic      │ Packet ID │        Payload            │
│ Header  │  (len+str)    │ (ignore)  │     (zero-copy)           │
└─────────┴───────────────┴───────────┴───────────────────────────┘
     │            │                              │
     │            │   Outbound uses 3 iovecs:    │
     ▼            ▼                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ iov[0]: Small    │ iov[1]: Topic from     │ iov[2]: Payload     │
│ header buf (8B)  │ original buffer        │ from original buf   │
│ [type][rem_len]  │ [len_hi][len_lo][...]  │ [...]               │
│ [pkt_id_hi]      │                        │                     │
│ [pkt_id_lo]      │                        │                     │
└─────────────────────────────────────────────────────────────────┘
       ▲
       │
  Per-subscriber: Only 8-byte header buffer allocated
  Payload: Zero-copy from original receive buffer
```

```c
struct small_header {
    u8 type_flags;               // PUBLISH + QoS + DUP + RETAIN
    u8 remaining_len[4];         // Variable byte integer (1-4 bytes)
    u8 packet_id[2];             // Per-subscriber packet ID
    u8 _pad;
};

void fanout_qos1_scatter(struct broker *b, struct mqtt_publish_parts *parts,
                         u32 buf_idx, u32 fd, u16 packet_id) {
    struct conn_slot *conn = &b->conns.slots[fd];
    u8 *buf = buf_ptr(&b->buffers, buf_idx);

    // Allocate tiny header from per-connection pool (or stack)
    struct small_header hdr;
    hdr.type_flags = 0x32;       // PUBLISH, QoS 1
    int rem_len_size = encode_vbi(hdr.remaining_len, parts->remaining_length);
    hdr.packet_id[0] = packet_id >> 8;
    hdr.packet_id[1] = packet_id & 0xFF;

    // Build iovec for scatter-gather
    struct iovec iov[3];

    // iov[0]: Our small header (fixed header + packet ID)
    iov[0].iov_base = &hdr;
    iov[0].iov_len = 1 + rem_len_size + 2;  // type + rem_len + pkt_id

    // iov[1]: Topic from original buffer (zero-copy)
    iov[1].iov_base = buf + parts->topic_offset;
    iov[1].iov_len = 2 + parts->topic_len;   // len bytes + topic

    // iov[2]: Payload from original buffer (zero-copy)
    iov[2].iov_base = buf + parts->payload_offset;
    iov[2].iov_len = parts->payload_len;

    // Submit vectored send
    submit_sendmsg_zc(&b->ring, fd, iov, 3, ref_idx);

    // Track for PUBACK
    conn_track_inflight(conn, packet_id, buf_idx, parts);
}
```

#### 9.6.2.3 Memory Efficiency

| Component | Size | Lifetime |
|-----------|------|----------|
| Original buffer | 64KB | Until all sends complete (refcounted) |
| Small header | 8 bytes | Until this send completes |
| Topic | 0 (pointer) | References original buffer |
| Payload | 0 (pointer) | References original buffer |

For 1000 subscribers, total overhead is ~8KB of header buffers, not 64MB of copied payloads.

### 9.6.3 QoS 2 (Exactly Once)

QoS 2 requires four-way handshake. The broker MUST track state per message per subscriber. This is inherently incompatible with stateless zero-copy fan-out.

## 9.7 Retained Messages

Retained messages are stored separately and delivered on subscribe:

```c
struct retained_msg {
    u32 topic_hash;
    u32 buf_idx;                     // Dedicated buffer (not in hot path pool)
    u32 offset;
    u32 len;
};

void handle_subscribe_retained(struct broker *b, struct conn_slot *conn,
                               const u8 *filter, u16 filter_len) {
    // Find matching retained messages
    for (u32 i = 0; i < b->retained_count; i++) {
        struct retained_msg *msg = &b->retained[i];
        
        if (topic_matches_filter(msg->topic, filter, filter_len)) {
            // Send retained message to new subscriber
            submit_send(&b->ring, conn->fd, msg->buf_idx, msg->offset, msg->len, 0);
        }
    }
}
```

## 9.8 Performance Characteristics

| Metric | Target | Notes |
|--------|--------|-------|
| Fan-out latency | O(subscribers) | Linear in subscriber count |
| Memory per message | 32 bytes | msg_ref only |
| Copies per message | 0 | Zero-copy send |
| Syscalls per fan-out | 1 | Batched io_uring_enter |
| Atomics per message | 1 + N | 1 store + N decrements |
