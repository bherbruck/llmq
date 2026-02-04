// mem/msgbuf.h - Zero-copy message metadata for fan-out
// Canonical messages store metadata only - offsets/lengths pointing into stolen recv buffers
// One stolen buffer, N references via atomic refcount

#ifndef MEM_MSGBUF_H
#define MEM_MSGBUF_H

#include "sys/types.h"
#include "sys/syscall.h"
#include "config.h"

// =============================================================================
// Atomic Operations (for refcount)
// =============================================================================

typedef volatile u32 atomic_u32;

INLINE u32 atomic_load(atomic_u32 *a) {
    return __atomic_load_n(a, __ATOMIC_ACQUIRE);
}

INLINE void atomic_store(atomic_u32 *a, u32 val) {
    __atomic_store_n(a, val, __ATOMIC_RELEASE);
}

INLINE u32 atomic_inc(atomic_u32 *a) {
    return __atomic_add_fetch(a, 1, __ATOMIC_ACQ_REL);
}

INLINE u32 atomic_dec(atomic_u32 *a) {
    return __atomic_sub_fetch(a, 1, __ATOMIC_ACQ_REL);
}

// =============================================================================
// Canonical Message (metadata only, ~32 bytes)
// =============================================================================

// Canonical message: metadata pointing into stolen recv buffer
// The actual data lives in the stolen buffer, not copied here
struct canonical_msg {
    u32 buf_idx;          // Index into recv_pool (stolen buffer)
    u16 topic_off;        // Offset to topic string within buffer
    u16 topic_len;        // Topic length
    u32 payload_off;      // Offset to payload within buffer
    u32 payload_len;      // Payload length
    u8 qos;               // Publisher's QoS level
    u8 retain;            // Retain flag
    u8 dup;               // DUP flag
    u8 _pad;
    atomic_u32 ref_count; // Decremented per-subscriber completion
};

STATIC_ASSERT(sizeof(struct canonical_msg) == 24, "canonical_msg should be 24 bytes");

// =============================================================================
// Message Pool (metadata pool, not buffer pool)
// =============================================================================

// Pool configuration
#ifndef LLMQ_MSG_POOL_SIZE
#define LLMQ_MSG_POOL_SIZE 1024 // Max concurrent fan-out messages
#endif

struct msg_pool {
    struct canonical_msg *msgs; // mmap'd array of canonical messages
    u32 *free_stack;            // Stack of free message indices
    u32 capacity;               // Total number of slots
    u32 free_count;             // Number of free slots
    u32 high_water;             // Peak usage
};

#define MSG_POOL_INVALID ((u32)-1)

// Initialize message pool
INLINE i32 msg_pool_init(struct msg_pool *p, u32 capacity) {
    p->capacity   = capacity;
    p->free_count = capacity;
    p->high_water = 0;

    // mmap the message array
    usize msgs_size = (usize)capacity * sizeof(struct canonical_msg);
    p->msgs         = (struct canonical_msg *)sys_mmap(NULL, msgs_size, PROT_READ | PROT_WRITE,
                                                       MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(p->msgs)) {
        return -1;
    }

    // mmap the free stack
    usize stack_size = (usize)capacity * sizeof(u32);
    p->free_stack =
        (u32 *)sys_mmap(NULL, stack_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(p->free_stack)) {
        sys_munmap(p->msgs, msgs_size);
        return -1;
    }

    // Initialize free stack (reverse order for cache locality)
    for (u32 i = 0; i < capacity; i++) {
        p->free_stack[i] = capacity - 1 - i;
    }

    return 0;
}

// Cleanup message pool
INLINE void msg_pool_cleanup(struct msg_pool *p) {
    if (p->msgs) {
        sys_munmap(p->msgs, (usize)p->capacity * sizeof(struct canonical_msg));
        p->msgs = NULL;
    }
    if (p->free_stack) {
        sys_munmap(p->free_stack, (usize)p->capacity * sizeof(u32));
        p->free_stack = NULL;
    }
}

// Get message by index
INLINE struct canonical_msg *msg_pool_get(struct msg_pool *p, u32 idx) {
    if (idx >= p->capacity) {
        return NULL;
    }
    return &p->msgs[idx];
}

// Allocate a message slot, returns index or MSG_POOL_INVALID
INLINE u32 msg_pool_alloc(struct msg_pool *p) {
    if (p->free_count == 0) {
        return MSG_POOL_INVALID;
    }
    p->free_count--;

    // Track high water mark
    u32 used = p->capacity - p->free_count;
    if (used > p->high_water) {
        p->high_water = used;
    }

    u32 idx                  = p->free_stack[p->free_count];
    struct canonical_msg *m  = &p->msgs[idx];
    m->buf_idx               = MSG_POOL_INVALID;
    atomic_store(&m->ref_count, 0);
    return idx;
}

// Free a message slot back to pool
INLINE void msg_pool_free(struct msg_pool *p, u32 idx) {
    if (idx >= p->capacity) {
        return;
    }
    p->free_stack[p->free_count] = idx;
    p->free_count++;
}

// Increment reference count (call before each subscriber send)
INLINE void msg_pool_ref(struct msg_pool *p, u32 idx) {
    if (idx >= p->capacity) {
        return;
    }
    atomic_inc(&p->msgs[idx].ref_count);
}

// Decrement reference count
// Returns: new ref count (0 means caller should free the message and stolen buffer)
INLINE u32 msg_pool_unref(struct msg_pool *p, u32 idx) {
    if (idx >= p->capacity) {
        return 0;
    }
    return atomic_dec(&p->msgs[idx].ref_count);
}

// Get current ref count
INLINE u32 msg_pool_refcount(struct msg_pool *p, u32 idx) {
    if (idx >= p->capacity) {
        return 0;
    }
    return atomic_load(&p->msgs[idx].ref_count);
}

// =============================================================================
// Send Descriptor (ephemeral, per-send, ~64 bytes)
// =============================================================================

// Send states
enum send_state {
    SEND_FREE     = 0, // Slot available
    SEND_PENDING  = 1, // Queued for send
    SEND_INFLIGHT = 2, // io_uring send in progress
};

// Send descriptor: ephemeral per-send state
// Materializes PUBLISH header at send time
struct send_desc {
    u32 msg_idx;         // Index into canonical_msg pool
    u32 slot_idx;        // Subscriber slot index
    u16 packet_id;       // Per-subscriber packet ID (QoS 1/2)
    u8 sub_qos;          // Subscriber's QoS level
    u8 state;            // enum send_state
    u8 header[7];        // Materialized PUBLISH header (max 7 bytes)
    u8 header_len;       // Actual header length (3-7 bytes)
    u8 pkt_id_be[2];     // Big-endian packet_id for iovec
    u8 slot_gen;         // Client generation when send was submitted (stale detection)
    u8 _pad;
    u32 cursor;          // For partial send resume (bytes sent so far)
    struct iovec iov[4]; // [header][topic][packet_id][payload]
};

STATIC_ASSERT(sizeof(struct send_desc) <= 96, "send_desc should be <= 96 bytes");

// =============================================================================
// Send Descriptor Pool
// =============================================================================

#ifndef LLMQ_SEND_DESC_POOL_SIZE
#define LLMQ_SEND_DESC_POOL_SIZE 4096 // Max concurrent sends
#endif

struct send_desc_pool {
    struct send_desc *descs; // mmap'd array of send descriptors
    u32 *free_stack;         // Stack of free descriptor indices
    u32 capacity;            // Total number of slots
    u32 free_count;          // Number of free slots
    u32 high_water;          // Peak usage
};

#define SEND_DESC_INVALID ((u32)-1)

// Initialize send descriptor pool
INLINE i32 send_desc_pool_init(struct send_desc_pool *p, u32 capacity) {
    p->capacity   = capacity;
    p->free_count = capacity;
    p->high_water = 0;

    // mmap the descriptor array
    usize descs_size = (usize)capacity * sizeof(struct send_desc);
    p->descs         = (struct send_desc *)sys_mmap(NULL, descs_size, PROT_READ | PROT_WRITE,
                                                    MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(p->descs)) {
        return -1;
    }

    // mmap the free stack
    usize stack_size = (usize)capacity * sizeof(u32);
    p->free_stack =
        (u32 *)sys_mmap(NULL, stack_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (IS_ERR(p->free_stack)) {
        sys_munmap(p->descs, descs_size);
        return -1;
    }

    // Initialize free stack and descriptors
    for (u32 i = 0; i < capacity; i++) {
        p->free_stack[i]   = capacity - 1 - i;
        p->descs[i].state  = SEND_FREE;
        p->descs[i].cursor = 0;
    }

    return 0;
}

// Cleanup send descriptor pool
INLINE void send_desc_pool_cleanup(struct send_desc_pool *p) {
    if (p->descs) {
        sys_munmap(p->descs, (usize)p->capacity * sizeof(struct send_desc));
        p->descs = NULL;
    }
    if (p->free_stack) {
        sys_munmap(p->free_stack, (usize)p->capacity * sizeof(u32));
        p->free_stack = NULL;
    }
}

// Get send descriptor by index
INLINE struct send_desc *send_desc_pool_get(struct send_desc_pool *p, u32 idx) {
    if (idx >= p->capacity) {
        return NULL;
    }
    return &p->descs[idx];
}

// Allocate a send descriptor, returns index or SEND_DESC_INVALID
INLINE u32 send_desc_pool_alloc(struct send_desc_pool *p) {
    if (p->free_count == 0) {
        return SEND_DESC_INVALID;
    }
    p->free_count--;

    // Track high water mark
    u32 used = p->capacity - p->free_count;
    if (used > p->high_water) {
        p->high_water = used;
    }

    u32 idx              = p->free_stack[p->free_count];
    struct send_desc *sd = &p->descs[idx];
    sd->state            = SEND_PENDING;
    sd->cursor           = 0;
    sd->msg_idx          = MSG_POOL_INVALID;
    return idx;
}

// Free a send descriptor back to pool
INLINE void send_desc_pool_free(struct send_desc_pool *p, u32 idx) {
    if (idx >= p->capacity) {
        return;
    }
    p->descs[idx].state  = SEND_FREE;
    p->free_stack[p->free_count] = idx;
    p->free_count++;
}

// =============================================================================
// PUBLISH Header Materialization
// =============================================================================

// MQTT PUBLISH fixed header format:
// Byte 0: 0x30 | (dup << 3) | (qos << 1) | retain
// Byte 1-4: Remaining length (variable 1-4 bytes)

// Materialize PUBLISH header into send_desc
// remaining_len = topic_len_field(2) + topic_len + packet_id(0 or 2) + payload_len
// Returns: header length (3-7 bytes)
INLINE u8 materialize_publish_header(struct send_desc *sd, u16 topic_len, u32 payload_len, u8 qos,
                                     bool retain, bool dup) {
    // Calculate remaining length
    u32 remaining = 2 + topic_len + payload_len; // topic_len field + topic + payload
    if (qos > 0) {
        remaining += 2; // packet_id
    }

    // Fixed header byte 0
    u8 flags   = 0;
    if (retain) flags |= 0x01;
    flags |= (qos << 1) & 0x06;
    if (dup) flags |= 0x08;
    sd->header[0] = 0x30 | flags;

    // Encode remaining length (variable byte integer)
    u8 pos = 1;
    u32 x  = remaining;
    do {
        u8 byte = (u8)(x & 0x7F);
        x >>= 7;
        if (x > 0) {
            byte |= 0x80; // continuation bit
        }
        sd->header[pos++] = byte;
    } while (x > 0);

    // Topic length (big-endian)
    sd->header[pos++] = (u8)(topic_len >> 8);
    sd->header[pos++] = (u8)(topic_len & 0xFF);

    sd->header_len = pos;
    return pos;
}

// Setup iovec for scatter-gather send
// Layout: [header][topic][packet_id][payload]
// For QoS 0: packet_id iovec has len=0
INLINE void setup_send_iovec(struct send_desc *sd, const u8 *topic_ptr, u16 topic_len,
                             const u8 *payload_ptr, u32 payload_len) {
    // iov[0]: materialized header (includes topic length field)
    sd->iov[0].iov_base = sd->header;
    sd->iov[0].iov_len  = sd->header_len;

    // iov[1]: topic string
    sd->iov[1].iov_base = (void *)topic_ptr;
    sd->iov[1].iov_len  = topic_len;

    // iov[2]: packet_id (2 bytes for QoS > 0, 0 bytes for QoS 0)
    sd->iov[2].iov_base = sd->pkt_id_be;
    sd->iov[2].iov_len  = (sd->sub_qos > 0) ? 2 : 0;

    // iov[3]: payload
    sd->iov[3].iov_base = (void *)payload_ptr;
    sd->iov[3].iov_len  = payload_len;
}

// Calculate total bytes in iovec
INLINE u32 send_desc_total_bytes(struct send_desc *sd) {
    return (u32)(sd->iov[0].iov_len + sd->iov[1].iov_len + sd->iov[2].iov_len + sd->iov[3].iov_len);
}

#endif // MEM_MSGBUF_H
