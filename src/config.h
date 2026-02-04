// config.h - Compile-time constants and limits
// Runtime config is in util/broker.config (the source of truth)
//
// These are MAXIMUM capacities - runtime config can use less but not more.

#ifndef CONFIG_H
#define CONFIG_H

// =============================================================================
// Compile-Time Capacity Limits (used for array sizes)
// =============================================================================

#ifndef LLMQ_MAX_CLIENTS
#define LLMQ_MAX_CLIENTS 4096 // Max array size, runtime can be less
#endif

#ifndef LLMQ_MAX_FDS
#define LLMQ_MAX_FDS 8192 // Should be >= 2 * LLMQ_MAX_CLIENTS
#endif

#ifndef LLMQ_LISTEN_BACKLOG
#define LLMQ_LISTEN_BACKLOG 128 // listen() backlog
#endif

// =============================================================================
// Compile-Time Buffer Sizes (affect struct layout)
// =============================================================================

#ifndef LLMQ_CLIENT_ID_MAX
#define LLMQ_CLIENT_ID_MAX 64 // Max client ID length
#endif

// Per-client receive buffer (MQTT allows up to 256MB, we cap at 64KB)
#ifndef LLMQ_RECV_BUF_SIZE
#define LLMQ_RECV_BUF_SIZE 65536
#endif

// Inflight message limits (for QoS tracking)
// Memory per client = MAX_INFLIGHT * sizeof(inflight_msg) = 1024 * 16 = 16KB
// With 4096 clients: 4096 * 16KB = 64MB total
#ifndef LLMQ_MAX_INFLIGHT
#define LLMQ_MAX_INFLIGHT 1024 // Max capacity for inflight array (runtime can use less)
#endif

#ifndef LLMQ_SEND_BUF_SIZE
#define LLMQ_SEND_BUF_SIZE 4096 // 4KB per-inflight send buffer
#endif

#ifndef LLMQ_PENDING_MSG_DATA
#define LLMQ_PENDING_MSG_DATA 1024 // 1KB per pending message (offline queue)
#endif

#ifndef LLMQ_MAX_PENDING_MSGS
#define LLMQ_MAX_PENDING_MSGS 32 // Offline message queue depth
#endif

#ifndef LLMQ_MAX_CLIENT_SUBS
#define LLMQ_MAX_CLIENT_SUBS 64 // Subscriptions per client
#endif

#ifndef LLMQ_LOG_BUF_SIZE
#define LLMQ_LOG_BUF_SIZE 512
#endif

// =============================================================================
// Topic Trie (compile-time, affects struct layout)
// =============================================================================

#ifndef LLMQ_TRIE_MAX_NODES
#define LLMQ_TRIE_MAX_NODES 1024
#endif

#ifndef LLMQ_TRIE_MAX_CHILDREN
#define LLMQ_TRIE_MAX_CHILDREN 32
#endif

#ifndef LLMQ_TRIE_MAX_SEGMENT
#define LLMQ_TRIE_MAX_SEGMENT 64
#endif

#ifndef LLMQ_MAX_TOPIC_FILTERS
#define LLMQ_MAX_TOPIC_FILTERS 8
#endif

// =============================================================================
// Validation Limits
// =============================================================================

#define LLMQ_MAX_CONNS_LIMIT  65536
#define LLMQ_MIN_RING_ENTRIES 16
#define LLMQ_MAX_RING_ENTRIES 32768
#define LLMQ_MAX_LOG_LEVEL    4

// =============================================================================
// Event Loop Tuning
// =============================================================================

#define LLMQ_FLUSH_BATCH_LIMIT 256 // Max CQEs to process when flushing for SQ space

// =============================================================================
// Numeric Constants
// =============================================================================

#define FD_STDIN  0
#define FD_STDOUT 1
#define FD_STDERR 2

#define FD_MULTIPLIER      2 // max_fds = max_clients * FD_MULTIPLIER
#define BITS_PER_BYTE      8
#define BYTE_MASK          0xFF
#define KB_DIVISOR         1024
#define MB_DIVISOR         (1024ULL * 1024)
#define PERCENT_MULTIPLIER 100

#define DECIMAL_BASE     10
#define HEX_BASE         16
#define HEX_NIBBLE_MASK  0xF
#define HEX_NIBBLE_SHIFT 4
#define I64_MAX_DIGITS   24
#define U64_HEX_DIGITS   16

#define MQTT_SUBACK_FAILURE   0x80
#define SOCKADDR_PADDING_SIZE 8

#endif // CONFIG_H
