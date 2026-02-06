// config.def.h - Broker configuration definition (THE source of truth)
//
// This file defines all broker config fields using the BROKER_FIELDS macro.
// It uses configlib.h macros to generate:
//   - struct broker_config
//   - broker_config_load() function
//
// Load order: defaults → INI file → env vars → CLI args

#ifndef CONFIG_DEF_H
#define CONFIG_DEF_H

#include "config.h"
#include "util/configlib.h"

// =============================================================================
// Broker Configuration Fields
// =============================================================================
// FIELD(section, name, type, default, cli_flag, description)

#define BROKER_FIELDS(FIELD)                                                     \
    /* Network */                                                                \
    FIELD(network, port, u16, 1883, "port", "Listen port")                       \
    FIELD(network, backlog, u16, 128, "backlog", "Listen backlog")               \
    /* Limits */                                                                 \
    FIELD(limits, max_conns, u32, 4096, "max-conns", "Max connections")          \
    FIELD(limits, max_sessions, u32, 4096, "max-sessions", "Max sessions")       \
    FIELD(limits, ring_entries, u32, 8192, "ring-entries", "io_uring depth")     \
    FIELD(limits, msg_pool_size, u32, 1024, "msg-pool-size", "Fan-out msg slots")\
    FIELD(limits, send_desc_mult, u8, 16, "send-desc-mult", "Send descs per conn")\
    FIELD(limits, send_desc_max_mb, u32, 1536, "send-desc-max", "Max send_desc MB")\
    /* Per-client buffers */                                                     \
    FIELD(client, max_inflight, u16, 256, "max-inflight", "QoS inflight/client") \
    /* Debug */                                                                  \
    FIELD(debug, log_level, u8, 2, "log-level", "Verbosity 0-4")

// =============================================================================
// Generate struct broker_config
// =============================================================================

CONFIG_STRUCT(broker_config, BROKER_FIELDS);

// =============================================================================
// Generate broker_config_load()
// =============================================================================

CONFIG_LOADER(broker, BROKER_FIELDS, "broker")

// =============================================================================
// Validation wrapper
// =============================================================================

INLINE i32 broker_config_parse(i32 argc, char **argv, char **envp, struct broker_config *cfg) {
    i32 rc = broker_config_load(cfg, argc, argv, envp);
    if (rc != 0)
        return rc;

    if (cfg->network_port == 0)
        return -1;
    if (cfg->limits_max_conns == 0 || cfg->limits_max_conns > LLMQ_MAX_CONNS_LIMIT)
        return -1;
    if (cfg->limits_max_sessions == 0 || cfg->limits_max_sessions > LLMQ_MAX_CONNS_LIMIT)
        return -1;
    if (cfg->limits_ring_entries < LLMQ_MIN_RING_ENTRIES ||
        cfg->limits_ring_entries > LLMQ_MAX_RING_ENTRIES)
        return -1;
    // Per-client buffer limits must fit in compile-time array sizes
    // max_inflight must be power of 2 for fast modulo via bitmask
    if (cfg->client_max_inflight == 0 || cfg->client_max_inflight > LLMQ_MAX_INFLIGHT ||
        (cfg->client_max_inflight & (cfg->client_max_inflight - 1)) != 0)
        return -1;
    if (cfg->debug_log_level > LLMQ_MAX_LOG_LEVEL)
        return -1;

    return 0;
}

#endif // CONFIG_DEF_H
