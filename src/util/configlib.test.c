// Test the configlib include pattern

#include "test/test.h"
#include "util/configlib.h"

// =============================================================================
// Test config definition (in a .def file in real usage)
// =============================================================================

#define TEST_FIELDS                                                         \
    FIELD(network, port, u16, 1883, "port", "Listen port")                  \
    FIELD(network, backlog, u16, 128, "backlog", "Connection backlog")      \
    FIELD(limits, max_clients, u32, 1024, "max-clients", "Max connections") \
    FIELD(debug, log_level, u8, 2, "log-level", "Verbosity 0-4")

// Generate struct
struct test_config {
#define FIELD(sec, name, type, def, cli, desc) type sec##_##name;
    TEST_FIELDS
#undef FIELD
};

// Generate loader
static inline i32 test_config_load(struct test_config *cfg, i32 argc, char **argv, char **envp) {
    // Defaults
#define FIELD(sec, name, type, def, cli, desc) cfg->sec##_##name = (type)(def);
    TEST_FIELDS
#undef FIELD

    // Env vars
#define FIELD(sec, name, type, def, cli, desc)                     \
    {                                                              \
        const char *_v = _cl_getenv(envp, "test", #sec "_" #name); \
        if (_v)                                                    \
            cfg->sec##_##name = (type)_cl_parse(_v);               \
    }
    TEST_FIELDS
#undef FIELD

    // CLI args
    for (i32 i = 1; i < argc; i++) {
        const char *_a = argv[i];
        if (_cl_streq(_a, "--help"))
            return 1;
        i32 _m = 0;
#define FIELD(sec, name, type, def, cli, desc)        \
    if (_cl_streq(_a, "--" cli)) {                    \
        if (++i >= argc)                              \
            return -1;                                \
        cfg->sec##_##name = (type)_cl_parse(argv[i]); \
        _m                = 1;                        \
    } else
        TEST_FIELDS
#undef FIELD
        {
            (void)0;
        }
        if (!_m && _a[0] == '-')
            return -1;
    }
    return 0;
}

// =============================================================================
// Tests
// =============================================================================

TEST(config_defaults) {
    struct test_config cfg;
    char *argv[] = {"test"};
    test_config_load(&cfg, 1, argv, (void *)0);

    ASSERT_EQ(cfg.network_port, 1883);
    ASSERT_EQ(cfg.network_backlog, 128);
    ASSERT_EQ(cfg.limits_max_clients, 1024);
    ASSERT_EQ(cfg.debug_log_level, 2);
}

TEST(config_args) {
    struct test_config cfg;
    char *argv[] = {"test", "--port", "9999", "--max-clients", "500"};
    i32 argc     = 5; // NOLINT
    test_config_load(&cfg, argc, argv, (void *)0);

    ASSERT_EQ(cfg.network_port, 9999);
    ASSERT_EQ(cfg.limits_max_clients, 500);
    ASSERT_EQ(cfg.network_backlog, 128); // unchanged
    ASSERT_EQ(cfg.debug_log_level, 2);   // unchanged
}

TEST(config_env) {
    struct test_config cfg;
    char *argv[] = {"test"};
    char *envp[] = {"TEST_NETWORK_PORT=4444", "TEST_DEBUG_LOG_LEVEL=3", (void *)0};
    test_config_load(&cfg, 1, argv, envp);

    ASSERT_EQ(cfg.network_port, 4444);
    ASSERT_EQ(cfg.debug_log_level, 3);
    ASSERT_EQ(cfg.network_backlog, 128); // unchanged
}

TEST(config_args_override_env) {
    struct test_config cfg;
    char *argv[] = {"test", "--port", "5555"};
    char *envp[] = {"TEST_NETWORK_PORT=4444", (void *)0};
    test_config_load(&cfg, 3, argv, envp);

    ASSERT_EQ(cfg.network_port, 5555); // args win
}
