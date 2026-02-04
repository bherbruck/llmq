// util/configlib.h - Generic config framework (like test/test.h)
//
// Usage:
//   1. Define BROKER_FIELDS macro with FIELD(section, name, type, default, cli, desc)
//   2. Use CONFIG_STRUCT(broker_config, BROKER_FIELDS) to generate struct
//   3. Use CONFIG_LOADER(broker, BROKER_FIELDS, "broker") to generate loader
//
// Load order: defaults → INI file → env vars → CLI args (each overrides previous)

#ifndef UTIL_CONFIGLIB_H
#define UTIL_CONFIGLIB_H

#include "sys/types.h"
#include "sys/syscall.h"

// =============================================================================
// String Helpers
// =============================================================================

static inline bool _cl_streq(const char *a, const char *b) {
    while (*a && *b)
        if (*a++ != *b++)
            return false;
    return *a == *b;
}

static inline u32 _cl_strlen(const char *s) {
    u32 n = 0;
    while (*s++)
        n++;
    return n;
}

static inline void _cl_print(const char *s) {
    sys_write(2, s, _cl_strlen(s));
}

static inline u64 _cl_parse(const char *s) {
    u64 v = 0;
    while (*s >= '0' && *s <= '9')
        v = v * 10 + (u64)(*s++ - '0'); // NOLINT
    return v;
}

// Match PREFIX_NAME in envp (case-insensitive)
static inline const char *_cl_getenv(char **envp, const char *prefix, const char *name) {
    if (!envp)
        return (void *)0;
    for (char **e = envp; *e; e++) {
        const char *s = *e;
        const char *p = prefix;
        while (*p && *s && *s != '=') {
            char pc = *p;
            char sc = *s;
            if (pc >= 'a' && pc <= 'z')
                pc -= 32; // NOLINT
            if (sc >= 'a' && sc <= 'z')
                sc -= 32; // NOLINT
            if (pc != sc)
                break;
            p++;
            s++;
        }
        if (*p != '\0')
            continue;
        if (*s != '_')
            continue;
        s++;
        const char *n = name;
        while (*n && *s && *s != '=') {
            char nc = *n;
            char sc = *s;
            if (nc >= 'a' && nc <= 'z')
                nc -= 32; // NOLINT
            if (sc >= 'a' && sc <= 'z')
                sc -= 32; // NOLINT
            if (nc != sc)
                break;
            n++;
            s++;
        }
        if (*n == '\0' && *s == '=')
            return s + 1;
    }
    return (void *)0;
}

// =============================================================================
// INI File Parser
// =============================================================================

// Skip whitespace, return pointer to first non-whitespace
static inline const char *_cl_skip_ws(const char *s) {
    while (*s == ' ' || *s == '\t')
        s++;
    return s;
}

// Copy string until whitespace/newline/comment, return length
static inline u32 _cl_copy_token(char *dst, const char *src, u32 max) {
    u32 i = 0;
    while (i < max - 1 && src[i] && src[i] != ' ' && src[i] != '\t' && src[i] != '\n' &&
           src[i] != '\r' && src[i] != '#' && src[i] != ';') {
        dst[i] = src[i];
        i++;
    }
    dst[i] = '\0';
    return i;
}

// Case-insensitive string compare
static inline bool _cl_streq_nocase(const char *a, const char *b) {
    while (*a && *b) {
        char ac = *a;
        char bc = *b;
        if (ac >= 'A' && ac <= 'Z')
            ac += 32; // NOLINT
        if (bc >= 'A' && bc <= 'Z')
            bc += 32; // NOLINT
        if (ac != bc)
            return false;
        a++;
        b++;
    }
    return *a == *b;
}

// INI line types
#define _CL_INI_EMPTY   0
#define _CL_INI_SECTION 1
#define _CL_INI_VALUE   2

// Parse a single INI line, returns type and fills section/name/value
static inline i32 _cl_parse_ini_line(const char *line, char *section, char *name, char *value) {
    const char *s = _cl_skip_ws(line);

    // Empty or comment
    if (*s == '\0' || *s == '\n' || *s == '\r' || *s == '#' || *s == ';')
        return _CL_INI_EMPTY;

    // Section header [section]
    if (*s == '[') {
        s++;
        u32 i = 0;
        while (s[i] && s[i] != ']' && s[i] != '\n' && i < 63) { // NOLINT
            section[i] = s[i];
            i++;
        }
        section[i] = '\0';
        return _CL_INI_SECTION;
    }

    // name = value
    u32 nlen = _cl_copy_token(name, s, 64); // NOLINT
    if (nlen == 0)
        return _CL_INI_EMPTY;

    s += nlen;
    s = _cl_skip_ws(s);
    if (*s != '=')
        return _CL_INI_EMPTY;
    s++;
    s = _cl_skip_ws(s);

    _cl_copy_token(value, s, 64); // NOLINT
    return _CL_INI_VALUE;
}

// =============================================================================
// CONFIG_STRUCT Macro - Generates struct from FIELDS
// =============================================================================

#define CONFIG_STRUCT(name, fields) \
    struct name {                   \
        fields(CONFIG_STRUCT_FIELD) \
    }

#define CONFIG_STRUCT_FIELD(sec, name, type, def, cli, desc) type sec##_##name;

// =============================================================================
// CONFIG_LOADER Macro - Generates loader function from FIELDS
// =============================================================================

#define CONFIG_LOADER(name, fields, prefix)                                                      \
    static inline i32 name##_config_load(struct name##_config *cfg, i32 argc, char **argv,       \
                                         char **envp) {                                          \
        const char *_prefix = prefix;                                                            \
        /* 1. Set defaults */                                                                    \
        fields(CONFIG_DEFAULT_FIELD)                                                             \
                                                                                                 \
            /* 2. Load from INI file if specified */                                             \
            const char *ini_path = NULL;                                                         \
        /* Check env var first */                                                                \
        ini_path = _cl_getenv(envp, _prefix, "CONFIG");                                          \
        /* Check CLI for --config */                                                             \
        for (i32 _i = 1; _i < argc - 1; _i++) {                                                  \
            if (_cl_streq(argv[_i], "--config")) {                                               \
                ini_path = argv[_i + 1];                                                         \
                break;                                                                           \
            }                                                                                    \
        }                                                                                        \
        if (ini_path) {                                                                          \
            i32 fd = sys_open(ini_path, O_RDONLY, 0);                                            \
            if (fd >= 0) {                                                                       \
                char buf[4096];                                                                  \
                isize n = sys_read(fd, buf, sizeof(buf) - 1);                                    \
                sys_close(fd);                                                                   \
                if (n > 0) {                                                                     \
                    buf[n]                 = '\0';                                               \
                    char cur_section[64]   = "";                                                 \
                    char line_name[64]     = "";                                                 \
                    char line_value[64]    = "";                                                 \
                    char section_tmp[64]   = "";                                                 \
                    const char *line_start = buf;                                                \
                    while (*line_start) {                                                        \
                        /* Find end of line */                                                   \
                        const char *line_end = line_start;                                       \
                        while (*line_end && *line_end != '\n')                                   \
                            line_end++;                                                          \
                        /* Copy line to temp buffer */                                           \
                        char line[256];                                                          \
                        u32 ll = 0;                                                              \
                        while (line_start + ll < line_end && ll < 255) {                         \
                            line[ll] = line_start[ll];                                           \
                            ll++;                                                                \
                        }                                                                        \
                        line[ll] = '\0';                                                         \
                        i32 lt   = _cl_parse_ini_line(line, section_tmp, line_name, line_value); \
                        if (lt == _CL_INI_SECTION) {                                             \
                            u32 si = 0;                                                          \
                            while (section_tmp[si] && si < 63) {                                 \
                                cur_section[si] = section_tmp[si];                               \
                                si++;                                                            \
                            }                                                                    \
                            cur_section[si] = '\0';                                              \
                        } else if (lt == _CL_INI_VALUE) {                                        \
                            /* Convert name: replace - with _ */                                 \
                            char norm_name[64];                                                  \
                            u32 ni = 0;                                                          \
                            while (line_name[ni] && ni < 63) {                                   \
                                norm_name[ni] = (line_name[ni] == '-') ? '_' : line_name[ni];    \
                                ni++;                                                            \
                            }                                                                    \
                            norm_name[ni] = '\0';                                                \
                            fields(CONFIG_INI_MATCH)                                             \
                        }                                                                        \
                        line_start = (*line_end) ? line_end + 1 : line_end;                      \
                    }                                                                            \
                }                                                                                \
            }                                                                                    \
        }                                                                                        \
                                                                                                 \
        /* 3. Environment variables (PREFIX_SECTION_NAME) */                                     \
        fields(CONFIG_ENV_FIELD)                                                                 \
                                                                                                 \
            /* 4. CLI arguments */                                                               \
            for (i32 _i = 1; _i < argc; _i++) {                                                  \
            const char *_a = argv[_i];                                                           \
            if (!_a)                                                                             \
                continue;                                                                        \
            if (_cl_streq(_a, "--help") || _cl_streq(_a, "-h")) {                                \
                _cl_print("Usage: " prefix " [OPTIONS]\n\n");                                    \
                _cl_print("Options:\n");                                                         \
                fields(CONFIG_HELP_FIELD)                                                        \
                    _cl_print("  --config FILE\t\tLoad config from INI file\n");                 \
                _cl_print("  --help\t\tShow this help\n");                                       \
                _cl_print("\nEnvironment: " prefix "_<SECTION>_<NAME> (uppercase)\n");           \
                return 1;                                                                        \
            }                                                                                    \
            if (_cl_streq(_a, "--config")) {                                                     \
                _i++; /* Skip the config file path */                                            \
                continue;                                                                        \
            }                                                                                    \
            i32 _matched = 0;                                                                    \
            fields(CONFIG_CLI_FIELD) if (!_matched && _a[0] == '-') {                              \
                _cl_print("Unknown option: ");                                                   \
                _cl_print(_a);                                                                   \
                _cl_print("\nUse --help for usage\n");                                           \
                return -1;                                                                       \
            }                                                                                    \
        }                                                                                        \
        (void)_prefix;                                                                           \
        return 0;                                                                                \
    }

// Helper macros used by CONFIG_LOADER
#define CONFIG_DEFAULT_FIELD(sec, name, type, def, cli, desc) cfg->sec##_##name = (type)(def);

#define CONFIG_ENV_FIELD(sec, name, type, def, cli, desc)           \
    {                                                               \
        const char *_v = _cl_getenv(envp, _prefix, #sec "_" #name); \
        if (_v)                                                     \
            cfg->sec##_##name = (type)_cl_parse(_v);                \
    }

#define CONFIG_HELP_FIELD(sec, name, type, def, cli, desc) _cl_print("  --" cli "\t\t" desc "\n");

#define CONFIG_CLI_FIELD(sec, name, type, def, cli, desc) \
    if (_cl_streq(_a, "--" cli)) {                        \
        if (++_i >= argc)                                 \
            return -1;                                    \
        cfg->sec##_##name = (type)_cl_parse(argv[_i]);    \
        _matched          = 1;                            \
    } else

#define CONFIG_INI_MATCH(sec, name, type, def, cli, desc)                            \
    if (_cl_streq_nocase(cur_section, #sec) && _cl_streq_nocase(norm_name, #name)) { \
        cfg->sec##_##name = (type)_cl_parse(line_value);                             \
    }

#endif // UTIL_CONFIGLIB_H
