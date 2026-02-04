// util/log.h - Zero-allocation leveled logger
// Stack-based formatting, no heap, runtime level control

#ifndef UTIL_LOG_H
#define UTIL_LOG_H

#include "sys/types.h"
#include "sys/syscall.h"
#include "config.h"

// =============================================================================
// Log Levels
// =============================================================================

enum log_level {
    LOG_TRACE = 0,
    LOG_DEBUG = 1,
    LOG_INFO  = 2,
    LOG_WARN  = 3,
    LOG_ERROR = 4,
    LOG_NONE  = 5, // Disable all logging
};

// Global log level (default: INFO)
static enum log_level g_log_level = LOG_INFO;

INLINE void log_set_level(enum log_level level) {
    g_log_level = level;
}

// =============================================================================
// Variadic Macros (compiler builtins, no libc needed)
// =============================================================================

typedef __builtin_va_list va_list;
#define va_start(ap, last) __builtin_va_start(ap, last)
#define va_end(ap)         __builtin_va_end(ap)
#define va_arg(ap, type)   __builtin_va_arg(ap, type)

// =============================================================================
// Formatting Helpers
// =============================================================================

// Format integer to buffer, returns chars written
INLINE u32 fmt_int(char *buf, i64 n, bool is_signed) {
    char tmp[I64_MAX_DIGITS];
    char *p  = tmp + sizeof(tmp);
    bool neg = false;

    if (is_signed && n < 0) {
        neg = true;
        n   = -n;
    }

    u64 un = (u64)n;
    do {
        *--p = (char)('0' + (un % DECIMAL_BASE));
        un /= DECIMAL_BASE;
    } while (un > 0);

    if (neg) {
        *--p = '-';
    }

    u32 len = (u32)(tmp + sizeof(tmp) - p);
    for (u32 i = 0; i < len; i++) {
        buf[i] = p[i];
    }
    return len;
}

// Format hex to buffer
INLINE u32 fmt_hex(char *buf, u64 n) {
    static const char hex[] = "0123456789abcdef";
    char tmp[U64_HEX_DIGITS];
    char *p = tmp + sizeof(tmp);

    do {
        *--p = hex[n & HEX_NIBBLE_MASK];
        n >>= HEX_NIBBLE_SHIFT;
    } while (n > 0);

    u32 len = (u32)(tmp + sizeof(tmp) - p);
    for (u32 i = 0; i < len; i++) {
        buf[i] = p[i];
    }
    return len;
}

// =============================================================================
// Core Logger
// =============================================================================

// Log buffer size (stack allocated, from config.h)
#define LOG_BUF_SIZE LLMQ_LOG_BUF_SIZE

// Level prefixes
static const char *const log_prefixes[] = {
    "[TRACE] ", "[DEBUG] ", "[INFO]  ", "[WARN]  ", "[ERROR] ",
};

// Internal: format and write log message
// Supports: %d (i32), %u (u32), %ld (i64), %lu (u64), %x (hex), %s (string), %.*s (len+ptr)
INLINE void log_writef(enum log_level level, const char *fmt, va_list ap) {
    char buf[LOG_BUF_SIZE];
    u32 pos = 0;

    // Write level prefix
    const char *prefix = log_prefixes[level];
    while (*prefix && pos < LOG_BUF_SIZE - 1) {
        buf[pos++] = *prefix++;
    }

    // Format message
    while (*fmt && pos < LOG_BUF_SIZE - 2) {
        if (*fmt != '%') {
            buf[pos++] = *fmt++;
            continue;
        }

        fmt++; // skip '%'

        // Check for %.*s (length-prefixed string)
        if (fmt[0] == '.' && fmt[1] == '*' && fmt[2] == 's') {
            fmt += 3;
            i32 len       = va_arg(ap, i32);
            const char *s = va_arg(ap, const char *);
            for (i32 i = 0; i < len && pos < LOG_BUF_SIZE - 2; i++) {
                buf[pos++] = s[i];
            }
            continue;
        }

        // Check for length modifier
        bool is_long = false;
        if (*fmt == 'l') {
            is_long = true;
            fmt++;
        }

        switch (*fmt) {
        case 'd':
        case 'i': {
            i64 val = is_long ? va_arg(ap, i64) : (i64)va_arg(ap, i32);
            pos += fmt_int(buf + pos, val, true);
            break;
        }
        case 'u': {
            u64 val = is_long ? va_arg(ap, u64) : (u64)va_arg(ap, u32);
            pos += fmt_int(buf + pos, (i64)val, false);
            break;
        }
        case 'x': {
            u64 val = is_long ? va_arg(ap, u64) : (u64)va_arg(ap, u32);
            pos += fmt_hex(buf + pos, val);
            break;
        }
        case 's': {
            const char *s = va_arg(ap, const char *);
            if (!s)
                s = "(null)";
            while (*s && pos < LOG_BUF_SIZE - 2) {
                buf[pos++] = *s++;
            }
            break;
        }
        case 'p': {
            u64 ptr    = (u64)va_arg(ap, void *);
            buf[pos++] = '0';
            buf[pos++] = 'x';
            pos += fmt_hex(buf + pos, ptr);
            break;
        }
        case '%':
            buf[pos++] = '%';
            break;
        default:
            // Unknown specifier, just copy it
            buf[pos++] = '%';
            if (pos < LOG_BUF_SIZE - 2) {
                buf[pos++] = *fmt;
            }
            break;
        }
        fmt++;
    }

    // Newline
    buf[pos++] = '\n';

    // Write to stderr
    sys_write(FD_STDERR, buf, pos);
}

// =============================================================================
// Public API
// =============================================================================

// In release builds (NDEBUG), trace/debug logging compiles to nothing
// No function call overhead, no argument evaluation
#ifdef NDEBUG
#define log_trace(...) ((void)0)
#define log_debug(...) ((void)0)
#else
INLINE void log_trace(const char *fmt, ...) {
    if (g_log_level > LOG_TRACE)
        return;
    va_list ap;
    va_start(ap, fmt);
    log_writef(LOG_TRACE, fmt, ap);
    va_end(ap);
}

INLINE void log_debug(const char *fmt, ...) {
    if (g_log_level > LOG_DEBUG)
        return;
    va_list ap;
    va_start(ap, fmt);
    log_writef(LOG_DEBUG, fmt, ap);
    va_end(ap);
}
#endif

INLINE void log_info(const char *fmt, ...) {
    if (g_log_level > LOG_INFO)
        return;
    va_list ap;
    va_start(ap, fmt);
    log_writef(LOG_INFO, fmt, ap);
    va_end(ap);
}

INLINE void log_warn(const char *fmt, ...) {
    if (g_log_level > LOG_WARN)
        return;
    va_list ap;
    va_start(ap, fmt);
    log_writef(LOG_WARN, fmt, ap);
    va_end(ap);
}

INLINE void log_error(const char *fmt, ...) {
    if (g_log_level > LOG_ERROR)
        return;
    va_list ap;
    va_start(ap, fmt);
    log_writef(LOG_ERROR, fmt, ap);
    va_end(ap);
}

#endif // UTIL_LOG_H
