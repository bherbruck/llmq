// mem/string.c - Non-inline memory operations for linker
// Required because compiler can emit memcpy/memset calls for struct copies

#include "sys/types.h"

// These are the actual symbols the linker will find
// The inline versions in string.h are optimized out but the compiler
// can still emit calls to these for large copies or struct assignments

void *memcpy(void *dest, const void *src, usize n) {
    u8 *d       = (u8 *)dest;
    const u8 *s = (const u8 *)src;
    while (n--) {
        *d++ = *s++;
    }
    return dest;
}

void *memset(void *dest, int c, usize n) {
    u8 *p = (u8 *)dest;
    while (n--) {
        *p++ = (u8)c;
    }
    return dest;
}

void *memmove(void *dest, const void *src, usize n) {
    u8 *d       = (u8 *)dest;
    const u8 *s = (const u8 *)src;
    if (d < s) {
        while (n--) {
            *d++ = *s++;
        }
    } else {
        d += n;
        s += n;
        while (n--) {
            *--d = *--s;
        }
    }
    return dest;
}
