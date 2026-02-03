// mem/string.h - Minimal string/memory operations (no libc)

#ifndef MEM_STRING_H
#define MEM_STRING_H

#include "sys/types.h"

INLINE void *memset(void *dest, int c, usize n) {
    u8 *p = (u8 *)dest;
    while (n--) {
        *p++ = (u8)c;
    }
    return dest;
}

INLINE void *memcpy(void *dest, const void *src, usize n) {
    u8 *d       = (u8 *)dest;
    const u8 *s = (const u8 *)src;
    while (n--) {
        *d++ = *s++;
    }
    return dest;
}

INLINE void *memmove(void *dest, const void *src, usize n) {
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

INLINE usize strlen(const char *s) {
    usize len = 0;
    while (*s++) {
        len++;
    }
    return len;
}

INLINE int memcmp(const void *s1, const void *s2, usize n) {
    const u8 *p1 = (const u8 *)s1;
    const u8 *p2 = (const u8 *)s2;
    while (n--) {
        if (*p1 != *p2) {
            return *p1 - *p2;
        }
        p1++;
        p2++;
    }
    return 0;
}

#endif // MEM_STRING_H
