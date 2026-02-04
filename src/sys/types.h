// sys/types.h - Primitive types for freestanding C
// No libc, no stdint.h - define everything ourselves

#ifndef SYS_TYPES_H
#define SYS_TYPES_H

// Fixed-width unsigned integers
typedef unsigned char u8;
typedef unsigned short u16;
typedef unsigned int u32;
typedef unsigned long long u64;

// Fixed-width signed integers
typedef signed char i8;
typedef signed short i16;
typedef signed int i32;
typedef signed long long i64;

// Size types (64-bit)
typedef u64 usize;
typedef i64 isize;

// Boolean
typedef _Bool bool;
#define true  1
#define false 0

// Null pointer
#define NULL ((void *)0)

// Limits
#define U8_MAX  0xFF
#define U16_MAX 0xFFFF
#define U32_MAX 0xFFFFFFFFU
#define U64_MAX 0xFFFFFFFFFFFFFFFFULL

#define I8_MIN  (-128)
#define I8_MAX  127
#define I16_MIN (-32768)
#define I16_MAX 32767
#define I32_MIN (-2147483647 - 1)
#define I32_MAX 2147483647
#define I64_MIN (-9223372036854775807LL - 1)
#define I64_MAX 9223372036854775807LL

// Compiler hints
#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

// Alignment and packing
#define ALIGNED(n) __attribute__((aligned(n)))
#define PACKED     __attribute__((packed))
#define UNUSED     __attribute__((unused))
#define NORETURN   __attribute__((noreturn))
// INLINE: always_inline in release, plain static in profile builds for visibility
#ifdef PROFILE_BUILD
#define INLINE static
#else
#define INLINE static inline __attribute__((always_inline))
#endif

// Cache line size (x86_64)
#define CACHE_LINE 64

// Static assertions
#define STATIC_ASSERT(cond, msg) _Static_assert(cond, msg)

// Verify our type sizes are correct
STATIC_ASSERT(sizeof(u8) == 1, "u8 must be 1 byte");
STATIC_ASSERT(sizeof(u16) == 2, "u16 must be 2 bytes");
STATIC_ASSERT(sizeof(u32) == 4, "u32 must be 4 bytes");
STATIC_ASSERT(sizeof(u64) == 8, "u64 must be 8 bytes");
STATIC_ASSERT(sizeof(void *) == 8, "pointers must be 8 bytes");

// =============================================================================
// I/O vector for scatter-gather I/O
// =============================================================================

struct iovec {
    void *iov_base;
    usize iov_len;
};

#endif // SYS_TYPES_H
