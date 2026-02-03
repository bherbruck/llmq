// test/test.h - Freestanding test framework
// Zero dependencies, linker-section auto-discovery, auto-generated _start
//
// Usage in foo.test.c:
//   #include "test/test.h"
//   #include "foo.h"
//   TEST(my_test) { ASSERT(1 + 1 == 2); }
//
// Build: clang <flags> -o test-foo foo.test.c && ./test-foo

#ifndef TEST_H
#define TEST_H

#include "sys/types.h"
#include "sys/syscall.h"
#include "mem/string.h"
#include "config.h"

// =============================================================================
// Test Registry (linker section magic)
// =============================================================================

struct test_entry {
    const char *name;
    void (*func)(void);
};

// Linker auto-generates __start/__stop symbols for custom sections
extern const struct test_entry __start_test_registry[];
extern const struct test_entry __stop_test_registry[];

// =============================================================================
// Test Macros
// =============================================================================

// Define a test - auto-registers via linker section
#define TEST(name)                                                                \
    static void _test_##name##_func(void);                                        \
    __attribute__((section("test_registry"),                                      \
                   used)) static const struct test_entry _test_##name##_entry = { \
        #name, _test_##name##_func};                                              \
    static void _test_##name##_func(void)

// Assert macro - fails fast with message
#define ASSERT(cond)                          \
    do {                                      \
        if (!(cond)) {                        \
            _test_print("FAIL: " #cond "\n"); \
            sys_exit(1);                      \
        }                                     \
    } while (0)

// Assert equality with better failure messages
#define ASSERT_EQ(a, b)                              \
    do {                                             \
        if ((a) != (b)) {                            \
            _test_print("FAIL: " #a " != " #b "\n"); \
            sys_exit(1);                             \
        }                                            \
    } while (0)

// =============================================================================
// Test Runner
// =============================================================================

static void _test_print(const char *s) {
    sys_write(1, s, strlen(s));
}

static void _test_print_num(i64 n) {
    char buf[I64_MAX_DIGITS];
    char *p  = buf + sizeof(buf) - 1;
    *p       = '\0';
    bool neg = n < 0;
    if (neg)
        n = -n;
    do {
        *--p = (char)('0' + (n % DECIMAL_BASE));
        n /= DECIMAL_BASE;
    } while (n);
    if (neg)
        *--p = '-';
    _test_print(p);
}

static void _run_all_tests(void) {
    const struct test_entry *start = __start_test_registry;
    const struct test_entry *stop  = __stop_test_registry;
    i32 count                      = 0;

    for (const struct test_entry *t = start; t < stop; t++) {
        _test_print("  ");
        _test_print(t->name);
        _test_print("... ");
        t->func();
        _test_print("ok\n");
        count++;
    }

    _test_print("\n");
    _test_print_num(count);
    _test_print(" tests passed\n");
}

// =============================================================================
// Auto-generated entrypoint
// =============================================================================

// _start is defined here - each .test.c compiles to its own binary
__asm__(".global _start\n"
        "_start:\n"
        "    xor %rbp, %rbp\n"
        "    and $-16, %rsp\n"
        "    call _test_main\n"
        "    mov $60, %eax\n"
        "    xor %edi, %edi\n"
        "    syscall\n");

__attribute__((used)) static void _test_main(void) {
    _run_all_tests();
}

#endif // TEST_H
