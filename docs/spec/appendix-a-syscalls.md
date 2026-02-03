# Appendix A: Syscall Reference

## A.1 Overview

This appendix provides the raw syscall wrappers used by the broker. All syscalls use the x86_64 Linux ABI.

## A.2 Syscall Numbers

```c
// Core syscalls
#define SYS_read              0
#define SYS_write             1
#define SYS_close             3
#define SYS_mmap              9
#define SYS_munmap            11
#define SYS_mprotect          10

// Socket syscalls
#define SYS_socket            41
#define SYS_accept            43
#define SYS_accept4           288
#define SYS_bind              49
#define SYS_listen            50
#define SYS_setsockopt        54
#define SYS_getsockopt        55
#define SYS_shutdown          48

// Process syscalls
#define SYS_exit              60
#define SYS_exit_group        231
#define SYS_fork              57
#define SYS_clone             56
#define SYS_wait4             61
#define SYS_sched_setaffinity 203

// io_uring syscalls
#define SYS_io_uring_setup    425
#define SYS_io_uring_enter    426
#define SYS_io_uring_register 427

// Time syscalls
#define SYS_clock_gettime     228
#define SYS_nanosleep         35
```

## A.3 Syscall Wrappers

### A.3.1 Base Wrappers

```c
static inline long syscall0(long n) {
    long ret;
    __asm__ volatile(
        "syscall"
        : "=a"(ret)
        : "a"(n)
        : "rcx", "r11", "memory"
    );
    return ret;
}

static inline long syscall1(long n, long a1) {
    long ret;
    __asm__ volatile(
        "syscall"
        : "=a"(ret)
        : "a"(n), "D"(a1)
        : "rcx", "r11", "memory"
    );
    return ret;
}

static inline long syscall2(long n, long a1, long a2) {
    long ret;
    __asm__ volatile(
        "syscall"
        : "=a"(ret)
        : "a"(n), "D"(a1), "S"(a2)
        : "rcx", "r11", "memory"
    );
    return ret;
}

static inline long syscall3(long n, long a1, long a2, long a3) {
    long ret;
    __asm__ volatile(
        "syscall"
        : "=a"(ret)
        : "a"(n), "D"(a1), "S"(a2), "d"(a3)
        : "rcx", "r11", "memory"
    );
    return ret;
}

static inline long syscall4(long n, long a1, long a2, long a3, long a4) {
    long ret;
    register long r10 __asm__("r10") = a4;
    __asm__ volatile(
        "syscall"
        : "=a"(ret)
        : "a"(n), "D"(a1), "S"(a2), "d"(a3), "r"(r10)
        : "rcx", "r11", "memory"
    );
    return ret;
}

static inline long syscall5(long n, long a1, long a2, long a3, long a4, long a5) {
    long ret;
    register long r10 __asm__("r10") = a4;
    register long r8 __asm__("r8") = a5;
    __asm__ volatile(
        "syscall"
        : "=a"(ret)
        : "a"(n), "D"(a1), "S"(a2), "d"(a3), "r"(r10), "r"(r8)
        : "rcx", "r11", "memory"
    );
    return ret;
}

static inline long syscall6(long n, long a1, long a2, long a3, long a4, long a5, long a6) {
    long ret;
    register long r10 __asm__("r10") = a4;
    register long r8 __asm__("r8") = a5;
    register long r9 __asm__("r9") = a6;
    __asm__ volatile(
        "syscall"
        : "=a"(ret)
        : "a"(n), "D"(a1), "S"(a2), "d"(a3), "r"(r10), "r"(r8), "r"(r9)
        : "rcx", "r11", "memory"
    );
    return ret;
}
```

### A.3.2 Memory Wrappers

```c
#define PROT_READ     0x1
#define PROT_WRITE    0x2
#define PROT_EXEC     0x4
#define MAP_SHARED    0x01
#define MAP_PRIVATE   0x02
#define MAP_ANONYMOUS 0x20
#define MAP_POPULATE  0x08000

static inline void *sys_mmap(void *addr, u64 len, int prot, int flags, int fd, u64 off) {
    return (void *)syscall6(SYS_mmap, (long)addr, len, prot, flags, fd, off);
}

static inline int sys_munmap(void *addr, u64 len) {
    return (int)syscall2(SYS_munmap, (long)addr, len);
}
```

### A.3.3 Socket Wrappers

```c
#define AF_INET       2
#define SOCK_STREAM   1
#define SOCK_NONBLOCK 04000
#define SOL_SOCKET    1
#define SO_REUSEADDR  2
#define SO_REUSEPORT  15

struct sockaddr_in {
    u16 sin_family;
    u16 sin_port;
    u32 sin_addr;
    u8  sin_zero[8];
};

static inline int sys_socket(int domain, int type, int protocol) {
    return (int)syscall3(SYS_socket, domain, type, protocol);
}

static inline int sys_bind(int fd, const struct sockaddr_in *addr, u32 addrlen) {
    return (int)syscall3(SYS_bind, fd, (long)addr, addrlen);
}

static inline int sys_listen(int fd, int backlog) {
    return (int)syscall2(SYS_listen, fd, backlog);
}

static inline int sys_setsockopt(int fd, int level, int optname, const void *optval, u32 optlen) {
    return (int)syscall5(SYS_setsockopt, fd, level, optname, (long)optval, optlen);
}

static inline int sys_close(int fd) {
    return (int)syscall1(SYS_close, fd);
}
```

### A.3.4 io_uring Wrappers

```c
static inline int sys_io_uring_setup(u32 entries, struct io_uring_params *params) {
    return (int)syscall2(SYS_io_uring_setup, entries, (long)params);
}

static inline int sys_io_uring_enter(int fd, u32 to_submit, u32 min_complete, u32 flags) {
    return (int)syscall4(SYS_io_uring_enter, fd, to_submit, min_complete, flags);
}

static inline int sys_io_uring_register(int fd, u32 opcode, void *arg, u32 nr_args) {
    return (int)syscall4(SYS_io_uring_register, fd, opcode, (long)arg, nr_args);
}
```

### A.3.5 Process Wrappers

```c
static inline void sys_exit(int code) {
    syscall1(SYS_exit_group, code);
    __builtin_unreachable();
}

static inline int sys_fork(void) {
    return (int)syscall0(SYS_fork);
}

static inline int sys_sched_setaffinity(int pid, u64 cpusetsize, const void *mask) {
    return (int)syscall3(SYS_sched_setaffinity, pid, cpusetsize, (long)mask);
}
```

### A.3.6 Time Wrappers

```c
#define CLOCK_MONOTONIC 1

struct timespec {
    i64 tv_sec;
    i64 tv_nsec;
};

static inline int sys_clock_gettime(int clock_id, struct timespec *tp) {
    return (int)syscall2(SYS_clock_gettime, clock_id, (long)tp);
}

static inline u64 time_now_ms(void) {
    struct timespec ts;
    sys_clock_gettime(CLOCK_MONOTONIC, &ts);
    return (u64)ts.tv_sec * 1000 + (u64)ts.tv_nsec / 1000000;
}

static inline u32 time_now_sec(void) {
    struct timespec ts;
    sys_clock_gettime(CLOCK_MONOTONIC, &ts);
    return (u32)ts.tv_sec;
}
```

## A.4 Error Handling

Syscalls return negative errno on failure:

```c
#define EAGAIN      11
#define ENOMEM      12
#define EACCES      13
#define EFAULT      14
#define EBUSY       16
#define ENOENT      2
#define EINTR       4
#define EIO         5
#define EBADF       9
#define EINVAL      22
#define ENOSYS      38
#define ECONNRESET  104
#define ENOTCONN    107

static inline int is_error(long ret) {
    return ret < 0 && ret >= -4095;
}

static inline int get_errno(long ret) {
    return is_error(ret) ? (int)(-ret) : 0;
}
```

## A.5 Entry Point

The program entry point without libc:

```c
// Defined by linker
void _start(void) {
    // Get argc, argv from stack (System V AMD64 ABI)
    register long *sp __asm__("rsp");
    int argc = (int)sp[0];
    char **argv = (char **)(sp + 1);
    char **envp = (char **)(sp + 1 + argc + 1);
    
    // Call main
    int ret = main(argc, argv, envp);
    
    // Exit
    sys_exit(ret);
}
```

## A.6 Atomic Operations

GCC built-in atomics (no libc dependency):

```c
#define atomic_load(p) \
    __atomic_load_n(p, __ATOMIC_ACQUIRE)

#define atomic_store(p, v) \
    __atomic_store_n(p, v, __ATOMIC_RELEASE)

#define atomic_add(p, v) \
    __atomic_fetch_add(p, v, __ATOMIC_ACQ_REL)

#define atomic_sub(p, v) \
    __atomic_fetch_sub(p, v, __ATOMIC_ACQ_REL)

#define atomic_or(p, v) \
    __atomic_fetch_or(p, v, __ATOMIC_ACQ_REL)

#define atomic_and(p, v) \
    __atomic_fetch_and(p, v, __ATOMIC_ACQ_REL)

#define atomic_cmpxchg(p, expected, desired) \
    __atomic_compare_exchange_n(p, expected, desired, 0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)
```

## A.7 Utility Functions

Minimal implementations without libc:

```c
static inline void *memset(void *s, int c, u64 n) {
    u8 *p = s;
    while (n--) *p++ = (u8)c;
    return s;
}

static inline void *memcpy(void *dst, const void *src, u64 n) {
    u8 *d = dst;
    const u8 *s = src;
    while (n--) *d++ = *s++;
    return dst;
}

static inline void *memmove(void *dst, const void *src, u64 n) {
    u8 *d = dst;
    const u8 *s = src;
    if (d < s) {
        while (n--) *d++ = *s++;
    } else {
        d += n;
        s += n;
        while (n--) *--d = *--s;
    }
    return dst;
}

static inline int memcmp(const void *s1, const void *s2, u64 n) {
    const u8 *p1 = s1, *p2 = s2;
    while (n--) {
        if (*p1 != *p2) return *p1 - *p2;
        p1++; p2++;
    }
    return 0;
}

static inline u64 strlen(const char *s) {
    const char *p = s;
    while (*p) p++;
    return p - s;
}
```
