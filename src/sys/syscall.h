// sys/syscall.h - Raw syscall wrappers for x86_64 Linux
// No libc - direct syscall instruction

#ifndef SYS_SYSCALL_H
#define SYS_SYSCALL_H

#include "types.h"

// =============================================================================
// Syscall numbers (x86_64)
// =============================================================================

#define SYS_read              0
#define SYS_write             1
#define SYS_open              2
#define SYS_close             3
#define SYS_writev            20
#define SYS_mmap              9
#define SYS_munmap            11
#define SYS_socket            41
#define SYS_sendmsg           46
#define SYS_accept            43
#define SYS_bind              49
#define SYS_listen            50
#define SYS_setsockopt        54

// sendmsg flags
#define MSG_DONTWAIT 0x40
#define SYS_exit              60
#define SYS_fcntl             72
#define SYS_rt_sigaction      13
#define SYS_io_uring_setup    425
#define SYS_io_uring_enter    426
#define SYS_io_uring_register 427

// Signal numbers
#define SIGINT  2
#define SIGTERM 15

// =============================================================================
// Raw syscall interface
// =============================================================================

INLINE i64 syscall0(i64 n) {
    i64 ret;
    __asm__ volatile("syscall" : "=a"(ret) : "a"(n) : "rcx", "r11", "memory");
    return ret;
}

INLINE i64 syscall1(i64 n, i64 a1) {
    i64 ret;
    __asm__ volatile("syscall" : "=a"(ret) : "a"(n), "D"(a1) : "rcx", "r11", "memory");
    return ret;
}

INLINE i64 syscall2(i64 n, i64 a1, i64 a2) {
    i64 ret;
    __asm__ volatile("syscall" : "=a"(ret) : "a"(n), "D"(a1), "S"(a2) : "rcx", "r11", "memory");
    return ret;
}

INLINE i64 syscall3(i64 n, i64 a1, i64 a2, i64 a3) {
    i64 ret;
    __asm__ volatile("syscall"
                     : "=a"(ret)
                     : "a"(n), "D"(a1), "S"(a2), "d"(a3)
                     : "rcx", "r11", "memory");
    return ret;
}

INLINE i64 syscall4(i64 n, i64 a1, i64 a2, i64 a3, i64 a4) {
    i64 ret;
    register i64 r10 __asm__("r10") = a4;
    __asm__ volatile("syscall"
                     : "=a"(ret)
                     : "a"(n), "D"(a1), "S"(a2), "d"(a3), "r"(r10)
                     : "rcx", "r11", "memory");
    return ret;
}

INLINE i64 syscall5(i64 n, i64 a1, i64 a2, i64 a3, i64 a4, i64 a5) {
    i64 ret;
    register i64 r10 __asm__("r10") = a4;
    register i64 r8 __asm__("r8")   = a5;
    __asm__ volatile("syscall"
                     : "=a"(ret)
                     : "a"(n), "D"(a1), "S"(a2), "d"(a3), "r"(r10), "r"(r8)
                     : "rcx", "r11", "memory");
    return ret;
}

INLINE i64 syscall6(i64 n, i64 a1, i64 a2, i64 a3, i64 a4, i64 a5, i64 a6) {
    i64 ret;
    register i64 r10 __asm__("r10") = a4;
    register i64 r8 __asm__("r8")   = a5;
    register i64 r9 __asm__("r9")   = a6;
    __asm__ volatile("syscall"
                     : "=a"(ret)
                     : "a"(n), "D"(a1), "S"(a2), "d"(a3), "r"(r10), "r"(r8), "r"(r9)
                     : "rcx", "r11", "memory");
    return ret;
}

// =============================================================================
// Error handling
// =============================================================================

// Linux syscalls return negative errno on error
#define IS_ERR(x)  ((u64)(x) >= (u64) - 4096)
#define ERR_VAL(x) (-(i32)(x))

// Common error codes
#define EPERM      1
#define ENOENT     2
#define EINTR      4
#define EIO        5
#define EAGAIN     11
#define ENOMEM     12
#define EACCES     13
#define EFAULT     14
#define EBUSY      16
#define EEXIST     17
#define EINVAL     22
#define ENOSPC     28
#define EPIPE      32
#define ENOTSOCK   88
#define EADDRINUSE 98
#define ECONNRESET 104
#define ETIMEDOUT  110

// =============================================================================
// Wrapped syscalls
// =============================================================================

INLINE void sys_exit(i32 code) {
    syscall1(SYS_exit, code);
    __builtin_unreachable();
}

INLINE i64 sys_read(i32 fd, void *buf, usize count) {
    return syscall3(SYS_read, fd, (i64)buf, (i64)count);
}

INLINE i64 sys_write(i32 fd, const void *buf, usize count) {
    return syscall3(SYS_write, fd, (i64)buf, (i64)count);
}

INLINE i64 sys_writev(i32 fd, const struct iovec *iov, i32 iovcnt) {
    return syscall3(SYS_writev, fd, (i64)iov, iovcnt);
}

// msghdr for sendmsg
struct msghdr {
    void *msg_name;
    u32 msg_namelen;
    struct iovec *msg_iov;
    usize msg_iovlen;
    void *msg_control;
    usize msg_controllen;
    i32 msg_flags;
};

INLINE i64 sys_sendmsg(i32 fd, const struct msghdr *msg, i32 flags) {
    return syscall3(SYS_sendmsg, fd, (i64)msg, flags);
}

INLINE i32 sys_close(i32 fd) {
    return (i32)syscall1(SYS_close, fd);
}

// open flags
#define O_RDONLY 0
#define O_WRONLY 1
#define O_RDWR   2
#define O_CREAT  0100
#define O_TRUNC  01000
#define O_APPEND 02000

INLINE i32 sys_open(const char *path, i32 flags, i32 mode) {
    return (i32)syscall3(SYS_open, (i64)path, flags, mode);
}

// mmap flags
#define PROT_READ   0x1
#define PROT_WRITE  0x2
#define MAP_SHARED  0x01
#define MAP_PRIVATE 0x02
#define MAP_ANON    0x20
#define MAP_FAILED  ((void *)-1)

INLINE void *sys_mmap(void *addr, usize len, i32 prot, i32 flags, i32 fd, i64 off) {
    return (void *)syscall6(SYS_mmap, (i64)addr, (i64)len, prot, flags, fd, off);
}

INLINE i32 sys_munmap(void *addr, usize len) {
    return (i32)syscall2(SYS_munmap, (i64)addr, (i64)len);
}

// Socket constants
#define AF_INET      2
#define SOCK_STREAM  1
#define SOL_SOCKET   1
#define SO_REUSEADDR 2
#define SO_REUSEPORT 15

INLINE i32 sys_socket(i32 domain, i32 type, i32 protocol) {
    return (i32)syscall3(SYS_socket, domain, type, protocol);
}

INLINE i32 sys_bind(i32 fd, const void *addr, u32 addrlen) {
    return (i32)syscall3(SYS_bind, fd, (i64)addr, addrlen);
}

INLINE i32 sys_listen(i32 fd, i32 backlog) {
    return (i32)syscall2(SYS_listen, fd, backlog);
}

INLINE i32 sys_setsockopt(i32 fd, i32 level, i32 optname, const void *optval, u32 optlen) {
    return (i32)syscall5(SYS_setsockopt, fd, level, optname, (i64)optval, optlen);
}

// Signal handling
#define SA_RESTORER 0x04000000
#define SIGSET_SIZE 8 // sizeof(sigset_t) on x86_64
#define SIGPIPE     13
#define SIG_IGN     ((void (*)(i32))1)

struct sigaction {
    void (*sa_handler)(i32);
    u64 sa_flags;
    void (*sa_restorer)(void);
    u64 sa_mask;
};

// Signal return trampoline (required for rt_sigaction)
__attribute__((naked)) static void sigreturn_trampoline(void) {
    __asm__ volatile("mov $15, %rax\n" // SYS_rt_sigreturn
                     "syscall\n");
}

INLINE i32 sys_sigaction(i32 signum, const struct sigaction *act, struct sigaction *oldact) {
    return (i32)syscall4(SYS_rt_sigaction, signum, (i64)act, (i64)oldact, SIGSET_SIZE);
}

// io_uring syscalls
INLINE i32 sys_io_uring_setup(u32 entries, void *params) {
    return (i32)syscall2(SYS_io_uring_setup, entries, (i64)params);
}

INLINE i32 sys_io_uring_enter(i32 fd, u32 to_submit, u32 min_complete, u32 flags, void *sig) {
    return (i32)syscall5(SYS_io_uring_enter, fd, to_submit, min_complete, flags, (i64)sig);
}

INLINE i32 sys_io_uring_register(i32 fd, u32 opcode, void *arg, u32 nr_args) {
    return (i32)syscall4(SYS_io_uring_register, fd, opcode, (i64)arg, nr_args);
}

#endif // SYS_SYSCALL_H
