# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

llmq is a high-performance MQTT 3.1.1 broker written in pure C with **zero dependencies** (no libc). It uses Linux io_uring for all I/O operations and direct syscalls, targeting Linux kernel 6.6+ on x86_64.

## Build Commands

```bash
just build        # Release build (optimized, stripped) -> bin/broker
just debug        # Debug build with symbols -> bin/broker
just sanitize     # Build with ASan/UBSan (requires libc) -> bin/broker-san
just lint         # Run clang-tidy
just analyze      # Run clang static analyzer
just fmt          # Format code with clang-format
just fmt-check    # Check formatting without modifying
just check        # Full check: lint + analyze + build
just run          # Build debug and run
just strace       # Run with strace to see io_uring syscalls
just compile-commands  # Generate compile_commands.json for LSP
```

## Architecture

**Single-threaded event loop** built around io_uring:
1. Wait for completion queue entries (CQEs) via `io_uring_enter()`
2. Dispatch completions by operation type (accept, recv, send, close)
3. Submit new submission queue entries (SQEs)

**Key design constraints:**
- No libc: Uses raw syscalls (`src/sys/syscall.h`) and custom `_start` entry point
- No allocation on hot path: All memory pre-allocated at startup
- Zero-copy publish: Message payloads never copied during fan-out (uses `SEND_ZC`)
- Single atomic per message: Refcount for buffer lifecycle

**Source layout:**
- `src/main.c` - Entry point, event loop, broker state
- `src/sys/` - Types, syscall wrappers, io_uring structures
- `src/mem/` - String utilities (future: buffer pools, atomics)

**User data encoding** for CQE dispatch: `[8-bit op][24-bit fd][32-bit context]`

## Code Conventions

- Fixed-width types: `u8`/`i8`, `u16`/`i16`, `u32`/`i32`, `u64`/`i64`, `usize`/`isize`
- Use `INLINE` macro for always-inline functions
- Use `likely()`/`unlikely()` for branch hints
- Syscalls return negative errno on error; check with `IS_ERR()` or compare `< 0`
- Functions should be <100 lines, <50 statements (enforced by clang-tidy)

## Documentation

The `docs/spec/` directory contains the full specification. Key sections:
- `03-architecture.md` - System design and data flow diagrams
- `04-memory.md` - Buffer pool and allocation strategy
- `05-io-uring.md` - Ring setup and operations
- `07-mqtt.md` - Protocol parsing and encoding
