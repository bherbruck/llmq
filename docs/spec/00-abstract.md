# Section 0: Abstract

## Status of This Document

This document specifies a high-performance MQTT broker implementation for Linux systems. This is a draft specification subject to revision.

## Abstract

This specification defines **Ringbroker**, an MQTT 3.1.1 compliant message broker implemented in pure C without external dependencies. MQTT 5.0 support is a future goal. The broker is designed around three core principles:

1. **Kernel-native I/O**: All network operations use Linux io_uring, eliminating per-operation syscall overhead and enabling zero-copy transmission
2. **Static memory model**: All memory is pre-allocated at startup with no allocations on the hot path
3. **Minimal synchronization**: A single atomic refcount per message enables safe buffer reuse across asynchronous completions

The implementation targets Linux kernel 6.6 or later, leveraging modern io_uring features including:

- `IORING_SETUP_SINGLE_ISSUER` for reduced kernel locking
- `IORING_SETUP_DEFER_TASKRUN` for controlled completion processing
- `IORING_OP_SEND_ZC` for zero-copy transmission
- Registered buffers and direct descriptors for fast-path optimization
- Multishot accept and receive for reduced SQE/CQE overhead

## Goals

| Goal | Metric |
|------|--------|
| Throughput | >1M messages/second on commodity hardware |
| Latency | <100μs p99 for QoS 0 fan-out |
| Memory | O(connections + topics + inflight), no growth |
| Syscalls | 1 per event loop iteration (io_uring_enter) |
| Binary size | <100KB static binary |
| Dependencies | None (no libc) |

## Non-Goals

- Windows or macOS support
- MQTT-SN, WebSocket, or other transports
- Persistent sessions across broker restarts
- Built-in TLS (use a terminating proxy)
- Plugin or extension system

## Target Environment

- Linux kernel 6.6+
- x86_64 architecture (ARM64 possible with syscall number changes)
- Single-node deployment (clustering via external coordination)

## Conformance

An implementation conforms to this specification if it:

1. Implements all MUST and MUST NOT requirements
2. Implements all SHOULD requirements or documents deviations
3. Passes the MQTT conformance test suite for supported protocol versions
4. Maintains the zero-copy and zero-allocation properties on the publish hot path
