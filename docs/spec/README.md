# io_uring Native MQTT Broker Specification

**Version:** 0.1.0-draft  
**Status:** Draft  
**Last Updated:** 2026-02-02  
**Codename:** Ringbroker

## Abstract

This specification defines a high-performance MQTT 3.1.1 broker implemented in pure C with zero external dependencies. MQTT 5.0 support is a future goal. The broker leverages Linux io_uring for all I/O operations, achieving zero-copy message fan-out and minimal syscall overhead. The implementation targets Linux kernel 6.6+ and uses direct syscalls without libc.

## Design Philosophy

1. **Zero dependencies** — No libc, no external libraries. Pure syscalls.
2. **Zero-copy hot path** — Message payloads never copied in userspace during fan-out
3. **Zero allocation hot path** — All memory pre-allocated at startup
4. **Minimal syscalls** — Batch all I/O through io_uring submission
5. **Single atomic per message** — Refcount for buffer lifecycle, nothing else

## Table of Contents

1. [Abstract](./00-abstract.md)
2. [Introduction](./01-introduction.md)
3. [Terminology](./02-terminology.md)
4. [System Architecture](./03-architecture.md)
5. [Memory Model](./04-memory.md)
6. [io_uring Integration](./05-io-uring.md)
7. [Connection Management](./06-connections.md)
8. [MQTT Protocol Handling](./07-mqtt.md)
9. [Topic Trie](./08-trie.md)
10. [Message Fan-out](./09-fanout.md)
11. [Buffer Lifecycle](./10-buffers.md)
12. [Multi-Process Scaling](./11-scaling.md)
13. [Security Considerations](./12-security.md)
14. [References](./13-references.md)
15. [Appendix A: Syscall Reference](./appendix-a-syscalls.md)
16. [Appendix B: Data Structures](./appendix-b-structures.md)
17. [Appendix C: Build System](./appendix-c-build.md)

## Document Index

| Section | File | Description | Lines |
|---------|------|-------------|-------|
| Abstract | [00-abstract.md](./00-abstract.md) | Project summary and goals | ~60 |
| Introduction | [01-introduction.md](./01-introduction.md) | Background, motivation, scope | ~150 |
| Terminology | [02-terminology.md](./02-terminology.md) | Definitions and RFC 2119 | ~100 |
| Architecture | [03-architecture.md](./03-architecture.md) | High-level system design | ~200 |
| Memory Model | [04-memory.md](./04-memory.md) | Allocation strategy, pools | ~250 |
| io_uring | [05-io-uring.md](./05-io-uring.md) | Ring setup, operations | ~300 |
| Connections | [06-connections.md](./06-connections.md) | Slot management, state machine, timeouts | ~250 |
| MQTT Protocol | [07-mqtt.md](./07-mqtt.md) | Parsing, encoding, QoS FSMs, sessions | ~550 |
| Topic Trie | [08-trie.md](./08-trie.md) | Subscription matching | ~250 |
| Fan-out | [09-fanout.md](./09-fanout.md) | Zero-copy publish distribution | ~200 |
| Buffers | [10-buffers.md](./10-buffers.md) | Refcounting, lifecycle | ~150 |
| Scaling | [11-scaling.md](./11-scaling.md) | Multi-process architecture | ~200 |
| Security | [12-security.md](./12-security.md) | Threat model, mitigations | ~150 |
| References | [13-references.md](./13-references.md) | Normative and informative | ~50 |
| Syscalls | [appendix-a-syscalls.md](./appendix-a-syscalls.md) | Raw syscall wrappers | ~200 |
| Structures | [appendix-b-structures.md](./appendix-b-structures.md) | All data structure definitions | ~450 |
| Build | [appendix-c-build.md](./appendix-c-build.md) | Compilation, flags, targets | ~100 |

## Quick Navigation

- **Implementers**: Start with [Architecture](./03-architecture.md), then [io_uring](./05-io-uring.md)
- **Hot path understanding**: [Fan-out](./09-fanout.md) → [Buffers](./10-buffers.md)
- **MQTT compliance**: [MQTT Protocol](./07-mqtt.md)
- **Data structures**: [Appendix B](./appendix-b-structures.md)
- **Building**: [Appendix C](./appendix-c-build.md)

## Source Layout

```
broker/
├── build.sh
├── docs/
│   └── spec/              # This specification
└── src/
    ├── main.c             # Entry point, event loop
    ├── sys/
    │   ├── types.h        # Primitive types
    │   ├── syscall.h      # Raw syscall wrappers
    │   └── io_uring.h     # io_uring structures
    ├── mem/
    │   ├── pool.h         # Buffer pool management
    │   └── atomic.h       # Atomic operations
    ├── net/
    │   ├── socket.h       # Socket syscall wrappers
    │   └── tcp.h          # TCP constants, sockaddr
    ├── mqtt/
    │   ├── proto.h        # MQTT constants
    │   ├── parse.h        # In-place packet parsing
    │   └── encode.h       # Response packet encoding
    └── broker/
        ├── broker.h       # Main broker state
        ├── conn.h         # Connection slots
        ├── trie.h         # Topic trie
        ├── fanout.h       # Publish distribution
        └── ref.h          # Message refcounting
```
