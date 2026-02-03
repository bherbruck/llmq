# Section 2: Terminology

## 2.1 RFC 2119 Keywords

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in RFC 2119.

## 2.2 Definitions

### Core Concepts

**Broker**
: The MQTT message broker process. Accepts connections, routes messages between publishers and subscribers.

**Connection Slot**
: A pre-allocated structure holding state for one TCP connection. Indexed by file descriptor or direct descriptor number.

**Direct Descriptor**
: An io_uring feature where file descriptors are registered in a table and referenced by index rather than kernel fd number. Avoids fd table lookup on each operation.

**Hot Path**
: The code path executed for every message publish. Must be zero-copy and zero-allocation.

**Cold Path**
: Code paths executed less frequently (connect, subscribe, disconnect). May allocate or copy.

### Memory

**Buffer Pool**
: A fixed array of equal-sized buffers allocated at startup and registered with io_uring.

**Buffer Index**
: Integer identifying a buffer within the pool. Used in io_uring operations with registered buffers.

**Message Reference (msg_ref)**
: Metadata structure tracking a message being fanned out. Contains buffer index, offset, length, and atomic refcount.

**Refcount**
: Atomic counter tracking how many send operations reference a buffer. When it reaches zero, the buffer is returned to the free pool.

### io_uring

**Submission Queue (SQ)**
: Ring buffer where userspace posts I/O requests (SQEs) for the kernel to process.

**Completion Queue (CQ)**
: Ring buffer where the kernel posts I/O results (CQEs) for userspace to consume.

**SQE (Submission Queue Entry)**
: A single I/O request (accept, recv, send, etc.).

**CQE (Completion Queue Entry)**
: Result of a completed I/O operation. Contains return value and user_data for correlation.

**Registered Buffer**
: A buffer whose virtual address is pre-registered with the kernel, enabling zero-copy operations.

**Multishot Operation**
: An SQE that generates multiple CQEs (e.g., multishot accept produces one CQE per accepted connection).

**Zero-Copy Send (SEND_ZC)**
: Send operation where the kernel transmits directly from the userspace buffer without copying to kernel buffers.

### MQTT

**QoS (Quality of Service)**
: MQTT delivery guarantee level. QoS 0 = at most once, QoS 1 = at least once, QoS 2 = exactly once.

**Topic**
: A UTF-8 string used for message routing. Hierarchical with `/` separator.

**Topic Filter**
: A subscription pattern that may include wildcards (`+` for single level, `#` for multi-level).

**Client ID**
: Unique identifier for an MQTT client connection.

**Session**
: State associated with a client including subscriptions and pending messages.

### Networking

**SO_REUSEPORT**
: Socket option allowing multiple sockets to bind to the same port. Kernel distributes connections across sockets.

**Backpressure**
: Mechanism to slow publishers when subscribers cannot keep up. Implemented via send buffer limits.

## 2.3 Notation

### Numeric

- `0x` prefix indicates hexadecimal (e.g., `0xFF`)
- `0b` prefix indicates binary (e.g., `0b1010`)
- Sizes use IEC units: KiB (1024), MiB (1024²), GiB (1024³)

### Code

- C code samples use C11 standard with GNU extensions for atomics
- Inline assembly is x86_64 AT&T syntax
- Structure layouts assume little-endian, naturally aligned

### Diagrams

- `──►` indicates data flow direction
- `───` indicates bidirectional or structural connection
- `[ ]` indicates a component or data structure
- `( )` indicates a process or operation

## 2.4 Abbreviations

| Abbreviation | Meaning |
|--------------|---------|
| CQ | Completion Queue |
| CQE | Completion Queue Entry |
| fd | File Descriptor |
| mmap | Memory Map |
| MQTT | Message Queuing Telemetry Transport |
| NIC | Network Interface Card |
| QoS | Quality of Service |
| SQ | Submission Queue |
| SQE | Submission Queue Entry |
| TCP | Transmission Control Protocol |
| ZC | Zero-Copy |
