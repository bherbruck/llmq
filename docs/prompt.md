Here's the architectural spec for Claude Code:

---

## LLMQ Broker Architecture Notes

### Core Design Principles

**Zero-copy message path.** Messages land in a pool slot via io_uring recv. Parse in place — compute offsets for topic and payload, never copy them. Fan-out builds iovecs pointing at the same bytes. Pool slot is immutable after recv completes.

**Separation of concerns.** Pool owns bytes (refcounted). Per-client FSM owns ceremony state (QoS handshakes). These never mix. The message doesn't know about subscribers. The FSM doesn't own any payload.

**Backpressure before ownership.** During fan-out, check if subscriber's inflight is full *before* incrementing refcount. Skip/drop/defer for slow clients. This is the architectural advantage — traditional brokers take ownership then discover backpressure.

---

### Pool

- mmap'd region, fixed-size slots (e.g., 64KB each)
- Free stack of slot indices — alloc is pop, free is push, O(1)
- Atomic refcount per slot
- Slot freed when refcount hits zero
- No sweeping, no GC, no expiration logic — pool is dumb storage

```
Slot lifecycle:
  recv fills slot → refcount starts at 0
  each subscriber attachment → refcount++
  each delivery completion → refcount--
  refcount == 0 → slot back to free stack
```

---

### Per-Client State

```
struct client {
    int fd;
    uint8_t recv_state;           // parser FSM state
    
    // Inflight deliveries (QoS 1/2 only)
    struct sub_fsm inflight[128]; // fixed, bounded
    uint8_t inflight_count;
    
    // Subscription list (pointers into trie or local copy)
    // ...
};

struct sub_fsm {
    uint16_t packet_id;
    uint8_t  state;          // NONE, PUBACK_PENDING, PUBREC_SENT, PUBREL_RECV
    uint8_t  qos;
    uint32_t slot_idx;       // pointer into pool
    uint64_t deadline;       // monotonic timestamp for timeout
};
```

---

### Hot Path (PUBLISH fan-out)

```
1. io_uring completion: recv into pool slot
2. Parse fixed header + topic offset + payload offset (in place)
3. Trie lookup: get subscriber list
4. For each subscriber:
     - if inflight_count >= 128: skip (backpressure)
     - else: 
         - attach FSM with slot_idx, packet_id, deadline
         - refcount++
         - build iovec: [header, payload pointer]
         - submit writev to io_uring
5. QoS 0: no FSM, just fire and forget
```

**Watch out for:**
- Never copy payload
- Never allocate per-subscriber
- Header can be stack-allocated or small scratch buffer (few bytes)
- Iovec building should stay in L1 cache

---

### Disconnection Handling

When client disconnects (graceful or detected dead):

```
void client_kick(struct client *c) {
    // Release all held refcounts
    for (int i = 0; i < c->inflight_count; i++) {
        refcount_dec(c->inflight[i].slot_idx);
    }
    c->inflight_count = 0;
    
    // Remove from trie subscriptions
    trie_remove_subscriber(c);
    
    // Close fd (io_uring will handle cleanup)
    close(c->fd);
}
```

**Watch out for:**
- Must decrement ALL inflight refcounts or you leak pool slots
- Detect dead clients via: keepalive timeout, write errors, read EOF
- Don't leave zombie clients holding refcounts

---

### FSM Timeouts

Each inflight FSM has a deadline. On each event loop iteration (or periodic sweep):

```
void tick_client_fsms(struct client *c, uint64_t now) {
    for (int i = 0; i < c->inflight_count; i++) {
        if (now > c->inflight[i].deadline) {
            refcount_dec(c->inflight[i].slot_idx);
            fsm_remove(c, i);
            // Optionally: disconnect client if too many timeouts
        }
    }
}
```

This enforces liveness — client's right to hold memory expires, not the message itself.

---

### Backpressure Policy

| Situation | Action |
|-----------|--------|
| inflight full | Skip delivery, don't increment refcount |
| Repeated timeouts | Disconnect client |
| Write returns EAGAIN | Let io_uring handle, don't block |
| Pool exhausted | Drop new publishes or reject with error |

**Key insight:** Backpressure decision happens *before* refcount increment. Slow subscribers never touch the pool.

---

### io_uring Usage

Single ring, SQPOLL mode if targeting zero-syscall hot path.

```
Operations:
  - recv (multishot if available)
  - writev (scatter to subscriber)
  - timeout (for FSM deadlines, optional)
  - close (on disconnect)
```

**Watch out for:**
- CQE reaping — don't let completion queue overflow
- Link recv completions to client struct via user_data
- Handle short writes (rare with TCP, but possible)

---

### Trie

- Owns topic → subscriber list mapping
- Single-threaded, no locks needed (for now)
- Wildcard matching: `+` (single level), `#` (multi level)
- Subscriber list is just pointers to client structs

**Watch out for:**
- Unsubscribe must remove client from all matching nodes
- Client disconnect must remove from trie (don't leak subscriptions)

---

### What NOT to do

- Don't queue messages per-subscriber (that's an output buffer in disguise)
- Don't copy payloads into packet structs
- Don't use shared_ptr or reference-counted wrappers around messages
- Don't allocate during fan-out
- Don't sweep the pool for expiration (FSM timeouts handle it)
- Don't implement shared subscriptions or topic aliases (waste of time for IoT)

---

### Future Multi-threading Notes (not yet, but keep in mind)

- Per-core io_uring rings
- Replicated read-only tries (subscribe broadcasts updates)
- Shared pool (mmap, all cores read, atomic refcounts)
- Connections pinned to cores
- Cross-core fan-out: just writev to their fd, kernel handles it

---

### MQTT Protocol Notes

MQTT 3.1.1 only for now. The QoS ceremonies:

```
QoS 0: PUBLISH → (nothing)
QoS 1: PUBLISH → PUBACK
QoS 2: PUBLISH → PUBREC → PUBREL → PUBCOMP
```

FSM states track where we are in the ceremony. Refcount released when ceremony completes or times out.

---

### The One Metric That Matters

```
Syscalls per message at fan-out of N:
  Target: 0 (with SQPOLL)
  Acceptable: 1 (io_uring_enter for batch)
  Bad: N (one per subscriber)
```