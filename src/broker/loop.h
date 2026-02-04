// broker/loop.h - io_uring event loop
// Submit operations and dispatch completions

#ifndef BROKER_LOOP_H
#define BROKER_LOOP_H

#include "broker/state.h"
#include "mqtt/packet.h"
#include "util/log.h"
#include "sys/syscall.h"

// =============================================================================
// io_uring Submit Operations
// =============================================================================

// Get SQE, flushing to kernel if full (simple non-blocking flush)
INLINE struct io_uring_sqe *get_sqe_with_flush(struct broker *b) {
    struct io_uring_sqe *sqe = ring_get_sqe(&b->ring);
    if (likely(sqe))
        return sqe;

    // SQ full - flush pending SQEs to kernel (non-blocking) - rare path
    ring_submit(&b->ring, 0);

    return ring_get_sqe(&b->ring);
}

INLINE void submit_accept(struct broker *b) {
    struct io_uring_sqe *sqe = get_sqe_with_flush(b);
    if (!sqe) {
        log_error("SQ full, can't submit accept");
        return;
    }
    ring_prep_accept(sqe, b->listen_fd, NULL, NULL, 0);
    sqe->user_data = make_user_data(OP_ACCEPT, 0, 0);
    ring_submit_sqe(&b->ring);
}

// Encode slot_idx (24 bits) and generation (8 bits) into 32-bit context
#define MAKE_RECV_CTX(slot, gen) (((u32)(gen) << 24) | ((slot) & 0xFFFFFF))
#define RECV_CTX_SLOT(ctx) ((ctx) & 0xFFFFFF)
#define RECV_CTX_GEN(ctx)  ((u8)((ctx) >> 24))

// Submit recv for client, returns true on success, false if SQ full (caller should retry)
INLINE bool submit_recv_internal(struct broker *b, struct client_slot *c) {
    struct io_uring_sqe *sqe = get_sqe_with_flush(b);
    if (!sqe) {
        return false; // SQ full - caller should retry later
    }
    u8 *recv_buf = client_recv_buf(c, &b->recv_pool);
    if (!recv_buf) {
        log_error("No recv buffer for fd=%d", c->fd);
        return true; // Not an SQ issue, don't retry
    }
    u8 *buf   = recv_buf + c->recv_len;
    u32 space = b->recv_pool.buf_size - c->recv_len;
    ring_prep_recv(sqe, c->fd, buf, space, 0);
    // Store (slot_idx, generation) in context to detect stale CQEs after slot reuse
    u32 slot_idx   = (u32)(c - b->clients);
    u32 ctx        = MAKE_RECV_CTX(slot_idx, c->generation);
    sqe->user_data = make_user_data(OP_RECV, (u32)c->fd, ctx);
    c->recv_pending = 1; // Mark recv in-flight (kernel may write to buffer)
    ring_submit_sqe(&b->ring);
    return true;
}

// Submit recv, queueing for retry if SQ is full
INLINE void submit_recv(struct broker *b, struct client_slot *c) {
    if (!submit_recv_internal(b, c)) {
        // SQ full - queue for retry after CQEs are processed
        u32 slot_idx = (u32)(c - b->clients);
        recv_retry_enqueue(b, slot_idx);
    }
}

INLINE void submit_send(struct broker *b, i32 fd, const u8 *buf, u32 len) {
    struct io_uring_sqe *sqe = get_sqe_with_flush(b);
    if (!sqe) {
        log_error("SQ full, can't submit send");
        return;
    }
    ring_prep_send(sqe, fd, buf, len, 0);
    sqe->user_data = make_user_data(OP_SEND, (u32)fd, 0);
    ring_submit_sqe(&b->ring);
}

// Submit zero-copy send using send descriptor's scatter-gather iovecs
// Context is the send_desc_pool index for completion handling
INLINE void submit_send_zc(struct broker *b, i32 fd, u32 send_desc_idx) {
    struct io_uring_sqe *sqe = get_sqe_with_flush(b);
    if (!sqe) {
        log_debug("SQ full, dropping zc send fd=%d", fd);
        b->msgs_dropped++;
        b->drops_sq_full++;

        struct send_desc *sd = send_desc_pool_get(&b->send_desc_pool, send_desc_idx);
        if (sd && sd->msg_idx != MSG_POOL_INVALID) {
            // For QoS 0: release ref now (no inflight entry exists)
            // For QoS > 0: inflight entry exists and holds the ref - let timeout/disconnect clean it up
            if (sd->sub_qos == 0) {
                u32 new_ref = msg_pool_unref(&b->msg_pool, sd->msg_idx);
                if (new_ref == 0) {
                    struct canonical_msg *msg = msg_pool_get(&b->msg_pool, sd->msg_idx);
                    if (msg && msg->buf_idx != MSG_POOL_INVALID) {
                        buf_pool_free(&b->recv_pool, msg->buf_idx);
                    }
                    msg_pool_free(&b->msg_pool, sd->msg_idx);
                }
            }
        }
        send_desc_pool_free(&b->send_desc_pool, send_desc_idx);
        return;
    }

    struct send_desc *sd = send_desc_pool_get(&b->send_desc_pool, send_desc_idx);
    if (!sd) {
        log_error("Invalid send_desc_idx: %u", send_desc_idx);
        return;
    }

    // Determine number of iovecs (4 for QoS > 0, 3 for QoS 0 since pkt_id iov is empty)
    u32 nr_vecs = (sd->sub_qos > 0) ? 4 : 3;
    if (sd->sub_qos == 0) {
        // For QoS 0, skip the packet_id iovec by shifting payload
        // Actually, iov[2] has len=0 for QoS 0, so we can send all 4
        nr_vecs = 4;
    }

    ring_prep_writev(sqe, fd, sd->iov, nr_vecs, 0);
    sqe->user_data = make_user_data(OP_SEND_ZC, (u32)fd, send_desc_idx);
    ring_submit_sqe(&b->ring);
}

// =============================================================================
// Synchronous Protocol Responses (simple, no buffering complexity)
// =============================================================================

// Send static const buffer synchronously (e.g., PINGRESP)
// Returns true if sent, false if client not active
INLINE bool send_static(struct broker *b, struct client_slot *c, const void *buf, u32 len) {
    if (c->state != CLIENT_ACTIVE) {
        return false;
    }
    i64 rc = sys_write(c->fd, buf, len);
    if (rc > 0) {
        b->bytes_sent += (u64)rc;
    }
    return rc == (i64)len;
}

// Send 4-byte protocol response synchronously (header + packet_id)
// Used for PUBACK, PUBREC, PUBREL, PUBCOMP
INLINE bool send_resp(struct broker *b, struct client_slot *c, const u8 *hdr, u16 packet_id) {
    if (c->state != CLIENT_ACTIVE) {
        return false;
    }
    u8 pkt[4] = {hdr[0], hdr[1], (u8)(packet_id >> 8), (u8)(packet_id & 0xFF)};
    i64 rc = sys_write(c->fd, pkt, 4);
    if (rc > 0) {
        b->bytes_sent += (u64)rc;
    }
    return rc == 4;
}

// Send variable-length buffer synchronously (e.g., CONNACK, SUBACK)
INLINE bool send_buf(struct broker *b, struct client_slot *c, const void *buf, u32 len) {
    if (c->state != CLIENT_ACTIVE) {
        return false;
    }
    i64 rc = sys_write(c->fd, buf, len);
    if (rc > 0) {
        b->bytes_sent += (u64)rc;
    }
    return rc == (i64)len;
}

// Submit close and mark client as CLOSING to prevent new operations on this fd.
// This prevents the fd-reuse race where a recv CQE arrives after fd is closed
// and reused for a new connection.
INLINE void submit_close(struct broker *b, i32 fd) {
    // Mark client as CLOSING FIRST to prevent new recv/send submissions.
    // This is critical: without it, handle_recv might submit another recv
    // after we queue the close, creating a race with fd reuse.
    struct client_slot *c = broker_get_client_by_fd(b, fd);
    if (c && c->state == CLIENT_ACTIVE) {
        c->state = CLIENT_CLOSING;
    }

    struct io_uring_sqe *sqe = get_sqe_with_flush(b);
    if (!sqe) {
        log_error("SQ full, can't submit close");
        return;
    }
    ring_prep_close(sqe, fd);
    // DRAIN ensures all pending sends complete before close.
    // Without this, SQPOLL can reorder: new accept gets same fd, pending send goes to wrong client.
    sqe->flags     = IOSQE_IO_DRAIN;
    sqe->user_data = make_user_data(OP_CLOSE, (u32)fd, 0);
    ring_submit_sqe(&b->ring);
}

// Release a message reference (decrement refcount, free if last)
// Called when QoS ceremony completes (ACK received) or on timeout/disconnect
INLINE void release_msg_ref(struct broker *b, u32 msg_idx) {
    if (msg_idx == MSG_POOL_INVALID) {
        return;
    }
    struct canonical_msg *msg = msg_pool_get(&b->msg_pool, msg_idx);
    u32 new_ref               = msg_pool_unref(&b->msg_pool, msg_idx);
    if (new_ref == 0 && msg) {
        if (msg->buf_idx != MSG_POOL_INVALID) {
            buf_pool_free(&b->recv_pool, msg->buf_idx);
        }
        msg_pool_free(&b->msg_pool, msg_idx);
    }
}

// Include handler after submit functions are defined
// Note: handler.h includes mqtt_fsm.h which provides fsm_client_disconnect, fsm_sweep_all_timeouts
#include "broker/handler.h"

// =============================================================================
// CQE Handlers
// =============================================================================

INLINE void handle_accept(struct broker *b, struct io_uring_cqe *cqe) {
    i32 client_fd = cqe->res;

    if (unlikely(client_fd < 0)) {
        log_error("accept() failed: %d", client_fd);
        submit_accept(b);
        return;
    }

    if (unlikely((u32)client_fd >= b->max_fds)) {
        log_error("fd too large: %d", client_fd);
        sys_close(client_fd);
        submit_accept(b);
        return;
    }

    b->accepts++;

    // Disable Nagle's algorithm for low latency
    i32 flag = 1;
    sys_setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    i32 slot_idx = broker_alloc_slot(b, client_fd);
    if (slot_idx < 0) {
        log_error("No free slots for fd=%d", client_fd);
        sys_close(client_fd);
        submit_accept(b);
        return;
    }

    struct client_slot *c = &b->clients[slot_idx];
    log_debug("Accepted fd=%d slot=%d", client_fd, slot_idx);

    submit_recv(b, c);
    submit_accept(b);
}

INLINE void process_recv_buffer(struct broker *b, struct client_slot *c) {
    u8 *recv_buf = client_recv_buf(c, &b->recv_pool);
    if (!recv_buf) {
        return;
    }

    while (c->recv_len > 0) {
        i32 packet_len = mqtt_packet_complete(recv_buf, c->recv_len);

        if (packet_len == MQTT_INCOMPLETE) {
            break;
        }

        if (packet_len < 0) {
            log_warn("Malformed packet from fd=%d", c->fd);
            submit_close(b, c->fd);
            return;
        }

        // Save buffer index to detect if buffer was stolen during processing
        u32 buf_idx_before = c->recv_buf_idx;

        i32 consumed = process_mqtt_packet(b, c, recv_buf, (u32)packet_len);

        if (consumed < 0) {
            // Negative return = close connection
            // Only warn if it wasn't a clean DISCONNECT (type=14)
            struct mqtt_fixed_header hdr;
            mqtt_parse_fixed_header(recv_buf, c->recv_len, &hdr);
            if (hdr.type != MQTT_DISCONNECT) {
                log_warn("Packet error fd=%d type=%d, closing", c->fd, hdr.type);
            }
            submit_close(b, c->fd);
            return;
        }

        // Check if buffer was stolen during PUBLISH processing
        if (c->recv_buf_idx != buf_idx_before) {
            // Buffer was stolen - remaining data already copied to new buffer at offset 0
            // recv_len already contains the correct remaining length
            // Re-fetch recv_buf and continue processing
            recv_buf = client_recv_buf(c, &b->recv_pool);
            if (!recv_buf) {
                return;
            }
            continue;
        }

        u32 remaining = c->recv_len - (u32)consumed;
        if (remaining > 0) {
            memmove(recv_buf, recv_buf + consumed, remaining);
        }
        c->recv_len = remaining;
    }
}

INLINE void handle_recv(struct broker *b, struct io_uring_cqe *cqe) {
    i32 fd       = (i32)ud_fd(cqe->user_data);
    u32 ctx      = ud_ctx(cqe->user_data);
    u32 slot_idx = RECV_CTX_SLOT(ctx);
    u8 cqe_gen   = RECV_CTX_GEN(ctx);

    // Use slot_idx from user_data to find the slot
    struct client_slot *c = broker_get_client(b, slot_idx);
    if (unlikely(!c)) {
        return;
    }

    // Check generation to detect stale CQEs from previous slot use.
    // Stale CQEs are rare - only happen during rapid reconnects
    if (unlikely(c->generation != cqe_gen)) {
        // Stale CQE - free the orphaned buffer that was saved during slot cleanup
        if (c->orphaned_recv_buf_idx != BUF_POOL_INVALID) {
            buf_pool_free(&b->recv_pool, c->orphaned_recv_buf_idx);
            c->orphaned_recv_buf_idx = BUF_POOL_INVALID;
        }
        return;
    }

    // Clear recv_pending - the recv has completed (success or error)
    c->recv_pending = 0;

    // Check state after clearing recv_pending
    if (unlikely(c->state != CLIENT_ACTIVE)) {
        // Client is CLOSING/FREE - don't process data
        return;
    }

    // Most recvs are successful with data
    if (unlikely(cqe->res <= 0)) {
        if (cqe->res < 0) {
            log_warn("recv error fd=%d err=%d", fd, cqe->res);
        } else {
            log_debug("client closed connection fd=%d", fd);
        }
        submit_close(b, fd);
        return;
    }

    c->recv_len += (u32)cqe->res;
    b->bytes_recv += (u64)cqe->res;

    process_recv_buffer(b, c);

    // Most clients stay active after recv
    if (likely(c->state == CLIENT_ACTIVE)) {
        submit_recv(b, c);
    }
}

INLINE void handle_send(struct broker *b, struct io_uring_cqe *cqe) {
    if (cqe->res < 0) {
        log_debug("send error fd=%d err=%d", (i32)ud_fd(cqe->user_data), cqe->res);
        // Don't close on send error - recv will detect the broken connection.
        // Closing here risks closing a reused fd: if the old connection's close
        // completed and fd was reused for a new client, we'd close the new client.
    } else {
        b->bytes_sent += (u64)cqe->res;
    }
}

// Handle completion of zero-copy send
// Cleanup: free send_desc, decrement msg refcount, free stolen buffer if last ref
INLINE void handle_send_zc(struct broker *b, struct io_uring_cqe *cqe) {
    u32 send_desc_idx = ud_ctx(cqe->user_data);

    struct send_desc *sd = send_desc_pool_get(&b->send_desc_pool, send_desc_idx);
    if (unlikely(!sd)) {
        log_error("handle_send_zc: invalid send_desc_idx %u", send_desc_idx);
        return;
    }

    if (unlikely(cqe->res < 0)) {
        log_debug("zc send error fd=%d err=%d", (i32)ud_fd(cqe->user_data), cqe->res);
        b->msgs_dropped++;
        b->drops_send_failed++;
        // Don't close on send error - recv will detect the broken connection.
        // Closing here risks closing a reused fd.
    } else {
        b->bytes_sent += (u64)cqe->res;
    }

    // For QoS 0: decrement refcount now (fire and forget, no ACK expected)
    // For QoS > 0: refcount held until ACK ceremony completes (enables retransmission)
    if (sd->sub_qos == 0) {
        u32 msg_idx               = sd->msg_idx;
        struct canonical_msg *msg = msg_pool_get(&b->msg_pool, msg_idx);
        u32 new_ref               = msg_pool_unref(&b->msg_pool, msg_idx);
        if (new_ref == 0 && msg) {
            if (msg->buf_idx != MSG_POOL_INVALID) {
                buf_pool_free(&b->recv_pool, msg->buf_idx);
            }
            msg_pool_free(&b->msg_pool, msg_idx);
        }
    }

    // For QoS > 0, handle inflight entry
    if (sd->sub_qos > 0 && sd->packet_id != 0) {
        struct client_slot *c = broker_get_client(b, sd->slot_idx);
        // Check generation to detect slot reuse - if slot was reused,
        // the old inflight entries are gone and we shouldn't touch new client's inflight
        if (c && c->generation == sd->slot_gen) {
            // O(1) lookup: packet_id maps directly to inflight slot
            u16 inf_idx = (sd->packet_id - 1) & c->inflight_mask;
            if (c->inflight[inf_idx].packet_id == sd->packet_id &&
                c->inflight[inf_idx].send_desc_idx == send_desc_idx) {
                if (cqe->res < 0) {
                    // Send failed - release ref and free inflight (no ACK coming)
                    release_msg_ref(b, c->inflight[inf_idx].msg_idx);
                    client_inflight_free(c, &c->inflight[inf_idx]);
                } else {
                    // Send succeeded - just unlink send_desc, wait for ACK
                    c->inflight[inf_idx].send_desc_idx = SEND_DESC_INVALID;
                }
            }
        }
    }

    // Free the send descriptor
    send_desc_pool_free(&b->send_desc_pool, send_desc_idx);
}

INLINE void handle_close(struct broker *b, struct io_uring_cqe *cqe) {
    i32 fd = (i32)ud_fd(cqe->user_data);

    // Find slot and handle disconnect
    // Verify fd matches to handle stale close CQEs after fd reuse
    struct client_slot *c = broker_get_client_by_fd(b, fd);
    if (c && (c->state == CLIENT_ACTIVE || c->state == CLIENT_CLOSING) && c->fd == fd) {
        i32 slot_idx = b->fd_to_slot[fd];
        log_debug("disconnect fd=%d slot=%d clean=%d", fd, slot_idx, c->clean_session);

        // Release message refs for pending inflight entries before cleanup
        // FSM handles releasing refs for messages we never got ACKs for
        fsm_client_disconnect(b, c);

        if (c->clean_session) {
            broker_free_slot(b, (u32)slot_idx);
        } else {
            broker_slot_go_dormant(b, (u32)slot_idx);
        }
    }
}

INLINE void dispatch_cqe(struct broker *b, struct io_uring_cqe *cqe) {
    u8 op = ud_op(cqe->user_data);

    switch (op) {
    case OP_ACCEPT:
        handle_accept(b, cqe);
        break;
    case OP_RECV:
        handle_recv(b, cqe);
        break;
    case OP_SEND:
        handle_send(b, cqe);
        break;
    case OP_SEND_ZC:
        handle_send_zc(b, cqe);
        break;
    case OP_CLOSE:
        handle_close(b, cqe);
        break;
    default:
        log_warn("Unknown op: %d", op);
        break;
    }
}

// =============================================================================
// Recv Retry Processing
// =============================================================================

// Process pending recv retries - call after draining CQEs when SQ space is available
// Returns number of successful retries
INLINE u32 process_recv_retries(struct broker *b) {
    u32 retried = 0;

    while (!recv_retry_empty(b)) {
        u32 slot_idx = recv_retry_dequeue(b);
        if (slot_idx >= b->max_clients) {
            break; // Invalid slot (shouldn't happen)
        }

        struct client_slot *c = &b->clients[slot_idx];

        // Check if client is still active and needs recv
        if (c->state != CLIENT_ACTIVE) {
            continue; // Client disconnected, skip
        }

        // Check if recv is already pending (shouldn't be, but safety check)
        if (c->recv_pending) {
            continue; // Already has pending recv
        }

        // Try to submit recv
        if (!submit_recv_internal(b, c)) {
            // Still SQ full - re-enqueue and stop processing
            recv_retry_enqueue(b, slot_idx);
            break;
        }

        retried++;
    }

    if (retried > 0) {
        b->recv_retries += retried;
    }

    return retried;
}

// =============================================================================
// Main Event Loop
// =============================================================================

#define ACCEPT_BATCH 32

// =============================================================================
// Timeout Sweep - cleanup expired inflight entries
// =============================================================================

// Note: timeout sweep functions moved to mqtt_fsm.h (fsm_sweep_client, fsm_sweep_all_timeouts)

// Log pool utilization stats
INLINE void log_pool_stats(struct broker *b) {
    u32 msg_used  = b->msg_pool.capacity - b->msg_pool.free_count;
    u32 sd_used   = b->send_desc_pool.capacity - b->send_desc_pool.free_count;
    u32 recv_used = b->recv_pool.capacity - b->recv_pool.free_count;

    // Show dynamic pool info: used/capacity (grows to max)
    // Drop breakdown: if=inflight_full, sd=send_desc_empty, sq=sq_full
    log_info("POOLS: msg=%u/%u sd=%u/%u(%u) recv=%u/%u active=%u drops=%lu (if=%lu sd=%lu sq=%lu)",
             msg_used, b->msg_pool.capacity,
             sd_used, b->send_desc_pool.capacity, b->send_desc_pool.grow_count,
             recv_used, b->recv_pool.capacity,
             b->active_count, b->msgs_dropped,
             b->drops_inflight_full, b->drops_send_desc_empty, b->drops_sq_full);
}

INLINE i32 broker_run(struct broker *b) {
    b->running = true;

    // Initialize timing
    b->now = now_ms();
    b->last_timeout_sweep = b->now;

    // Submit multiple accepts to handle connection bursts
    for (i32 i = 0; i < ACCEPT_BATCH; i++) {
        submit_accept(b);
    }

    log_info("Entering event loop...");

    u64 last_stats_time = 0;
    u64 cqe_count       = 0;

    while (b->running) {
        // Update current time (used for deadlines)
        b->now = now_ms();

        // Submit pending SQEs without blocking
        i32 rc = ring_submit(&b->ring, 0);
        if (rc < 0 && rc != -EINTR) {
            log_error("io_uring_enter failed: %d", rc);
            return rc;
        }

        // Process all available CQEs (batched CQ advance - single store-release at end)
        ring_cq_sync(&b->ring);
        struct io_uring_cqe *cqe;
        u32 processed = 0;
        while ((cqe = ring_peek_cqe(&b->ring)) != NULL) {
            dispatch_cqe(b, cqe);
            ring_cq_advance(&b->ring, 1);
            cqe_count++;
            processed++;
        }
        ring_cq_flush(&b->ring);

        process_recv_retries(b);

        // Periodic timeout sweep - FSM handles retransmission or cleanup
        if (b->now - b->last_timeout_sweep >= LLMQ_TIMEOUT_SWEEP_INTERVAL_MS) {
            u32 processed_timeouts = fsm_sweep_all_timeouts(b);
            if (processed_timeouts > 0) {
                log_debug("Timeout sweep: %u inflight entries processed", processed_timeouts);
            }
            b->last_timeout_sweep = b->now;
        }

        // Only block when idle
        if (processed == 0) {
            rc = ring_submit(&b->ring, 1);
            if (rc < 0 && rc != -EINTR) {
                log_error("io_uring_enter failed: %d", rc);
                return rc;
            }
        }

        if (cqe_count - last_stats_time > LLMQ_STATS_LOG_INTERVAL) {
            log_pool_stats(b);
            last_stats_time = cqe_count;
        }
    }

    return 0;
}

#endif // BROKER_LOOP_H
