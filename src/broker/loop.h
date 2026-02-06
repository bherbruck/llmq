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

// Get SQE, flushing to kernel if full
// Returns NULL only on error (very rare)
INLINE struct io_uring_sqe *get_sqe_with_flush(struct broker *b) {
    struct io_uring_sqe *sqe = ring_get_sqe(&b->ring);
    if (likely(sqe))
        return sqe;

    // SQ full - flush local tail to kernel and submit
    ring_flush_sqes(&b->ring);
    ring_submit(&b->ring, 0);

    // Should have space now
    return ring_get_sqe(&b->ring);
}

// Returns true if accept was queued, false if SQ full
INLINE bool submit_accept(struct broker *b) {
    struct io_uring_sqe *sqe = get_sqe_with_flush(b);
    if (!sqe) {
        log_debug("SQ full, can't submit accept");
        return false;
    }
    ring_prep_accept(sqe, b->listen_fd, NULL, NULL, 0);
    sqe->user_data = make_user_data(OP_ACCEPT, 0, 0);
    b->accept_pending++;
    // No flush - batched with other SQEs, flushed at end of event loop
    return true;
}

// Encode slot_idx (24 bits) and generation (8 bits) into 32-bit context
#define MAKE_RECV_CTX(slot, gen) (((u32)(gen) << 24) | ((slot) & 0xFFFFFF))
#define RECV_CTX_SLOT(ctx) ((ctx) & 0xFFFFFF)
#define RECV_CTX_GEN(ctx)  ((u8)((ctx) >> 24))

// Compact recv buffer if needed
// Returns true if compaction happened
// Compacts when: (1) start > 50% of buffer, OR (2) remaining space < 25% of buffer
INLINE bool maybe_compact_recv_buf(struct broker *b, struct client_slot *c) {
    if (c->recv_len == 0) {
        // No data - just reset start to 0 (no memmove needed)
        c->recv_start = 0;
        return false;
    }

    u32 buf_size = b->recv_pool.buf_size;
    u32 space    = buf_size - c->recv_start - c->recv_len;

    // Compact when: start > 50% of buffer, OR remaining space < 25% of buffer
    // The second condition prevents submitting recv with too little space
    if (c->recv_start > buf_size / 2 || space < buf_size / 4) {
        u8 *recv_buf = client_recv_buf(c, &b->recv_pool);
        if (recv_buf) {
            memmove(recv_buf, recv_buf + c->recv_start, c->recv_len);
            c->recv_start = 0;
            return true;
        }
    }
    return false;
}

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

    // Compact if needed before submitting recv (ensures contiguous space)
    maybe_compact_recv_buf(b, c);

    // Write at end of valid data: recv_start + recv_len
    u8 *buf   = recv_buf + c->recv_start + c->recv_len;
    u32 space = b->recv_pool.buf_size - c->recv_start - c->recv_len;

    // If buffer is full after compaction, client is sending oversized/malformed packets
    if (space == 0) {
        log_warn("recv buffer full fd=%d, closing (oversized packet?)", c->fd);
        // Use the already-acquired SQE for close instead
        sys_shutdown(c->fd, SHUT_RDWR);
        ring_prep_close(sqe, c->fd);
        sqe->user_data = make_user_data(OP_CLOSE, (u32)c->fd, 0);
        c->state = CLIENT_CLOSING;
        return true;
    }

    ring_prep_recv(sqe, c->fd, buf, space, 0);
    // Store (slot_idx, generation) in context to detect stale CQEs after slot reuse
    u32 slot_idx   = (u32)(c - b->clients);
    u32 ctx        = MAKE_RECV_CTX(slot_idx, c->generation);
    sqe->user_data = make_user_data(OP_RECV, (u32)c->fd, ctx);
    c->recv_pending = 1; // Mark recv in-flight (kernel may write to buffer)
    // No flush - batched with other SQEs, flushed at end of event loop
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
    // No flush - batched with other SQEs, flushed at end of event loop
}

// Internal: actually submit the zc send SQE (called from submit_send_zc and process_send_retries)
// Returns true on success, false if SQ full
INLINE bool submit_send_zc_internal(struct broker *b, i32 fd, u32 send_desc_idx) {
    struct io_uring_sqe *sqe = ring_get_sqe(&b->ring);
    if (!sqe) {
        return false; // SQ full - caller should queue for retry
    }

    struct send_desc *sd = send_desc_pool_get(&b->send_desc_pool, send_desc_idx);
    if (!sd) {
        log_error("Invalid send_desc_idx: %u", send_desc_idx);
        return true; // Not an SQ issue, don't retry
    }

    // iov[2] has len=0 for QoS 0, so always send all 4 iovecs
    ring_prep_writev(sqe, fd, sd->iov, 4, 0);
    sqe->user_data = make_user_data(OP_SEND_ZC, (u32)fd, send_desc_idx);
    return true;
}

// Clean up send_desc when dropping (SQ full and retry queue also full)
INLINE void drop_send_zc(struct broker *b, u32 send_desc_idx) {
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
}

// Submit zero-copy send using send descriptor's scatter-gather iovecs
// Context is the send_desc_pool index for completion handling
// If SQ full, queues for retry instead of dropping
// NOTE: Does NOT flush SQEs - caller must call ring_flush_sqes() when batch is complete
INLINE void submit_send_zc(struct broker *b, i32 fd, u32 send_desc_idx) {
    if (submit_send_zc_internal(b, fd, send_desc_idx)) {
        return; // Success
    }

    // SQ full - try to queue for retry
    if (send_retry_enqueue(b, fd, send_desc_idx)) {
        return; // Queued for retry
    }

    // Both SQ and retry queue full - must drop
    log_debug("SQ and retry queue full, dropping zc send fd=%d", fd);
    drop_send_zc(b, send_desc_idx);
}

// =============================================================================
// Async Protocol Responses via io_uring (batched with other operations)
// =============================================================================

// Context encoding for OP_SEND_RESP: (gen << 24) | (slot_idx << 8) | resp_slot
// This allows detecting stale CQEs from previous slot occupants
#define MAKE_RESP_CTX(slot_idx, resp_slot, gen) \
    (((u32)(gen) << 24) | (((slot_idx) & 0xFFFF) << 8) | ((resp_slot) & 0xFF))
#define RESP_CTX_SLOT(ctx)      (((ctx) >> 8) & 0xFFFF)
#define RESP_CTX_RESP_SLOT(ctx) ((ctx) & 0xFF)
#define RESP_CTX_GEN(ctx)       ((u8)((ctx) >> 24))

// Submit send from client's resp_buf slot
// Returns true if queued, false if SQ full or no slots
INLINE bool submit_send_resp_slot(struct broker *b, struct client_slot *c, u8 resp_slot, u32 len) {
    struct io_uring_sqe *sqe = get_sqe_with_flush(b);
    if (!sqe) {
        return false;
    }

    u32 slot_idx = (u32)(c - b->clients);
    ring_prep_send(sqe, c->fd, c->resp_buf[resp_slot], len, 0);
    sqe->user_data = make_user_data(OP_SEND_RESP, (u32)c->fd,
                                    MAKE_RESP_CTX(slot_idx, resp_slot, c->generation));
    client_resp_slot_mark_inflight(c, resp_slot);
    return true;
}

// Send static const buffer (e.g., PINGRESP) - async via io_uring
// Returns true if queued, false if client not active or no slots
INLINE bool send_static(struct broker *b, struct client_slot *c, const void *buf, u32 len) {
    if (c->state != CLIENT_ACTIVE) {
        return false;
    }
    if (len > RESP_BUF_SIZE) {
        return false;
    }

    u8 resp_slot = client_get_resp_slot(c);
    if (resp_slot == RESP_SLOTS) {
        // All slots busy - drop (rare under normal load)
        b->drops_resp_full++;
        return false;
    }

    // Copy to resp_buf slot (static buffers are small, typically 2 bytes)
    for (u32 i = 0; i < len; i++) {
        c->resp_buf[resp_slot][i] = ((const u8 *)buf)[i];
    }

    return submit_send_resp_slot(b, c, resp_slot, len);
}

// Send 4-byte protocol response (header + packet_id) - async via io_uring
// Used for PUBACK, PUBREC, PUBREL, PUBCOMP
INLINE bool send_resp(struct broker *b, struct client_slot *c, const u8 *hdr, u16 packet_id) {
    if (c->state != CLIENT_ACTIVE) {
        return false;
    }

    u8 resp_slot = client_get_resp_slot(c);
    if (resp_slot == RESP_SLOTS) {
        // All async slots busy - fall back to synchronous write
        // Critical for QoS 2: dropped PUBREC/PUBCOMP causes infinite retransmit loops
        u8 pkt[4] = {hdr[0], hdr[1], (u8)(packet_id >> 8), (u8)(packet_id & 0xFF)};
        i64 rc = sys_write(c->fd, pkt, 4);
        b->drops_resp_full++;
        return rc == 4;
    }

    // Build packet in resp_buf slot
    c->resp_buf[resp_slot][0] = hdr[0];
    c->resp_buf[resp_slot][1] = hdr[1];
    c->resp_buf[resp_slot][2] = (u8)(packet_id >> 8);
    c->resp_buf[resp_slot][3] = (u8)(packet_id & 0xFF);

    return submit_send_resp_slot(b, c, resp_slot, 4);
}

// Send variable-length buffer (e.g., CONNACK, SUBACK) - async via io_uring
INLINE bool send_buf(struct broker *b, struct client_slot *c, const void *buf, u32 len) {
    if (c->state != CLIENT_ACTIVE) {
        return false;
    }
    if (len > RESP_BUF_SIZE) {
        return false;
    }

    u8 resp_slot = client_get_resp_slot(c);
    if (resp_slot == RESP_SLOTS) {
        // All async slots busy - fall back to synchronous write
        i64 rc = sys_write(c->fd, buf, len);
        b->drops_resp_full++;
        return rc == (i64)len;
    }

    // Copy to resp_buf slot
    for (u32 i = 0; i < len; i++) {
        c->resp_buf[resp_slot][i] = ((const u8 *)buf)[i];
    }

    return submit_send_resp_slot(b, c, resp_slot, len);
}

// =============================================================================
// Async Protocol Responses (for high-volume QoS flows)
// =============================================================================

// Submit async send for protocol response from inflight entry
// The response is pre-built in inflight_cold->resp_pkt, which lives until ceremony completes
// Returns true if queued, false if SQ full
INLINE bool submit_send_inf(struct broker *b, struct client_slot *c,
                            u16 inf_slot, const u8 *hdr) {
    if (c->state != CLIENT_ACTIVE) {
        return false;
    }

    struct io_uring_sqe *sqe = get_sqe_with_flush(b);
    if (!sqe) {
        return false;
    }

    struct inflight_cold *cold = &c->inflight_cold[inf_slot];

    // Build response packet in inflight buffer (lives until ceremony completes)
    cold->resp_pkt[0] = hdr[0];
    cold->resp_pkt[1] = hdr[1];
    cold->resp_pkt[2] = cold->packet_id_be[0];
    cold->resp_pkt[3] = cold->packet_id_be[1];

    u32 slot_idx = (u32)(c - b->clients);
    ring_prep_send(sqe, c->fd, cold->resp_pkt, 4, 0);
    sqe->user_data = make_user_data(OP_SEND_INF, (u32)c->fd, slot_idx);
    sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;
    // No flush - batched with other SQEs, flushed at end of event loop
    return true;
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

    // Force any pending sends/recvs to fail fast (prevents fd-reuse corruption)
    // This is a cold-path syscall; avoids IOSQE_IO_DRAIN stalls.
    sys_shutdown(fd, SHUT_RDWR);

    struct io_uring_sqe *sqe = get_sqe_with_flush(b);
    if (!sqe) {
        log_error("SQ full, can't submit close");
        return;
    }
    ring_prep_close(sqe, fd);
    sqe->user_data = make_user_data(OP_CLOSE, (u32)fd, 0);
    // No flush - batched with other SQEs, flushed at end of event loop
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
    // Decrement pending count first - CQE arrived means one accept completed
    if (b->accept_pending > 0) {
        b->accept_pending--;
    }

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

    // Read from recv_start offset
    u8 *data = recv_buf + c->recv_start;

    while (c->recv_len > 0) {
        // Fast path for 4-byte protocol messages (PUBACK/PUBREC/PUBREL/PUBCOMP)
        // These are ~200K/s in QoS 2 workloads - skip full parser overhead
        // Only use fast path after CONNECT (protocol_version > 0)
        if (c->protocol_version > 0 && c->recv_len >= 4 && data[1] == 0x02) {
            u8 type = data[0] >> 4;
            u16 packet_id = ((u16)data[2] << 8) | data[3];

            if (type == MQTT_PUBACK) {
                fsm_sub_puback_received(b, c, packet_id);
                c->recv_start += 4; c->recv_len -= 4; data += 4;
                continue;
            }
            if (type == MQTT_PUBREC) {
                fsm_sub_pubrec_received(b, c, packet_id);
                c->recv_start += 4; c->recv_len -= 4; data += 4;
                continue;
            }
            if (type == MQTT_PUBREL) {
                fsm_pub_pubrel_received(b, c, packet_id);  // FSM handles send_pubcomp
                c->recv_start += 4; c->recv_len -= 4; data += 4;
                continue;
            }
            if (type == MQTT_PUBCOMP) {
                fsm_sub_pubcomp_received(b, c, packet_id);
                c->recv_start += 4; c->recv_len -= 4; data += 4;
                continue;
            }
        }

        // Standard path for other packets (CONNECT, PUBLISH, SUBSCRIBE, etc.)
        struct mqtt_fixed_header hdr;
        i32 packet_len = mqtt_packet_complete_ex(data, c->recv_len, &hdr);

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

        // Use pre-parsed header to avoid re-parsing
        i32 consumed = process_mqtt_packet_ex(b, c, data, (u32)packet_len, &hdr);

        if (consumed < 0) {
            // Negative return = close connection
            // Header already parsed above - no need to re-parse
            if (hdr.type != MQTT_DISCONNECT) {
                log_warn("Packet error fd=%d type=%d, closing", c->fd, hdr.type);
            }
            submit_close(b, c->fd);
            return;
        }

        // Check if buffer was stolen during PUBLISH processing
        if (c->recv_buf_idx != buf_idx_before) {
            // Buffer was stolen - remaining data already copied to new buffer at offset 0
            // recv_len already contains the correct remaining length, recv_start reset to 0
            c->recv_start = 0;
            // Re-fetch recv_buf and data pointer for new buffer
            recv_buf = client_recv_buf(c, &b->recv_pool);
            if (!recv_buf) {
                return;
            }
            data = recv_buf;
            continue;
        }

        // Advance read pointer instead of memmove (O(1) instead of O(n))
        c->recv_start += (u32)consumed;
        c->recv_len   -= (u32)consumed;
        data          += consumed;
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
        // Client was freed/dormanted while recv was in-flight.
        // The orphaned buffer was saved during slot cleanup - free it now
        // that the kernel is done writing to it.
        if (c->orphaned_recv_buf_idx != BUF_POOL_INVALID) {
            buf_pool_free(&b->recv_pool, c->orphaned_recv_buf_idx);
            c->orphaned_recv_buf_idx = BUF_POOL_INVALID;
        }
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
            if (c->inflight_hot[inf_idx].packet_id == sd->packet_id &&
                c->inflight_cold[inf_idx].send_desc_idx == send_desc_idx) {
                if (cqe->res < 0) {
                    // Send failed - release ref and free inflight (no ACK coming)
                    release_msg_ref(b, c->inflight_hot[inf_idx].msg_idx);
                    client_inflight_free_slot(c, inf_idx);
                } else {
                    // Send succeeded - just unlink send_desc, wait for ACK
                    c->inflight_cold[inf_idx].send_desc_idx = SEND_DESC_INVALID;
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
    case OP_SEND_INF:
        // Protocol response from inflight buffer - just count bytes
        if (cqe->res > 0) {
            b->bytes_sent += (u64)cqe->res;
        }
        break;
    case OP_SEND_RESP: {
        // Protocol response from resp_buf slot - clear in-flight flag
        u32 ctx       = ud_ctx(cqe->user_data);
        u32 slot_idx  = RESP_CTX_SLOT(ctx);
        u8 resp_slot  = (u8)RESP_CTX_RESP_SLOT(ctx);
        u8 cqe_gen    = RESP_CTX_GEN(ctx);
        if (cqe->res > 0) {
            b->bytes_sent += (u64)cqe->res;
        }
        struct client_slot *c = broker_get_client(b, slot_idx);
        // Check generation to avoid corrupting state of a new client in reused slot
        if (c && resp_slot < RESP_SLOTS && c->generation == cqe_gen) {
            client_resp_slot_complete(c, resp_slot);
        }
        break;
    }
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

// Process pending send retries - call after draining CQEs when SQ space is available
// Returns number of successful retries
INLINE u32 process_send_retries(struct broker *b) {
    u32 retried = 0;

    while (!send_retry_empty(b)) {
        u64 packed       = send_retry_dequeue(b);
        i32 fd           = SEND_RETRY_FD(packed);
        u32 send_desc_idx = SEND_RETRY_SD_IDX(packed);

        struct send_desc *sd = send_desc_pool_get(&b->send_desc_pool, send_desc_idx);
        if (!sd || sd->state == SEND_FREE) {
            continue; // Already freed (client disconnected)
        }

        // Validate client is still connected with same fd
        struct client_slot *c = broker_get_client(b, sd->slot_idx);
        if (!c || c->state != CLIENT_ACTIVE || c->fd != fd || c->generation != sd->slot_gen) {
            // Client gone or slot reused - drop the send
            drop_send_zc(b, send_desc_idx);
            continue;
        }

        // Try to submit
        if (!submit_send_zc_internal(b, fd, send_desc_idx)) {
            // Still SQ full - re-enqueue and stop
            if (!send_retry_enqueue(b, fd, send_desc_idx)) {
                // Retry queue also full - drop
                drop_send_zc(b, send_desc_idx);
            }
            break;
        }

        retried++;
    }

    if (retried > 0) {
        b->send_retries += retried;
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

        // Flush any batched SQEs to kernel, then submit.
        // CRITICAL: With COOP_TASKRUN, io_uring_enter is the ONLY time the kernel
        // generates CQEs. We submit BEFORE processing CQEs to avoid a feedback loop
        // where intermediate submits during CQE processing generate new CQEs that
        // keep the loop spinning (livelock under high fan-out load).
        ring_flush_sqes(&b->ring);
        i32 rc = ring_submit(&b->ring, 0);
        if (rc < 0 && rc != -EINTR) {
            log_error("io_uring_enter failed: %d", rc);
            return rc;
        }

        // Process all available CQEs (batched CQ advance - single store-release at end)
        // No io_uring_enter calls during processing - breaks the feedback loop
        ring_cq_sync(&b->ring);
        struct io_uring_cqe *cqe;
        u32 processed = 0;
        while (b->running && (cqe = ring_peek_cqe(&b->ring)) != NULL) {
            dispatch_cqe(b, cqe);
            ring_cq_advance(&b->ring, 1);
            cqe_count++;
            processed++;
        }
        ring_cq_flush(&b->ring);

        process_recv_retries(b);
        process_send_retries(b);

        // Replenish accept queue if it dropped below target
        while (b->accept_pending < ACCEPT_BATCH) {
            if (!submit_accept(b)) {
                break; // SQ full, try again next iteration
            }
        }

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
            rc = ring_submit_and_wait(&b->ring, 1);
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
