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
// Egress Flush Engine
// =============================================================================

// Discard unsent segment (stale, invalid msg/buf, pre-send error).
// Releases ref if egress owns it (packet_id==0 → no inflight allocated).
INLINE void egress_discard_head(struct broker *b, struct client_slot *c,
                                struct egress_segment *seg) {
    if (seg->kind == SEG_PUBLISH && seg->msg_idx != MSG_POOL_INVALID &&
        seg->packet_id == 0) {
        release_msg_ref(b, seg->msg_idx);
    }
    egress_pop(&c->egress_head, &c->egress_count, c->egress_mask);
}

// Pop head after successful send completion.
// QoS 0: release ref. QoS 1/2: ref now owned by inflight entry.
INLINE void egress_complete_head(struct broker *b, struct client_slot *c,
                                 struct egress_segment *seg) {
    if (seg->kind == SEG_PUBLISH && seg->qos == 0 && seg->msg_idx != MSG_POOL_INVALID) {
        release_msg_ref(b, seg->msg_idx);
    }
    egress_pop(&c->egress_head, &c->egress_count, c->egress_mask);
}

INLINE void egress_apply_cursor(struct iovec *iov, u8 iov_cnt, u32 cursor) {
    for (u8 i = 0; i < iov_cnt && cursor > 0; i++) {
        u32 len = (u32)iov[i].iov_len;
        if (len == 0) {
            continue;
        }
        if (cursor >= len) {
            cursor -= len;
            iov[i].iov_len = 0;
            continue;
        }
        iov[i].iov_base = (u8 *)iov[i].iov_base + cursor;
        iov[i].iov_len  = len - cursor;
        cursor          = 0;
    }
}

// Flush client's egress queue via io_uring.
// Batches up to EGRESS_BATCH_MAX consecutive PUBLISH segments into one writev.
// CTRL segments are sent individually (small, infrequent).
// Returns:
//  - EGRESS_FLUSH_SUBMITTED: SQE queued
//  - EGRESS_FLUSH_SQ_FULL: SQ full, caller should retry later
//  - EGRESS_FLUSH_NOOP: nothing to do (empty/inflight/inactive)
INLINE u8 egress_flush(struct broker *b, struct client_slot *c, u32 slot_idx) {
    if (c->egress_inflight || c->egress_count == 0 || c->state != CLIENT_ACTIVE)
        return EGRESS_FLUSH_NOOP;

    // Pre-clean: discard stale/completed segments from head.
    while (c->egress_count > 0) {
        struct egress_segment *seg = egress_head_seg(c->egress, c->egress_head, c->egress_mask);
        if (seg->slot_gen != c->generation) {
            egress_discard_head(b, c, seg);
            continue;
        }
        if (seg->kind == SEG_PUBLISH) {
            u32 wl = egress_seg_load_wire_len(seg);
            if (wl == 0 || seg->cursor >= wl) { egress_discard_head(b, c, seg); continue; }
        } else if (seg->kind == SEG_CTRL) {
            if (seg->ctrl_len == 0 || seg->cursor >= seg->ctrl_len) {
                egress_discard_head(b, c, seg);
                continue;
            }
        } else {
            egress_discard_head(b, c, seg);
            continue;
        }
        break;
    }
    if (c->egress_count == 0)
        return EGRESS_FLUSH_NOOP;

    struct egress_segment *head = egress_head_seg(c->egress, c->egress_head, c->egress_mask);

    // --- Batch up to EGRESS_BATCH_MAX segments (PUBLISH + CTRL interleaved) ---
    u8 batch   = 0;   // Total segments in batch
    u8 pub_idx = 0;   // Index into scratch arrays (PUBLISH only)
    u8 iov_cnt = 0;
    u32 total_wire_len = 0; // Running total for fast-path CQE completion
    u8 has_qos0_refs = 0;   // Track if any QoS 0 PUBLISH needs ref release

    for (u8 i = 0; i < EGRESS_BATCH_MAX && (u16)i < c->egress_count; i++) {
        struct egress_segment *seg = &c->egress[(c->egress_head + i) & c->egress_mask];

        if (seg->slot_gen != c->generation)
            break;

        if (seg->kind == SEG_CTRL) {
            // CTRL: single iov entry pointing at segment's inline data
            if (seg->ctrl_len == 0 || seg->cursor >= seg->ctrl_len) break;
            // Only first segment can have non-zero cursor (partial resume)
            if (batch > 0 && seg->cursor > 0) break;
            u32 seg_wire = seg->ctrl_len - seg->cursor;
            c->egress_iov[iov_cnt].iov_base = seg->ctrl + seg->cursor;
            c->egress_iov[iov_cnt].iov_len  = seg_wire;
            total_wire_len += seg_wire;
            iov_cnt++;
            batch++;
            continue;
        }

        if (seg->kind != SEG_PUBLISH)
            break;

        struct canonical_msg *msg = msg_pool_get(&b->msg_pool, seg->msg_idx);
        if (!msg) break;
        u8 *msgbuf = buf_pool_get(&b->recv_pool, msg->buf_idx);
        if (!msgbuf) break;

        u32 wire_len = egress_seg_load_wire_len(seg);
        if (wire_len == 0) break;

        // Only first segment can have non-zero cursor (partial resume)
        if (batch > 0 && seg->cursor > 0) break;
        total_wire_len += wire_len - seg->cursor;

        // QoS 1/2: allocate inflight at send time (deferred from fan-out).
        // packet_id==0 means first send; packet_id>0 means retransmit.
        if (seg->qos > 0 && seg->packet_id == 0) {
            u64 deadline = b->now + LLMQ_INFLIGHT_TIMEOUT_MS;
            u16 packet_id;
            i32 inf_idx = client_inflight_alloc(c, seg->qos, seg->msg_idx,
                                                 deadline, &packet_id);
            if (inf_idx < 0) {
                if (batch == 0) return EGRESS_FLUSH_NOOP;
                break; // Send what we have so far
            }
            seg->packet_id = packet_id;
        }

        // Build fixed header: type(0x30) + flags, remaining length, topic length.
        u32 remaining = 2 + msg->topic_len + msg->payload_len;
        if (seg->qos > 0) remaining += 2;
        u8 flags = 0;
        if (seg->retain) flags |= 0x01;
        flags |= (seg->qos << 1) & 0x06;
        if (seg->dup) flags |= 0x08;
        c->egress_headers[pub_idx][0] = 0x30 | flags;
        u8 pos = 1;
        u32 x  = remaining;
        do {
            u8 byte = (u8)(x & 0x7F);
            x >>= 7;
            if (x > 0) byte |= 0x80;
            c->egress_headers[pub_idx][pos++] = byte;
        } while (x > 0);
        c->egress_headers[pub_idx][pos++] = (u8)(msg->topic_len >> 8);
        c->egress_headers[pub_idx][pos++] = (u8)(msg->topic_len & 0xFF);

        // Build iov: [header][topic][pktid][payload]
        c->egress_iov[iov_cnt].iov_base = c->egress_headers[pub_idx];
        c->egress_iov[iov_cnt].iov_len  = pos;
        iov_cnt++;
        c->egress_iov[iov_cnt].iov_base = msgbuf + msg->topic_off;
        c->egress_iov[iov_cnt].iov_len  = msg->topic_len;
        iov_cnt++;
        if (seg->qos > 0) {
            c->egress_pkt_ids[pub_idx][0]     = (u8)(seg->packet_id >> 8);
            c->egress_pkt_ids[pub_idx][1]     = (u8)(seg->packet_id & 0xFF);
            c->egress_iov[iov_cnt].iov_base = c->egress_pkt_ids[pub_idx];
            c->egress_iov[iov_cnt].iov_len  = 2;
        } else {
            c->egress_iov[iov_cnt].iov_base = NULL;
            c->egress_iov[iov_cnt].iov_len  = 0;
            if (seg->msg_idx != MSG_POOL_INVALID) has_qos0_refs = 1;
        }
        iov_cnt++;
        c->egress_iov[iov_cnt].iov_base = msgbuf + msg->payload_off;
        c->egress_iov[iov_cnt].iov_len  = msg->payload_len;
        iov_cnt++;

        pub_idx++;
        batch++;
    }

    if (batch == 0)
        return EGRESS_FLUSH_NOOP;

    // Apply cursor to first segment's iov entries (partial send resume)
    if (head->cursor > 0) {
        u8 head_iovs = (head->kind == SEG_CTRL) ? 1 : 4;
        egress_apply_cursor(c->egress_iov, head_iovs, head->cursor);
    }

    struct io_uring_sqe *sqe = ring_get_sqe(&b->ring);
    if (!sqe) { b->drops_sq_full++; return EGRESS_FLUSH_SQ_FULL; }

    ring_prep_writev(sqe, c->fd, c->egress_iov, iov_cnt, 0);
    sqe->user_data = make_user_data(OP_EGRESS, (u32)c->fd,
                                    MAKE_EGRESS_CTX(slot_idx, c->generation));
    c->egress_inflight        = 1;
    c->egress_batch_count     = batch;
    c->egress_batch_wire_len  = total_wire_len;
    c->egress_batch_qos0_refs = has_qos0_refs;
    return EGRESS_FLUSH_SUBMITTED;
}

// Handle completion of batched egress writev/send.
// Walks batch segments using pre-stored wire length (no msg_pool_get on CQE path).
// Ref ownership: QoS 0 released here; QoS 1/2 owned by inflight until ACK.
INLINE void handle_egress_cqe(struct broker *b, struct io_uring_cqe *cqe) {
    u32 ctx      = ud_ctx(cqe->user_data);
    u32 slot_idx = EGRESS_CTX_SLOT(ctx);
    u8 cqe_gen   = EGRESS_CTX_GEN(ctx);
    i32 cqe_fd   = (i32)ud_fd(cqe->user_data);

    struct client_slot *c = broker_get_client(b, slot_idx);
    if (!c || c->generation != cqe_gen || c->fd != cqe_fd)
        return;

    c->egress_inflight = 0;
    u8 batch = c->egress_batch_count;

    if (c->egress_count == 0)
        return;

    if (likely(cqe->res > 0)) {
        b->bytes_sent += (u64)cqe->res;

        // Fast path: entire batch sent in one shot (common case)
        if (likely((u32)cqe->res == c->egress_batch_wire_len)) {
            if (likely(!c->egress_batch_qos0_refs)) {
                // Ultra-fast: no QoS 0 refs — bulk pop without touching segments
                if (batch <= c->egress_count) {
                    c->egress_head = (c->egress_head + batch) & c->egress_mask;
                    c->egress_count -= batch;
                } else {
                    c->egress_head = (c->egress_head + c->egress_count) & c->egress_mask;
                    c->egress_count = 0;
                }
            } else {
                // QoS 0 refs present — iterate to release
                for (u8 i = 0; i < batch && c->egress_count > 0; i++) {
                    struct egress_segment *seg =
                        egress_head_seg(c->egress, c->egress_head, c->egress_mask);
                    egress_complete_head(b, c, seg);
                }
            }
        } else {
            // Slow path: partial send — walk segments to find boundary
            u32 bytes_left = (u32)cqe->res;
            for (u8 i = 0; i < batch && c->egress_count > 0 && bytes_left > 0; i++) {
                struct egress_segment *seg =
                    egress_head_seg(c->egress, c->egress_head, c->egress_mask);
                u32 wire_len = (seg->kind == SEG_CTRL)
                                   ? (u32)seg->ctrl_len
                                   : egress_seg_load_wire_len(seg);
                u32 seg_remaining = wire_len - seg->cursor;

                if (bytes_left >= seg_remaining) {
                    bytes_left -= seg_remaining;
                    egress_complete_head(b, c, seg);
                } else {
                    seg->cursor += bytes_left;
                    bytes_left = 0;
                }
            }
        }
    } else if (cqe->res == -EAGAIN || cqe->res == -EINTR) {
        // Transient — retry (cursors already at correct positions)
    } else {
        // Fatal error — discard all segments in batch
        b->drops_send_failed++;
        for (u8 i = 0; i < batch && c->egress_count > 0; i++) {
            struct egress_segment *seg =
                egress_head_seg(c->egress, c->egress_head, c->egress_mask);
            // Undo inflight for first sends (dup=0, packet_id>0)
            if (seg->kind == SEG_PUBLISH && seg->qos > 0 &&
                seg->packet_id > 0 && seg->dup == 0) {
                i32 inf = client_inflight_find_direct(c, seg->packet_id);
                if (inf >= 0) {
                    release_msg_ref(b, c->inflight_hot[inf].msg_idx);
                    client_inflight_free_slot(c, (u16)inf);
                }
                egress_pop(&c->egress_head, &c->egress_count, c->egress_mask);
            } else {
                egress_discard_head(b, c, seg);
            }
        }
    }

    // Chain: flush next batch if queue non-empty
    if (c->egress_count > 0 && c->state == CLIENT_ACTIVE) {
        u8 rc = egress_flush(b, c, slot_idx);
        if (rc == EGRESS_FLUSH_SQ_FULL)
            egress_flush_enqueue(b, slot_idx);
    }
}

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
    case OP_EGRESS:
        handle_egress_cqe(b, cqe);
        break;
    case OP_CLOSE:
        handle_close(b, cqe);
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

// Process pending egress flushes - call after draining CQEs when SQ space is available
// Returns number of successful flushes
INLINE u32 process_egress_flushes(struct broker *b) {
    u32 retried = 0;

    while (!egress_flush_empty(b)) {
        u32 ctx = egress_flush_dequeue(b);
        u32 slot_idx = EGRESS_CTX_SLOT(ctx);
        u8 queued_gen = EGRESS_CTX_GEN(ctx);
        if (slot_idx >= b->max_clients)
            break;

        struct client_slot *c = &b->clients[slot_idx];
        if (c->generation != queued_gen) {
            continue; // Stale queued retry after slot reuse
        }
        if (c->state != CLIENT_ACTIVE || c->egress_count == 0)
            continue;

        u8 rc = egress_flush(b, c, slot_idx);
        if (rc == EGRESS_FLUSH_SUBMITTED) {
            retried++;
            continue;
        }
        if (rc == EGRESS_FLUSH_SQ_FULL) {
            // SQ still full - re-enqueue and stop
            egress_flush_enqueue(b, slot_idx);
            break;
        }
    }

    if (retried > 0)
        b->egress_retries += retried;

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
    u32 recv_used = b->recv_pool.capacity - b->recv_pool.free_count;

    u64 total_drops = b->msgs_dropped + b->drops_inflight_full + b->drops_egress_full +
                      b->drops_send_failed;
    log_info("POOLS: msg=%u/%u recv=%u/%u active=%u drops=%lu (if=%lu eg=%lu sf=%lu)",
             msg_used, b->msg_pool.capacity,
             recv_used, b->recv_pool.capacity,
             b->active_count, total_drops,
             b->drops_inflight_full, b->drops_egress_full, b->drops_send_failed);
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
        process_egress_flushes(b);

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
