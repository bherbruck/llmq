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
// This just pushes pending SQEs to kernel - no CQE processing to avoid recursion
INLINE struct io_uring_sqe *get_sqe_with_flush(struct broker *b) {
    struct io_uring_sqe *sqe = ring_get_sqe(&b->ring);
    if (sqe)
        return sqe;

    // SQ full - flush pending SQEs to kernel (non-blocking)
    // This lets kernel start processing so SQ head advances
    ring_submit(&b->ring, 0);

    // Try again
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

INLINE void submit_recv(struct broker *b, struct client_slot *c) {
    struct io_uring_sqe *sqe = get_sqe_with_flush(b);
    if (!sqe) {
        log_error("SQ full, can't submit recv");
        return;
    }
    u8 *buf   = c->recv_buf + c->recv_len;
    u32 space = RECV_BUF_SIZE - c->recv_len;
    ring_prep_recv(sqe, c->fd, buf, space, 0);
    sqe->user_data = make_user_data(OP_RECV, (u32)c->fd, 0);
    ring_submit_sqe(&b->ring);
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

// Submit send with context for tracking inflight buffers
INLINE void submit_send_ctx(struct broker *b, i32 fd, const u8 *buf, u32 len, u32 ctx) {
    struct io_uring_sqe *sqe = get_sqe_with_flush(b);
    if (!sqe) {
        log_error("SQ full, can't submit send");
        return;
    }
    ring_prep_send(sqe, fd, buf, len, 0);
    sqe->user_data = make_user_data(OP_SEND, (u32)fd, ctx);
    ring_submit_sqe(&b->ring);
}

INLINE void submit_close(struct broker *b, i32 fd) {
    struct io_uring_sqe *sqe = get_sqe_with_flush(b);
    if (!sqe) {
        log_error("SQ full, can't submit close");
        return;
    }
    ring_prep_close(sqe, fd);
    sqe->user_data = make_user_data(OP_CLOSE, (u32)fd, 0);
    ring_submit_sqe(&b->ring);
}

// Include handler after submit_send is defined
#include "broker/handler.h"

// =============================================================================
// CQE Handlers
// =============================================================================

INLINE void handle_accept(struct broker *b, struct io_uring_cqe *cqe) {
    i32 client_fd = cqe->res;

    if (client_fd < 0) {
        log_error("accept() failed: %d", client_fd);
        submit_accept(b);
        return;
    }

    if ((u32)client_fd >= b->max_fds) {
        log_error("fd too large: %d", client_fd);
        sys_close(client_fd);
        submit_accept(b);
        return;
    }

    b->accepts++;

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
    while (c->recv_len > 0) {
        i32 packet_len = mqtt_packet_complete(c->recv_buf, c->recv_len);

        if (packet_len == MQTT_INCOMPLETE) {
            break;
        }

        if (packet_len < 0) {
            log_warn("Malformed packet from fd=%d", c->fd);
            submit_close(b, c->fd);
            return;
        }

        i32 consumed = process_mqtt_packet(b, c, c->recv_buf, (u32)packet_len);

        if (consumed < 0) {
            submit_close(b, c->fd);
            return;
        }

        u32 remaining = c->recv_len - (u32)consumed;
        if (remaining > 0) {
            memmove(c->recv_buf, c->recv_buf + consumed, remaining);
        }
        c->recv_len = remaining;
    }
}

INLINE void handle_recv(struct broker *b, struct io_uring_cqe *cqe) {
    i32 fd                = (i32)ud_fd(cqe->user_data);
    struct client_slot *c = broker_get_client_by_fd(b, fd);

    if (!c || c->state != CLIENT_ACTIVE) {
        return;
    }

    if (cqe->res <= 0) {
        if (cqe->res < 0) {
            log_error("recv error fd=%d err=%d", fd, cqe->res);
        }
        submit_close(b, fd);
        return;
    }

    c->recv_len += (u32)cqe->res;
    b->bytes_recv += (u64)cqe->res;

    process_recv_buffer(b, c);

    if (c->state == CLIENT_ACTIVE) {
        submit_recv(b, c);
    }
}

INLINE void handle_send(struct broker *b, struct io_uring_cqe *cqe) {
    i32 fd  = (i32)ud_fd(cqe->user_data);
    u32 ctx = ud_ctx(cqe->user_data);

    if (cqe->res < 0) {
        log_debug("send error fd=%d err=%d", fd, cqe->res);
        // Connection broken - close it
        struct client_slot *c = broker_get_client_by_fd(b, fd);
        if (c && c->state == CLIENT_ACTIVE) {
            submit_close(b, fd);
        }
    } else {
        b->bytes_sent += (u64)cqe->res;
    }

    // Free inflight buffer for QoS 0 sends (QoS 1/2 wait for ACK)
    if (ctx != 0) {
        u32 slot_idx    = send_ctx_slot(ctx);
        u8 inflight_idx = send_ctx_inflight(ctx);
        u8 qos          = send_ctx_qos(ctx);

        struct client_slot *c = broker_get_client(b, slot_idx);
        if (c && inflight_idx < MAX_INFLIGHT) {
            struct inflight_msg *inf = &c->inflight[inflight_idx];
            // QoS 0: Free immediately on send complete
            // QoS 1/2: Buffer stays until PUBACK/PUBCOMP (or send error)
            if (qos == 0 || cqe->res < 0) {
                client_inflight_free(c, inf);
            }
        }
    }
}

INLINE void handle_close(struct broker *b, struct io_uring_cqe *cqe) {
    i32 fd = (i32)ud_fd(cqe->user_data);

    // Find slot and handle disconnect
    struct client_slot *c = broker_get_client_by_fd(b, fd);
    if (c) {
        i32 slot_idx = b->fd_to_slot[fd];
        log_debug("client disconnected fd=%d slot=%d", fd, slot_idx);
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
    case OP_CLOSE:
        handle_close(b, cqe);
        break;
    default:
        log_warn("Unknown op: %d", op);
        break;
    }
}

// =============================================================================
// Main Event Loop
// =============================================================================

INLINE i32 broker_run(struct broker *b) {
    b->running = true;
    submit_accept(b);

    log_info("Entering event loop...");

    while (b->running) {
        i32 rc = ring_submit(&b->ring, 1);
        if (rc < 0) {
            log_error("io_uring_enter failed: %d", rc);
            return rc;
        }

        struct io_uring_cqe *cqe;
        while ((cqe = ring_peek_cqe(&b->ring)) != NULL) {
            dispatch_cqe(b, cqe);
            ring_cq_advance(&b->ring, 1);
        }
    }

    return 0;
}

#endif // BROKER_LOOP_H
