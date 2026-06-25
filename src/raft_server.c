// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-raft-node/raft_server.h"
#include "a-raft-node/raft_io.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>

// ============================================================================
// INTERNAL TYPES & FORWARD DECLARATIONS
// ============================================================================

#pragma pack(push, 1)
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint64_t term;
    uint64_t voted_for;
    uint64_t commit_index;
    uint64_t last_applied;
    uint64_t snapshot_index;
    uint64_t snapshot_term;
    uint32_t num_peers;
} raft_meta_header_t;

typedef struct {
    uint64_t peer_id;
    uint8_t is_learner;
} raft_meta_peer_t;
#pragma pack(pop)

typedef struct {
    uv_write_t req;
    uv_buf_t buf;
} write_req_t;

void raft_node_pump(raft_node_t* node);
static void attempt_reconnect(uv_timer_t* handle);
static void remove_peer(raft_server_t* server, peer_connection_t* peer);
static void router_send_rpc(raft_server_t* server, uint64_t group_id, uint64_t target_node_id, uint8_t* payload, uint32_t len);

static bool queue_inbound_msg(raft_node_t* node, raft_msg_t* msg) {
    if (node->inbound_queue_len >= node->inbound_queue_cap) {
        // Prevent unbounded memory growth if the disk completely stalls
        if (node->inbound_queue_cap >= 10000) {
            raft_msg_free_payloads(msg);
            return false;
        }

        size_t new_cap = node->inbound_queue_cap == 0 ? 16 : node->inbound_queue_cap * 2;
        if (new_cap > 10000) new_cap = 10000; // Clamp the maximum capacity

        raft_msg_t* new_q = realloc(node->inbound_queue, new_cap * sizeof(raft_msg_t));

        if (!new_q) {
            // Fail closed on OOM
            node->fatal_error = true;
            raft_msg_free_payloads(msg);
            return false;
        }
        node->inbound_queue = new_q;
        node->inbound_queue_cap = new_cap;
    }
    node->inbound_queue[node->inbound_queue_len++] = *msg;
    return true;
}

static void queue_local_msg(raft_node_t* node, msg_type_t type) {
    raft_msg_t msg = {0};
    msg.type = type;
    queue_inbound_msg(node, &msg);
}

// ============================================================================
// DISK I/O & PERSISTENCE HELPERS
// ============================================================================
static bool sync_parent_dir(const char* dir_path) {
    int dir_fd = open(dir_path, O_RDONLY | O_DIRECTORY);
    if (dir_fd < 0) return false;
    bool ok = false;
#ifdef __APPLE__
    ok = (fcntl(dir_fd, F_FULLFSYNC, 0) != -1);
#else
    ok = (fsync(dir_fd) == 0);
#endif
    close(dir_fd);
    return ok;
}

static bool save_hardstate(raft_node_t* node, uint64_t term, uint64_t vote, uint64_t commit, uint64_t applied, uint64_t snap_idx, uint64_t snap_term) {
    char tmp_path[512], meta_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s/meta_grp%llu.tmp", node->server->data_dir, (unsigned long long)node->group_id);
    snprintf(meta_path, sizeof(meta_path), "%s/meta_grp%llu.dat", node->server->data_dir, (unsigned long long)node->group_id);

    FILE* f = fopen(tmp_path, "wb");
    if (!f) return false;

    raft_meta_header_t hdr = {
        .magic = 0x4D455441, .version = 1,
        .term = term, .voted_for = vote, .commit_index = commit, .last_applied = applied,
        .snapshot_index = snap_idx, .snapshot_term = snap_term
    };

    uint64_t peers[RAFT_MAX_PEERS];
    bool is_learner[RAFT_MAX_PEERS];
    hdr.num_peers = (uint32_t)raft_peers_ext(node->core, peers, is_learner, RAFT_MAX_PEERS);

    if (fwrite(&hdr, sizeof(raft_meta_header_t), 1, f) != 1) { fclose(f); return false; }

    for (uint32_t i = 0; i < hdr.num_peers; i++) {
        raft_meta_peer_t p = { .peer_id = peers[i], .is_learner = is_learner[i] ? 1 : 0 };
        if (fwrite(&p, sizeof(raft_meta_peer_t), 1, f) != 1) { fclose(f); return false; }
    }

    // Mathematically verify every system call
    bool ok = true;
    if (fflush(f) != 0) ok = false;
    if (ok && fsync(fileno(f)) != 0) ok = false;
    if (fclose(f) != 0) ok = false;

    if (!ok) {
        unlink(tmp_path);
        return false;
    }

    if (rename(tmp_path, meta_path) != 0) return false;

    // FIX 3: Strict directory fsync. Fail closed if the directory is not durably synced!
    if (!sync_parent_dir(node->server->data_dir)) {
        return false;
    }

    return true;
}

// ============================================================================
// TIMERS & SCHEDULING
// ============================================================================

static void on_election_timeout(uv_timer_t* handle) {
    raft_node_t* n = (raft_node_t*)handle->data;
    if (n->is_flushing) {
        queue_local_msg(n, raft_state(n->core) == RAFT_STATE_LEADER ? MSG_CHECK_QUORUM : MSG_HUP);
        return;
    }
    if (raft_state(n->core) == RAFT_STATE_LEADER) {
        raft_msg_t chk = { .type = MSG_CHECK_QUORUM };
        raft_step_local(n->core, &chk);
    } else {
        raft_msg_t hup = { .type = MSG_HUP };
        raft_step_local(n->core, &hup);
    }
    raft_node_pump(n);
}

static void reset_election_timer(raft_node_t* node) {
    uv_timer_stop(&node->election_timer);
    uint64_t timeout = 150 + (rand() % 150);
    uv_timer_start(&node->election_timer, (uv_timer_cb)on_election_timeout, timeout, 0);
}

static void on_heartbeat_tick(uv_timer_t* handle) {
    raft_node_t* n = (raft_node_t*)handle->data;
    if (n->is_flushing) {
        queue_local_msg(n, MSG_TICK);
        return;
    }
    raft_msg_t tick = { .type = MSG_TICK };
    raft_step_local(n->core, &tick);
    raft_node_pump(n);
}

static void pump_update_timers(raft_node_t* node) {
    if (raft_state(node->core) == RAFT_STATE_LEADER) {
        if (!uv_is_active((uv_handle_t*)&node->heartbeat_timer)) {
            uv_timer_start(&node->heartbeat_timer, (uv_timer_cb)on_heartbeat_tick, 50, 50);
        }
        if (!uv_is_active((uv_handle_t*)&node->election_timer)) reset_election_timer(node);
    } else {
        if (uv_is_active((uv_handle_t*)&node->heartbeat_timer)) uv_timer_stop(&node->heartbeat_timer);
        if (!uv_is_active((uv_handle_t*)&node->election_timer)) reset_election_timer(node);
    }
}

// ============================================================================
// PUMP SUBSYSTEMS (EVENT LOOP LOGIC)
// ============================================================================

static void pump_queue_pending_reads(raft_node_t* node, raft_ready_t* ready) {
    for (size_t i = 0; i < ready->num_read_states; i++) {
        if (node->num_pending_reads < 128) {
            node->pending_reads[node->num_pending_reads++] = ready->read_states[i];
        }
    }
}

static void pump_dispatch_network_messages(raft_node_t* node, raft_ready_t* ready) {
    for (size_t i = 0; i < ready->num_messages; i++) {
        raft_msg_t* m = &ready->messages[i];

        if (m->type == MSG_INSTALL_SNAPSHOT && m->snapshot_len == 0 && !m->snapshot_done) {
            char dat_snap[512];
            snprintf(dat_snap, sizeof(dat_snap), "%s/snap_grp%llu.dat", node->server->data_dir, (unsigned long long)node->group_id);
            FILE* sf = fopen(dat_snap, "rb");
            if (sf) {
                fseek(sf, m->snapshot_offset, SEEK_SET);

                // FIX 5: Clamp outbound chunk size to prevent codec frame limit breaches
                size_t chunk_size = RAFT_MAX_PAYLOAD_SIZE;

                m->snapshot_data = malloc(chunk_size);
                if (m->snapshot_data) {
                    size_t bytes_read = fread(m->snapshot_data, 1, chunk_size, sf);
                    m->snapshot_len = bytes_read;
                    m->snapshot_done = (bytes_read < chunk_size);
                }
                fclose(sf);
            } else m->snapshot_done = true;
        }

        size_t enc_sz = raft_msg_encoded_size(m);
        uint8_t* payload = malloc(enc_sz);
        if (payload) {
            if (raft_msg_encode(m, payload, enc_sz)) {
                router_send_rpc(node->server, node->group_id, m->to, payload, (uint32_t)enc_sz);
            } else free(payload);
        }

        if (m->type == MSG_INSTALL_SNAPSHOT && m->snapshot_data) {
            free(m->snapshot_data); m->snapshot_data = NULL;
        }
    }
}

static bool pump_apply_state_machine(raft_node_t* node, raft_ready_t* ready, uint64_t* actual_applied_idx) {
    bool applied_changed = false;
    uint64_t current_term = raft_term(node->core);

    if (ready->num_committed_entries > 0) {
        for (size_t i = 0; i < ready->num_committed_entries; i++) {
            raft_entry_t* e = &ready->committed_entries[i];
            if (e->index <= *actual_applied_idx) continue;

            if (node->apply_cb) {
                int res = node->apply_cb(node->apply_ctx, e, current_term);
                if (res == RAFT_APPLY_TRANSIENT) break;
                else if (res == RAFT_APPLY_FATAL) {
                    node->fatal_error = true;
                    raft_msg_t hup = { .type = MSG_CHECK_QUORUM };
                    raft_step_local(node->core, &hup);
                    break;
                }
            }
            *actual_applied_idx = e->index;
            applied_changed = true;
        }
    }
    return applied_changed;
}

static void pump_resolve_read_indices(raft_node_t* node, uint64_t actual_applied_idx) {
    for (size_t i = 0; i < node->num_pending_reads; ) {
        if (node->pending_reads[i].index <= actual_applied_idx) {
            if (node->read_cb) node->read_cb(node->read_ctx, node->pending_reads[i].read_seq);

            node->num_pending_reads--;
            if (i < node->num_pending_reads) node->pending_reads[i] = node->pending_reads[node->num_pending_reads];
        } else i++;
    }
}

static void pump_cleanup_ready(raft_ready_t* ready) {
    if (ready->num_entries_to_save > 0 && ready->entries_to_save) free(ready->entries_to_save);
    if (ready->num_committed_entries > 0 && ready->committed_entries) free(ready->committed_entries);
}

// ============================================================================
// PHASE 5: ASYNCHRONOUS THREAD POOL WORKER
// ============================================================================

static void pump_worker_thread(uv_work_t* req) {
    raft_node_t* node = (raft_node_t*)req->data;
    disk_flush_ctx_t* ctx = &node->flush_ctx;
    raft_ready_t* ready = &ctx->ready;

    // 1. Flush Snapshot Chunk to Disk
    if (ready->install_snapshot) {
        char tmp_snap[512], dat_snap[512];
        snprintf(tmp_snap, sizeof(tmp_snap), "%s/snap_grp%llu.tmp", node->server->data_dir, (unsigned long long)node->group_id);
        snprintf(dat_snap, sizeof(dat_snap), "%s/snap_grp%llu.dat", node->server->data_dir, (unsigned long long)node->group_id);

        // FIX 2: Use O_TRUNC to wipe stale tmp files on chunk 0
        int flags = O_CREAT | O_WRONLY;
        if (ready->snapshot_offset == 0) flags |= O_TRUNC;

        int fd = open(tmp_snap, flags, 0644);
        if (fd < 0) {
            ctx->snap_success = false;
        } else {
            bool ok = true;
            if (ready->snapshot_len > 0) {
                ssize_t written = pwrite(fd, ready->snapshot_data, ready->snapshot_len, ready->snapshot_offset);
                if (written < 0 || (size_t)written != ready->snapshot_len) {
                    ok = false;
                }
            }

            // FIX 2: Chop off trailing garbage bytes if a previous larger snapshot aborted
            if (ready->snapshot_done && ok) {
                off_t final_size = ready->snapshot_offset + ready->snapshot_len;
                if (ftruncate(fd, final_size) != 0) ok = false;
            }

            if (ok && fsync(fd) != 0) ok = false;
            if (close(fd) != 0) ok = false;

            if (!ok) {
                ctx->snap_success = false;
            } else if (ready->snapshot_done) {
                if (rename(tmp_snap, dat_snap) != 0) {
                    ctx->snap_success = false;
                } else {
                    // FIX 5: Directory Fsync to guarantee the rename survived!
                    if (!sync_parent_dir(node->server->data_dir)) {
                        ctx->snap_success = false;
                    }
                }
            }
        }
    }

    // 2. Persist WAL Entries
    if (ready->num_entries_to_save > 0) {
        if (!raft_io_save(&node->wal, ready)) {
            ctx->io_failed = true;
            return;
        }
    }

    // 3. Persist Hardstate
    if (ctx->meta_changed && !ctx->io_failed) {
        if (!save_hardstate(node, ctx->term, ctx->vote, ctx->commit, ctx->actual_applied_idx, ctx->snap_idx, ctx->snap_term)) {
            ctx->io_failed = true;
        }
    }
}

static void pump_after_work(uv_work_t* req, int status) {
    (void)status;
    raft_node_t* node = (raft_node_t*)req->data;
    disk_flush_ctx_t* ctx = &node->flush_ctx;
    raft_ready_t* ready = &ctx->ready;

    if (ctx->io_failed) {
        fprintf(stderr, "[FATAL] Raft Async Disk I/O failed! Halting node.\n");
        node->fatal_error = true;
        goto finalize;
    }

    uint64_t highest_saved_index = ctx->actual_saved_idx;
    if (ready->num_entries_to_save > 0) {
        uint64_t last_in_batch = ready->entries_to_save[ready->num_entries_to_save - 1].index;
        if (last_in_batch > highest_saved_index) {
            highest_saved_index = last_in_batch;
        }
    }

    // Capture this flag securely before the pointer is overwritten below
    bool just_installed_snapshot = false;

    if (ready->install_snapshot) {
        if (ready->snapshot_done && ctx->snap_success) {
            char dat_snap[512];
            snprintf(dat_snap, sizeof(dat_snap), "%s/snap_grp%llu.dat", node->server->data_dir, (unsigned long long)node->group_id);

            if (node->snap_cb && node->snap_cb(node->snap_ctx, ready->snapshot_index, ready->snapshot_term, dat_snap) != RAFT_SNAPSHOT_OK) {
                ctx->snap_success = false;
            }

            if (ctx->snap_success) {
                node->saved_snap_idx = ready->snapshot_index;
                node->saved_snap_term = ready->snapshot_term;
                node->saved_applied = ready->snapshot_index;
                node->saved_commit = ready->snapshot_index;

                ctx->actual_applied_idx = ready->snapshot_index > ctx->actual_applied_idx ? ready->snapshot_index : ctx->actual_applied_idx;
                highest_saved_index = ready->snapshot_index > highest_saved_index ? ready->snapshot_index : highest_saved_index;

                just_installed_snapshot = true;
            }
        }

        raft_snapshot_acked(node->core, ctx->snap_success);

        if (ctx->snap_success && ready->snapshot_done) {
            uint64_t new_tail = raft_last_index(node->core);
            if (new_tail <= node->wal.max_disk_index) {
                raft_wal_truncate_tail(&node->wal, new_tail + 1);
            }
        }

        pump_cleanup_ready(ready);
        *ready = raft_get_ready(node->core);
    }

    pump_dispatch_network_messages(node, ready);

    bool applied_changed = pump_apply_state_machine(node, ready, &ctx->actual_applied_idx);
    pump_resolve_read_indices(node, ctx->actual_applied_idx);

    raft_advance(node->core, highest_saved_index, ctx->actual_applied_idx);

    // FIX 1: Post-apply save checks OS durability. WAL purge is strictly deferred until hardstate rename completes.
    if (applied_changed || ctx->actual_applied_idx > node->saved_applied || ctx->meta_changed || just_installed_snapshot) {
        uint64_t snap_idx = raft_snapshot_index(node->core);

        if (!save_hardstate(node, raft_term(node->core), raft_voted_for(node->core), raft_commit_index(node->core), ctx->actual_applied_idx, snap_idx, raft_snapshot_term(node->core))) {
            node->fatal_error = true;
            goto finalize;
        }

        if (just_installed_snapshot) {
            if (raft_wal_purge_head(&node->wal, snap_idx) != 0) {
                node->fatal_error = true;
                goto finalize;
            }
        }

        node->saved_applied = ctx->actual_applied_idx;
    }

    node->saved_term = raft_term(node->core);
    node->saved_vote = raft_voted_for(node->core);
    node->saved_commit = raft_commit_index(node->core);
    node->saved_snap_idx = raft_snapshot_index(node->core);
    node->saved_snap_term = raft_snapshot_term(node->core);

    pump_update_timers(node);

finalize:
    pump_cleanup_ready(ready);
    node->is_flushing = false;

    // FIX 1.1: If fatally wounded by a disk drop, instantly shed inbound load
    if (node->fatal_error) {
        for (size_t i = 0; i < node->inbound_queue_len; i++) {
            raft_msg_free_payloads(&node->inbound_queue[i]);
        }
        node->inbound_queue_len = 0;
        return;
    }

    bool requires_repump = false;
    for (size_t i = 0; i < node->inbound_queue_len; i++) {
        raft_msg_t* m = &node->inbound_queue[i];
        if (m->type == MSG_TICK || m->type == MSG_HUP || m->type == MSG_CHECK_QUORUM || m->type == MSG_PROPOSE || m->type == MSG_READ_INDEX) {
            raft_step_local(node->core, m);
        } else {
            raft_step_remote(node->core, m);
            if (raft_activity_accepted(node->core)) reset_election_timer(node);
        }
        raft_msg_free_payloads(m);
        requires_repump = true;
    }
    node->inbound_queue_len = 0;

    if (requires_repump) raft_node_pump(node);
}

// ----------------------------------------------------------------------------
// PRIMARY EVENT LOOP ORCHESTRATOR
// ----------------------------------------------------------------------------

void raft_node_pump(raft_node_t* node) {
    if (node->fatal_error || !node->core || raft_has_fatal_error(node->core)) return;
    if (node->is_flushing) return;

    raft_ready_t ready = raft_get_ready(node->core);
    uint64_t actual_saved_idx = raft_last_index(node->core) - ready.num_entries_to_save;
    uint64_t actual_applied_idx = node->saved_applied;

    pump_queue_pending_reads(node, &ready);

    uint64_t current_term = raft_term(node->core);
    uint64_t voted_for = raft_voted_for(node->core);
    uint64_t commit_idx = raft_commit_index(node->core);
    uint64_t snap_idx = raft_snapshot_index(node->core);
    uint64_t snap_term = raft_snapshot_term(node->core);

    bool meta_changed = (current_term != node->saved_term || voted_for != node->saved_vote || commit_idx > node->saved_commit || snap_idx > node->saved_snap_idx);
    bool needs_disk = ready.install_snapshot || ready.num_entries_to_save > 0 || meta_changed;

    if (needs_disk) {
        node->is_flushing = true;
        node->flush_ctx.ready = ready;
        node->flush_ctx.actual_applied_idx = actual_applied_idx;
        node->flush_ctx.actual_saved_idx = actual_saved_idx;
        node->flush_ctx.term = current_term;
        node->flush_ctx.vote = voted_for;
        node->flush_ctx.commit = commit_idx;
        node->flush_ctx.snap_idx = snap_idx;
        node->flush_ctx.snap_term = snap_term;
        node->flush_ctx.meta_changed = meta_changed;
        node->flush_ctx.snap_success = true;
        node->flush_ctx.io_failed = false;

        node->flush_work.data = node;

        // FIX 3: Check uv_queue_work return value and safely abort if the thread pool is overwhelmed
        int rc = uv_queue_work(node->server->loop, &node->flush_work, pump_worker_thread, pump_after_work);
        if (rc != 0) {
            node->is_flushing = false;
            pump_cleanup_ready(&ready);
            node->fatal_error = true;
            return;
        }
    } else {
        pump_dispatch_network_messages(node, &ready);
        bool applied_changed = pump_apply_state_machine(node, &ready, &actual_applied_idx);
        pump_resolve_read_indices(node, actual_applied_idx);
        raft_advance(node->core, actual_saved_idx, actual_applied_idx);

        if (applied_changed || actual_applied_idx > node->saved_applied) {
            // FIX 4: Fast-path hardstate must fail closed if the directory or file metadata sync fails
            if (!save_hardstate(node, node->saved_term, node->saved_vote, node->saved_commit, actual_applied_idx, node->saved_snap_idx, node->saved_snap_term)) {
                node->fatal_error = true;
                pump_cleanup_ready(&ready);
                return;
            }
            node->saved_applied = actual_applied_idx;
        }
        pump_update_timers(node);
        pump_cleanup_ready(&ready);
    }
}

// ============================================================================
// OUTBOUND NETWORKING SUBSYSTEM
// ============================================================================

static void on_write_done(uv_write_t* req, int status) {
    (void)status;
    write_req_t* wr = (write_req_t*)req;
    if (wr->buf.base) free(wr->buf.base);
    free(wr);
}

static void flush_outbound(known_peer_t* kp) {
    if (!kp->conn || kp->out_queue_len == 0) return;

    if (kp->conn->handle.write_queue_size > 10 * 1024 * 1024) return;

    write_req_t* wr = malloc(sizeof(write_req_t));
    if (!wr) return;

    wr->buf = uv_buf_init((char*)kp->out_queue, kp->out_queue_len);

    if (uv_write(&wr->req, (uv_stream_t*)&kp->conn->handle, &wr->buf, 1, on_write_done) != 0) {
        free(wr->buf.base);
        free(wr);
    }

    kp->out_queue_cap = 65536;
    kp->out_queue = malloc(kp->out_queue_cap);
    kp->out_queue_len = 0;
}

bool enqueue_outbound_frame(known_peer_t* kp, const uint8_t* frame, uint32_t frame_len) {
    if (!kp->out_queue && kp->out_queue_cap > 0) return false; // Guard against previous failed alloc

    uint32_t needed = kp->out_queue_len + frame_len;
    if (needed < kp->out_queue_len) return false; // Overflow check on total length

    if (needed > kp->out_queue_cap) {
        uint32_t new_cap = kp->out_queue_cap > 0 ? kp->out_queue_cap : 65536;
        while (new_cap < needed) {
            uint32_t prev = new_cap;
            new_cap *= 2;
            if (new_cap <= prev) return false; // FIX 2: Guard against capacity multiplication overflow
        }

        uint8_t* new_q = realloc(kp->out_queue, new_cap);
        if (!new_q) return false;

        kp->out_queue = new_q;
        kp->out_queue_cap = new_cap;
    }

    memcpy(kp->out_queue + kp->out_queue_len, frame, frame_len);
    kp->out_queue_len += frame_len;
    return true;
}

static void router_send_rpc(raft_server_t* server, uint64_t group_id, uint64_t target_node_id, uint8_t* payload, uint32_t len) {
    if (server->network_isolated) {
        free(payload);
        return;
    }

    uint32_t frame_size = sizeof(raft_net_header_t) + len;
    uint8_t* frame = malloc(frame_size);
    if (!frame) {
        free(payload);
        return;
    }

    raft_net_header_t* h = (raft_net_header_t*)frame;
    h->payload_len = len;
    h->group_id = group_id;
    h->sender_id = server->physical_node_id;
    memcpy(frame + sizeof(raft_net_header_t), payload, len);

    free(payload);

    // Route to Known Peers (Outbound connections)
    for (uint32_t i = 0; i < server->known_peer_count; i++) {
        known_peer_t* kp = server->known_peers[i];
        if (kp->node_id == target_node_id) {
            if (enqueue_outbound_frame(kp, frame, frame_size) && kp->conn) flush_outbound(kp);
            free(frame);
            return;
        }
    }

    // Route to Active Peers (Inbound connections)
    for (uint32_t i = 0; i < server->active_peer_count; i++) {
        peer_connection_t* peer = server->active_peers[i];
        if (peer->remote_node_id == target_node_id) {

            if (peer->handle.write_queue_size > 10 * 1024 * 1024) {
                free(frame); return;
            }

            write_req_t* wr = malloc(sizeof(write_req_t));
            if (!wr) { free(frame); return; }

            wr->buf = uv_buf_init((char*)frame, frame_size);
            if (uv_write(&wr->req, (uv_stream_t*)&peer->handle, &wr->buf, 1, on_write_done) != 0) {
                free(frame);
                free(wr);
            }
            return;
        }
    }

    free(frame); // Drop if peer not found
}

// ============================================================================
// INBOUND NETWORKING SUBSYSTEM
// ============================================================================

static void alloc_cb(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    (void)handle;
    buf->base = malloc(suggested_size);
    if (!buf->base) buf->len = 0;
    else buf->len = suggested_size;
}

static void on_client_close(uv_handle_t* handle) {
    peer_connection_t* peer = (peer_connection_t*)handle;
    if (peer->kp) {
        peer->kp->conn = NULL;
        uv_timer_start(&peer->kp->reconnect_timer, (uv_timer_cb)attempt_reconnect, 1000, 0);
    }
    if (peer->buffer) free(peer->buffer);
    free(peer);
}

static void remove_peer(raft_server_t* server, peer_connection_t* peer) {
    for (uint32_t i = 0; i < server->active_peer_count; i++) {
        if (server->active_peers[i] == peer) {
            server->active_peers[i] = server->active_peers[--server->active_peer_count];
            return;
        }
    }
}

static void register_peer(raft_server_t* server, peer_connection_t* peer) {
    if (server->active_peer_count < RAFT_MAX_PEERS) {
        server->active_peers[server->active_peer_count++] = peer;
    }
}

static void close_and_cleanup_peer(peer_connection_t* peer, const uv_buf_t* buf) {
    remove_peer(peer->server, peer);
    uv_close((uv_handle_t*)&peer->handle, on_client_close);
    if (buf->base) free(buf->base);
}

static bool ensure_peer_buffer_capacity(peer_connection_t* peer, size_t needed) {
    if (needed < peer->buffer_len) return false;

    size_t new_cap = peer->buffer_cap > 0 ? peer->buffer_cap * 2 : 65536;
    while (new_cap < needed) {
        size_t old_cap = new_cap;
        new_cap *= 2;
        if (new_cap < old_cap) return false;
    }

    size_t max_allowed = RAFT_MAX_FRAME_SIZE + sizeof(raft_net_header_t);
    if (new_cap > max_allowed) new_cap = max_allowed;

    if (needed > new_cap) return false;

    uint8_t* temp = realloc(peer->buffer, new_cap);
    if (!temp) return false;

    peer->buffer = temp;
    peer->buffer_cap = new_cap;
    return true;
}

static size_t parse_peer_frames(peer_connection_t* peer) {
    size_t offset = 0;

    while (peer->buffer_len - offset >= sizeof(raft_net_header_t)) {
        raft_net_header_t *h = (raft_net_header_t *)(peer->buffer + offset);

        if (h->payload_len > RAFT_MAX_FRAME_SIZE) return SIZE_MAX; // Fatal

        uint32_t frame_size = sizeof(raft_net_header_t) + h->payload_len;
        if (peer->buffer_len - offset < frame_size) break;

        if (peer->remote_node_id != 0 && peer->remote_node_id != h->sender_id) return SIZE_MAX;
        peer->remote_node_id = h->sender_id;

        uint8_t *payload = peer->buffer + offset + sizeof(raft_net_header_t);
        if (h->group_id < peer->server->max_groups && peer->server->groups[h->group_id]) {
            raft_node_t* target_node = peer->server->groups[h->group_id];
            raft_msg_t msg;

            if (raft_msg_decode(payload, h->payload_len, &msg)) {
                msg.from = h->sender_id;

                if (msg.to == target_node->server->physical_node_id) {
                    if (target_node->is_flushing) {
                        queue_inbound_msg(target_node, &msg);
                    } else {
                        raft_step_remote(target_node->core, &msg);
                        if (raft_activity_accepted(target_node->core)) reset_election_timer(target_node);
                        raft_msg_free_payloads(&msg);
                        raft_node_pump(target_node);
                    }
                } else raft_msg_free_payloads(&msg);
            }
        }
        offset += frame_size;
    }
    return offset;
}

static void on_read(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {
    peer_connection_t *peer = (peer_connection_t *)client;

    if (nread <= 0 || peer->server->network_isolated) {
        close_and_cleanup_peer(peer, buf);
        return;
    }

    if (peer->buffer_len + nread > peer->buffer_cap) {
        if (!ensure_peer_buffer_capacity(peer, peer->buffer_len + nread)) {
            close_and_cleanup_peer(peer, buf);
            return;
        }
    }

    memcpy(peer->buffer + peer->buffer_len, buf->base, nread);
    peer->buffer_len += nread;

    size_t offset = parse_peer_frames(peer);

    if (offset == SIZE_MAX) {
        close_and_cleanup_peer(peer, buf);
        return;
    }

    if (offset > 0 && offset < peer->buffer_len) {
        memmove(peer->buffer, peer->buffer + offset, peer->buffer_len - offset);
    }
    peer->buffer_len -= offset;

    if (buf->base) free(buf->base);
}

// ============================================================================
// CONNECTION LIFECYCLE
// ============================================================================
static void free_temp_handle_cb(uv_handle_t* h) {
    free(h);
}

static void on_failed_peer_close(uv_handle_t* h) {
    peer_connection_t* peer = (peer_connection_t*)h->data;
    if (peer->buffer) free(peer->buffer);
    free(peer);
}

static void on_new_connection(uv_stream_t *server_stream, int status) {
    if (status < 0) return;
    raft_server_t *server = (raft_server_t*)server_stream->data;

    peer_connection_t *peer = calloc(1, sizeof(peer_connection_t));
    if (!peer) {
        // FIX 3: Safe, heap-allocated dropping of connections under OOM
        uv_tcp_t* temp = malloc(sizeof(uv_tcp_t));
        if (temp && uv_tcp_init(server->loop, temp) == 0) {
            if (uv_accept(server_stream, (uv_stream_t*)temp) == 0) {
                uv_close((uv_handle_t*)temp, free_temp_handle_cb);
            } else {
                uv_close((uv_handle_t*)temp, free_temp_handle_cb);
            }
        } else if (temp) {
            free(temp);
        }
        return;
    }

    peer->buffer_cap = 65536;
    peer->buffer = malloc(peer->buffer_cap);
    if (!peer->buffer || uv_tcp_init(server->loop, &peer->handle) != 0) {
        if (peer->buffer) free(peer->buffer);
        free(peer);

        uv_tcp_t* temp = malloc(sizeof(uv_tcp_t));
        if (temp && uv_tcp_init(server->loop, temp) == 0) {
            if (uv_accept(server_stream, (uv_stream_t*)temp) == 0) uv_close((uv_handle_t*)temp, free_temp_handle_cb);
            else uv_close((uv_handle_t*)temp, free_temp_handle_cb);
        } else if (temp) free(temp);
        return;
    }

    peer->handle.data = peer; // Map handle back to struct for close callback
    peer->server = server;

    if (uv_accept(server_stream, (uv_stream_t*)&peer->handle) == 0) {
        if (server->network_isolated || uv_read_start((uv_stream_t*)&peer->handle, alloc_cb, on_read) != 0) {
            uv_close((uv_handle_t*)&peer->handle, on_failed_peer_close);
            return;
        }
        register_peer(server, peer);
    } else {
        uv_close((uv_handle_t*)&peer->handle, on_failed_peer_close);
    }
}

static void on_connect(uv_connect_t* req, int status) {
    peer_connection_t* peer = (peer_connection_t*)req->data;
    if (status == 0 && !peer->server->network_isolated) {
        register_peer(peer->server, peer);
        if (peer->kp) {
            peer->kp->conn = peer;
            flush_outbound(peer->kp);
        }
        uv_read_start((uv_stream_t*)&peer->handle, alloc_cb, on_read);
    } else {
        uv_close((uv_handle_t*)&peer->handle, on_client_close);
    }
    free(req);
}

static void attempt_reconnect(uv_timer_t* handle) {
    known_peer_t* kp = (known_peer_t*)handle->data;

    if (kp->server->network_isolated || kp->conn) return;

    peer_connection_t* peer = calloc(1, sizeof(peer_connection_t));
    if (!peer) return;

    peer->buffer_cap = 65536;
    peer->buffer = malloc(peer->buffer_cap);
    if (!peer->buffer || uv_tcp_init(kp->server->loop, &peer->handle) != 0) {
        if (peer->buffer) free(peer->buffer);
        free(peer);
        return;
    }

    peer->server = kp->server;
    peer->remote_node_id = kp->node_id;
    peer->kp = kp;
    peer->handle.data = peer; // Map for failed connection cleanup

    struct sockaddr_in dest;
    if (uv_ip4_addr(kp->ip, kp->port, &dest) != 0) {
        uv_close((uv_handle_t*)&peer->handle, on_failed_peer_close);
        return;
    }

    uv_connect_t* req = malloc(sizeof(uv_connect_t));
    if (!req) {
        uv_close((uv_handle_t*)&peer->handle, on_failed_peer_close);
        return;
    }

    req->data = peer;
    if (uv_tcp_connect(req, &peer->handle, (const struct sockaddr*)&dest, on_connect) != 0) {
        free(req);
        uv_close((uv_handle_t*)&peer->handle, on_failed_peer_close);
    }
}

// ============================================================================
// PUBLIC SERVER API
// ============================================================================

int raft_server_init(raft_server_t* server, uv_loop_t* loop, uint64_t node_id, uint32_t max_groups, const char* data_dir) {
    if (node_id == 0 || max_groups == 0) return -1;

    memset(server, 0, sizeof(*server));
    server->physical_node_id = node_id;
    server->loop = loop;
    server->max_groups = max_groups;
    server->network_isolated = false;
    strncpy(server->data_dir, data_dir, sizeof(server->data_dir) - 1);

    server->groups = calloc(max_groups, sizeof(raft_node_t*));

    uv_tcp_init(loop, &server->listener);
    server->listener.data = server;
    return 0;
}

int raft_server_listen(raft_server_t* server, const char* ip, int port) {
    struct sockaddr_in addr;
    uv_ip4_addr(ip, port, &addr);
    uv_tcp_bind(&server->listener, (const struct sockaddr*)&addr, 0);
    return uv_listen((uv_stream_t*)&server->listener, 128, on_new_connection);
}

void raft_server_connect(raft_server_t* server, const char* ip, int port, uint64_t target_node_id) {
    if (server->network_isolated || server->known_peer_count >= RAFT_MAX_PEERS) return;

    known_peer_t* kp = calloc(1, sizeof(known_peer_t));
    kp->server = server;
    kp->node_id = target_node_id;
    strncpy(kp->ip, ip, 63);
    kp->port = port;

    kp->out_queue_cap = 65536;
    kp->out_queue = malloc(kp->out_queue_cap);

    uv_timer_init(server->loop, &kp->reconnect_timer);
    kp->reconnect_timer.data = kp;

    server->known_peers[server->known_peer_count++] = kp;
    attempt_reconnect(&kp->reconnect_timer);
}

// ============================================================================
// PUBLIC NODE API
// ============================================================================
void raft_node_init(raft_node_t* node, raft_server_t* server, uint64_t group_id, uint64_t* init_peers, size_t num_peers,
                    raft_apply_fn apply_cb, void* apply_ctx, raft_read_cb read_cb, void* read_ctx,
                    raft_snapshot_fn snap_cb, void* snap_ctx) {

    memset(node, 0, sizeof(*node));

    if (group_id >= server->max_groups) return;

    if (num_peers > RAFT_MAX_PEERS || (num_peers > 0 && !init_peers)) {
        node->fatal_error = true;
        return;
    }

    char wal_path[512];
    snprintf(wal_path, sizeof(wal_path), "%s/wal_grp%llu", server->data_dir, (unsigned long long)group_id);
    if (raft_wal_init(&node->wal, wal_path, 16, 4) != 0) {
        node->fatal_error = true;
        return;
    }

    uint64_t load_peers[RAFT_MAX_PEERS];
    bool load_learners[RAFT_MAX_PEERS];
    size_t active_peers = num_peers;

    bool found_self = false;
    for (size_t i = 0; i < num_peers; i++) {
        if (init_peers[i] == server->physical_node_id) found_self = true;
        load_peers[i] = init_peers[i];
        load_learners[i] = false;
    }

    if (!found_self && num_peers > 0) {
        if (num_peers >= RAFT_MAX_PEERS) {
            raft_wal_close(&node->wal);
            node->fatal_error = true;
            return;
        }
        load_peers[active_peers] = server->physical_node_id;
        load_learners[active_peers] = false;
        active_peers++;
    }

    uint64_t saved_term = 0, saved_vote = 0, saved_commit = 0, saved_applied = 0;
    uint64_t snap_idx = 0, snap_term = 0;

    char meta_path[512];
    snprintf(meta_path, sizeof(meta_path), "%s/meta_grp%llu.dat", server->data_dir, (unsigned long long)group_id);
    FILE* f = fopen(meta_path, "rb");
    if (f) {
        uint32_t magic;
        if (fread(&magic, sizeof(uint32_t), 1, f) == 1 && magic == 0x4D455441) {
            fseek(f, 0, SEEK_SET);
            raft_meta_header_t hdr;
            if (fread(&hdr, sizeof(raft_meta_header_t), 1, f) == 1 && hdr.version == 1) {
                if (hdr.num_peers > 0 && hdr.num_peers <= RAFT_MAX_PEERS) {
                    active_peers = hdr.num_peers;
                    for (uint32_t i = 0; i < hdr.num_peers; i++) {
                        raft_meta_peer_t p;
                        // FIX 4: Fail hard on short reads to prevent mixed topology arrays
                        if (fread(&p, sizeof(raft_meta_peer_t), 1, f) != 1) {
                            fclose(f);
                            raft_wal_close(&node->wal);
                            node->fatal_error = true;
                            return;
                        }
                        load_peers[i] = p.peer_id;
                        load_learners[i] = (p.is_learner != 0);
                    }
                    // Only accept the state if the whole file was read safely
                    saved_term = hdr.term; saved_vote = hdr.voted_for;
                    saved_commit = hdr.commit_index; saved_applied = hdr.last_applied;
                    snap_idx = hdr.snapshot_index; snap_term = hdr.snapshot_term;
                }
            }
        }
        fclose(f);
    }

    node->core = raft_io_boot(&node->wal, server->physical_node_id, load_peers, load_learners, active_peers,
                              saved_term, saved_vote, saved_commit, saved_applied, snap_idx, snap_term);

    if (!node->core) {
        raft_wal_close(&node->wal);
        node->fatal_error = true;
        return;
    }

    // FIX 4: Only publish the node to the server AFTER full initialization succeeds
    node->group_id = group_id;
    node->server = server;
    node->apply_cb = apply_cb; node->apply_ctx = apply_ctx;
    node->read_cb = read_cb; node->read_ctx = read_ctx;
    node->snap_cb = snap_cb; node->snap_ctx = snap_ctx;

    node->saved_term = saved_term; node->saved_vote = saved_vote;
    node->saved_commit = saved_commit; node->saved_applied = saved_applied;
    node->saved_snap_idx = snap_idx; node->saved_snap_term = snap_term;

    server->groups[group_id] = node;

    if (uv_timer_init(server->loop, &node->election_timer) == 0 &&
        uv_timer_init(server->loop, &node->heartbeat_timer) == 0) {
        node->election_timer.data = node;
        node->heartbeat_timer.data = node;
        reset_election_timer(node);
    } else {
        node->fatal_error = true;
    }
}

int raft_node_propose(raft_node_t* node, const uint8_t* payload, uint32_t len, uint64_t client_id, uint64_t client_seq, uint64_t* out_leader_id) {
    if (node->fatal_error || !node->core || raft_has_fatal_error(node->core)) return RAFT_ERR_NOT_LEADER;
    if (raft_state(node->core) != RAFT_STATE_LEADER) {
        if (out_leader_id) *out_leader_id = raft_leader_id(node->core);
        return RAFT_ERR_NOT_LEADER;
    }

    if (len > RAFT_MAX_PAYLOAD_SIZE) return RAFT_ERR_QUEUE_FULL;
    if (raft_last_index(node->core) - raft_commit_index(node->core) > 2000) return RAFT_ERR_QUEUE_FULL;
    if (raft_uncommitted_bytes(node->core) > 10 * 1024 * 1024) return RAFT_ERR_QUEUE_FULL;

    // Explicitly prevent null payloads when length is greater than 0
    if (len > 0 && !payload) return -1;

    // Single safe allocation path with no leaks
    raft_entry_t* e_arr = calloc(1, sizeof(*e_arr));
    if (!e_arr) return RAFT_ERR_NOMEM;

    e_arr[0].type = ENTRY_NORMAL;
    e_arr[0].client_id = client_id;
    e_arr[0].client_seq = client_seq;
    e_arr[0].data_len = len;

    if (len > 0) {
        e_arr[0].data = malloc(len);
        if (!e_arr[0].data) {
            free(e_arr);
            return RAFT_ERR_NOMEM;
        }
        memcpy(e_arr[0].data, payload, len);
    }

    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = e_arr, .num_entries = 1 };

    if (node->is_flushing) {
        if (!queue_inbound_msg(node, &prop)) return RAFT_ERR_NOMEM;
    } else {
        raft_step_local(node->core, &prop);
        raft_msg_free_payloads(&prop);
        raft_node_pump(node);
    }
    return RAFT_OK;
}

int raft_node_read_index(raft_node_t* node, uint64_t read_seq, uint64_t* out_leader_id) {
    if (node->fatal_error || !node->core || raft_has_fatal_error(node->core)) return RAFT_ERR_NOT_LEADER;

    if (raft_state(node->core) != RAFT_STATE_LEADER) {
        if (out_leader_id) *out_leader_id = raft_leader_id(node->core);
        return RAFT_ERR_NOT_LEADER;
    }

    if (node->is_flushing) {
        raft_msg_t ri = { .type = MSG_READ_INDEX, .read_seq = read_seq };
        // Gracefully reject read indices if the mailbox queue fails
        if (!queue_inbound_msg(node, &ri)) return -1;
    } else {
        raft_msg_t req = { .type = MSG_READ_INDEX, .read_seq = read_seq };
        raft_step_local(node->core, &req);
        raft_node_pump(node);
    }

    return RAFT_OK;
}

int raft_node_compact(raft_node_t* node, uint64_t compact_index) {
    if (node->fatal_error || !node->core || raft_has_fatal_error(node->core)) return RAFT_APPLY_FATAL;

    if (compact_index <= raft_snapshot_index(node->core)) return RAFT_OK;

    // FIX 2: Application Contract Guard. Verify the snapshot file actually exists!
    char dat_snap[512];
    snprintf(dat_snap, sizeof(dat_snap), "%s/snap_grp%llu.dat", node->server->data_dir, (unsigned long long)node->group_id);
    struct stat st;
    if (stat(dat_snap, &st) != 0) {
        return -1;
    }

    uint64_t old_snap = raft_snapshot_index(node->core);
    uint64_t term = raft_log_term(node->core, compact_index);

    raft_compact_after_snapshot(node->core, compact_index, term);

    if (raft_has_fatal_error(node->core)) return RAFT_APPLY_FATAL;

    uint64_t new_snap = raft_snapshot_index(node->core);
    if (new_snap == old_snap || new_snap != compact_index) {
        return -1;
    }

    uint64_t commit = raft_commit_index(node->core);
    uint64_t applied = raft_last_applied(node->core);

    if (!save_hardstate(node, raft_term(node->core), raft_voted_for(node->core), commit, applied, new_snap, raft_snapshot_term(node->core))) {
        return -1;
    }

    node->saved_commit = commit;
    node->saved_applied = applied;
    node->saved_snap_idx = new_snap;
    node->saved_snap_term = raft_snapshot_term(node->core);

    if (raft_wal_purge_head(&node->wal, new_snap) != 0) return -1;
    return RAFT_OK;
}

// ============================================================================
// PUBLIC CONFIGURATION API
// ============================================================================

static int propose_config(raft_node_t* node, entry_type_t type, uint64_t target_node_id) {
    if (node->fatal_error || !node->core || raft_has_fatal_error(node->core)) return RAFT_ERR_NOT_LEADER;
    if (raft_state(node->core) != RAFT_STATE_LEADER) return RAFT_ERR_NOT_LEADER;

    // Blocker 8: Safe memory allocation for configuration proposals
    raft_entry_t* e_arr = calloc(1, sizeof(raft_entry_t));
    if (!e_arr) return RAFT_APPLY_FATAL; // Check primary array allocation

    e_arr[0].type = type;
    e_arr[0].data_len = sizeof(uint64_t);
    e_arr[0].data = malloc(sizeof(uint64_t));

    if (!e_arr[0].data) {
        free(e_arr);
        return RAFT_APPLY_FATAL; // Fail closed if payload allocation fails
    }
    memcpy(e_arr[0].data, &target_node_id, sizeof(uint64_t));

    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = e_arr, .num_entries = 1 };

    if (node->is_flushing) {
        if (!queue_inbound_msg(node, &prop)) {
            // Queue failed (OOM or overflow limits hit), payloads were automatically freed
            return RAFT_APPLY_FATAL;
        }
    } else {
        raft_step_local(node->core, &prop);
        raft_msg_free_payloads(&prop);
        raft_node_pump(node);
    }
    return RAFT_OK;
}

int raft_node_add_server(raft_node_t* node, uint64_t target_node_id) {
    return propose_config(node, ENTRY_CONF_ADD_LEARNER, target_node_id);
}

int raft_node_promote_server(raft_node_t* node, uint64_t target_node_id) {
    return propose_config(node, ENTRY_CONF_PROMOTE_LEARNER, target_node_id);
}

int raft_node_remove_server(raft_node_t* node, uint64_t target_node_id) {
    return propose_config(node, ENTRY_CONF_REMOVE, target_node_id);
}
