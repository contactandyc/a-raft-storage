// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef RAFT_SERVER_H
#define RAFT_SERVER_H

#include <stdint.h>
#include <stdbool.h>
#include <uv.h>
#include "a-raft-core/raft.h"
#include "a-raft-storage/raft_wal.h"
#include "a-raft-transport/raft_codec.h"

#define RAFT_MAX_PEERS 64

#pragma pack(push, 1)
typedef struct {
    uint32_t payload_len;
    uint64_t group_id;
    uint64_t sender_id;
} raft_net_header_t;
#pragma pack(pop)

#define RAFT_APPLY_OK 0
#define RAFT_APPLY_TRANSIENT 1
#define RAFT_APPLY_FATAL 2

#define RAFT_SNAPSHOT_OK 0
#define RAFT_SNAPSHOT_FAILED 1

typedef int (*raft_apply_fn)(void* ctx, const raft_entry_t* entry, uint64_t current_term);
typedef void (*raft_read_cb)(void* ctx, uint64_t read_seq);
typedef int (*raft_snapshot_fn)(void* ctx, uint64_t index, uint64_t term, const char* file_path);

typedef struct raft_node_s raft_node_t;
typedef struct raft_server_s raft_server_t;
typedef struct peer_connection_s peer_connection_t;
typedef struct known_peer_s known_peer_t;

struct peer_connection_s {
    uv_tcp_t handle;
    raft_server_t* server;
    known_peer_t* kp;
    uint8_t* buffer;
    size_t buffer_len;
    size_t buffer_cap;
    uint64_t remote_node_id;
};

struct known_peer_s {
    raft_server_t* server;
    uint64_t node_id;
    char ip[64];
    int port;
    peer_connection_t* conn;
    uv_timer_t reconnect_timer;

    uint8_t* out_queue;
    size_t out_queue_len;
    size_t out_queue_cap;
};

struct raft_server_s {
    uint64_t physical_node_id;
    uv_loop_t* loop;
    uv_tcp_t listener;
    char data_dir[256];

    uint32_t max_groups;
    raft_node_t** groups;

    known_peer_t* known_peers[RAFT_MAX_PEERS];
    uint32_t known_peer_count;

    peer_connection_t* active_peers[RAFT_MAX_PEERS];
    uint32_t active_peer_count;

    bool network_isolated;
};

// Phase 5: Asynchronous I/O Context
typedef struct {
    raft_ready_t ready;
    uint64_t actual_applied_idx;
    uint64_t actual_saved_idx;

    bool meta_changed;
    uint64_t term, vote, commit, snap_idx, snap_term;

    bool snap_success;
    bool io_failed;
} disk_flush_ctx_t;

struct raft_node_s {
    uint64_t group_id;
    raft_server_t* server;

    raft_t* core;
    raft_wal_t wal;

    uint64_t saved_term;
    uint64_t saved_vote;
    uint64_t saved_commit;
    uint64_t saved_applied;
    uint64_t saved_snap_idx;
    uint64_t saved_snap_term;

    uv_timer_t election_timer;
    uv_timer_t heartbeat_timer;

    raft_apply_fn apply_cb;
    void* apply_ctx;

    raft_read_cb read_cb;
    void* read_ctx;

    raft_snapshot_fn snap_cb;
    void* snap_ctx;

    raft_read_state_t pending_reads[128];
    size_t num_pending_reads;

    // Phase 5: Async I/O State & Mailbox
    bool is_flushing;
    uv_work_t flush_work;
    disk_flush_ctx_t flush_ctx;

    raft_msg_t* inbound_queue;
    size_t inbound_queue_len;
    size_t inbound_queue_cap;

    bool fatal_error;
};

int  raft_server_init(raft_server_t* server, uv_loop_t* loop, uint64_t node_id, uint32_t max_groups, const char* data_dir);
int  raft_server_listen(raft_server_t* server, const char* ip, int port);
void raft_server_connect(raft_server_t* server, const char* ip, int port, uint64_t target_node_id);

void raft_node_init(raft_node_t* node, raft_server_t* server, uint64_t group_id, uint64_t* init_peers, size_t num_peers,
                    raft_apply_fn apply_cb, void* apply_ctx, raft_read_cb read_cb, void* read_ctx,
                    raft_snapshot_fn snap_cb, void* snap_ctx);

int  raft_node_propose(raft_node_t* node, const uint8_t* payload, uint32_t len, uint64_t client_id, uint64_t client_seq, uint64_t* out_leader_id);
int  raft_node_read_index(raft_node_t* node, uint64_t read_seq, uint64_t* out_leader_id);
int  raft_node_compact(raft_node_t* node, uint64_t compact_index);

// Phase 5: Clean Configuration APIs
int  raft_node_add_server(raft_node_t* node, uint64_t target_node_id);
int  raft_node_promote_server(raft_node_t* node, uint64_t target_node_id);
int  raft_node_remove_server(raft_node_t* node, uint64_t target_node_id);

#endif // RAFT_SERVER_H
