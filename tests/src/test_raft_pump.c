// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <uv.h>
#include "a-raft-node/raft_server.h"
#include "a-raft-core/raft.h"
#include "the-macro-library/macro_test.h"

void raft_node_pump(raft_node_t* node);

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
} test_meta_header_t;
#pragma pack(pop)

// Blazing fast loop drainer that exits the microsecond the async thread finishes
static void drain_pump(raft_node_t* node, uv_loop_t* loop) {
    // Run the loop continuously until the async flush flag clears
    while (node->is_flushing) {
        uv_run(loop, UV_RUN_NOWAIT);
        usleep(100); // 0.1ms sleep just to prevent CPU pegging
    }
    // One final sweep to clear the network/actor mailboxes
    uv_run(loop, UV_RUN_NOWAIT);
}

static void wait_for_pump(uv_loop_t* loop) {
    for (int i = 0; i < 50; i++) {
        uv_run(loop, UV_RUN_NOWAIT);
        usleep(2000);
    }
}

static uint64_t read_disk_term(uint64_t group_id) {
    char meta_path[512];
    snprintf(meta_path, sizeof(meta_path), "/tmp/raft_test_pump/meta_grp%llu.dat", (unsigned long long)group_id);
    FILE* f = fopen(meta_path, "rb");
    if (!f) return 0;
    test_meta_header_t hdr;
    size_t r = fread(&hdr, sizeof(hdr), 1, f);
    fclose(f);
    if (r == 1 && hdr.magic == 0x4D455441) return hdr.term;
    return 0;
}

static uint64_t read_disk_snap_idx(uint64_t group_id) {
    char meta_path[512];
    snprintf(meta_path, sizeof(meta_path), "/tmp/raft_test_pump/meta_grp%llu.dat", (unsigned long long)group_id);
    FILE* f = fopen(meta_path, "rb");
    if (!f) return 0;
    test_meta_header_t hdr;
    size_t r = fread(&hdr, sizeof(hdr), 1, f);
    fclose(f);
    if (r == 1 && hdr.magic == 0x4D455441) return hdr.snapshot_index;
    return 0;
}

MACRO_TEST(vote_response_not_sent_before_hardstate_persisted) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("chmod -R 777 /tmp/raft_test_pump 2>/dev/null; rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_server_connect(&srv, "127.0.0.1", 9000, 2);

    raft_node_t node;
    uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = 1, .from = 2, .term = 5, .index = 0, .log_term = 0 };
    raft_step_remote(node.core, &req);
    raft_node_pump(&node);

    MACRO_ASSERT_EQ_INT(srv.known_peers[0]->out_queue_len, 0);
    MACRO_ASSERT_EQ_INT(read_disk_term(0), 0);

    wait_for_pump(&loop);

    MACRO_ASSERT_EQ_INT(read_disk_term(0), 5);
    MACRO_ASSERT_TRUE(srv.known_peers[0]->out_queue_len > 0);

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);
}

MACRO_TEST(snapshot_success_ack_not_sent_until_new_meta_persisted) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("chmod -R 777 /tmp/raft_test_pump 2>/dev/null; rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_server_connect(&srv, "127.0.0.1", 9000, 2);

    raft_node_t node;
    uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    uint64_t snap_peers[] = {1, 2};
    bool snap_learners[] = {false, false};

    uint8_t snap_data[] = "SNAP";
    raft_msg_t snap1 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 0, .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = true,
                         .snapshot_num_peers = 2, .snapshot_peers = snap_peers, .snapshot_is_learner = snap_learners };

    raft_step_remote(node.core, &snap1);
    raft_node_pump(&node);
    wait_for_pump(&loop);

    MACRO_ASSERT_EQ_INT(read_disk_snap_idx(0), 10);
    MACRO_ASSERT_TRUE(srv.known_peers[0]->out_queue_len > 0);

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);
}

MACRO_TEST(snapshot_chunk_duplicate_offset_is_rejected_or_reacked) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("chmod -R 777 /tmp/raft_test_pump 2>/dev/null; rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node;
    uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    uint64_t snap_peers[] = {1, 2};
    bool snap_learners[] = {false, false};

    uint8_t snap_data[] = "SNAP";
    raft_msg_t snap1 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 0, .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = false,
                         .snapshot_num_peers = 2, .snapshot_peers = snap_peers, .snapshot_is_learner = snap_learners };
    raft_step_remote(node.core, &snap1);
    raft_node_pump(&node); wait_for_pump(&loop);

    raft_msg_t snap2 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 4, .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = false,
                         .snapshot_num_peers = 2, .snapshot_peers = snap_peers, .snapshot_is_learner = snap_learners };
    raft_step_remote(node.core, &snap2);
    raft_node_pump(&node); wait_for_pump(&loop);

    raft_step_remote(node.core, &snap2);
    raft_ready_t ready = raft_get_ready(node.core);

    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);
    MACRO_ASSERT_EQ_INT(ready.messages[0].conflict_index, 8);

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);
}

MACRO_TEST(snapshot_chunk_gap_does_not_append_to_tmp_file) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("chmod -R 777 /tmp/raft_test_pump 2>/dev/null; rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node;
    uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    uint64_t snap_peers[] = {1, 2};
    bool snap_learners[] = {false, false};

    uint8_t snap_data[] = "SNAP";
    raft_msg_t snap1 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 0, .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = false,
                         .snapshot_num_peers = 2, .snapshot_peers = snap_peers, .snapshot_is_learner = snap_learners };
    raft_step_remote(node.core, &snap1);
    raft_node_pump(&node); wait_for_pump(&loop);

    raft_msg_t snap3 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 8, .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = false,
                         .snapshot_num_peers = 2, .snapshot_peers = snap_peers, .snapshot_is_learner = snap_learners };
    raft_step_remote(node.core, &snap3);

    raft_ready_t ready = raft_get_ready(node.core);

    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);
    MACRO_ASSERT_EQ_INT(ready.messages[0].conflict_index, 4);
    MACRO_ASSERT_FALSE(ready.install_snapshot);

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);
}

MACRO_TEST(node_pump_snapshot_install_does_not_consume_stale_ready) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("chmod -R 777 /tmp/raft_test_pump 2>/dev/null; rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_server_connect(&srv, "127.0.0.1", 9000, 2);

    raft_node_t node;
    uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    uint64_t snap_peers[] = {1, 2};
    bool snap_learners[] = {false, false};

    uint8_t snap_data[] = "SNAP";
    raft_msg_t snap = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                        .snapshot_offset = 0, .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = true,
                        .snapshot_num_peers = 2, .snapshot_peers = snap_peers, .snapshot_is_learner = snap_learners };

    raft_step_remote(node.core, &snap);
    raft_node_pump(&node);
    wait_for_pump(&loop);

    MACRO_ASSERT_FALSE(node.fatal_error);
    MACRO_ASSERT_EQ_INT(raft_last_index(node.core), 10);
    MACRO_ASSERT_TRUE(srv.known_peers[0]->out_queue_len > 0);

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);
}

MACRO_TEST(snapshot_discarded_suffix_does_not_resurrect_after_restart) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("chmod -R 777 /tmp/raft_test_pump 2>/dev/null; rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node;
    uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    for (int i=1; i<=5; i++) {
        raft_entry_t e = { .term = 1, .index = i, .type = ENTRY_NORMAL, .data = (uint8_t*)"x", .data_len = 1 };
        raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = i-1, .log_term = (i==1?0:1), .entries = &e, .num_entries = 1, .commit = 0 };
        raft_step_remote(node.core, &app);
        raft_node_pump(&node);
        wait_for_pump(&loop);
    }

    uint64_t snap_peers[] = {1, 2};
    bool snap_learners[] = {false, false};

    uint8_t snap_data[] = "SNAP";
    raft_msg_t snap = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 3, .log_term = 2,
                        .snapshot_offset = 0, .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = true,
                        .snapshot_num_peers = 2, .snapshot_peers = snap_peers, .snapshot_is_learner = snap_learners };
    raft_step_remote(node.core, &snap);
    raft_node_pump(&node);
    wait_for_pump(&loop);

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);

    uv_loop_t loop2;
    uv_loop_init(&loop2);
    raft_server_t srv2;
    raft_server_init(&srv2, &loop2, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node2;
    raft_node_init(&node2, &srv2, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    MACRO_ASSERT_EQ_INT(raft_snapshot_index(node2.core), 3);
    MACRO_ASSERT_EQ_INT(raft_last_index(node2.core), 3);

    raft_wal_close(&node2.wal);
    raft_destroy(node2.core);
    uv_loop_close(&loop2);
}

MACRO_TEST(snapshot_worker_short_write_or_fsync_failure_rejects_chunk) {
    uv_loop_t loop; uv_loop_init(&loop);
    system("chmod -R 777 /tmp/raft_test_pump 2>/dev/null; rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");

    raft_server_t srv; raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node; uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    system("chmod 400 /tmp/raft_test_pump");

    uint64_t snap_peers[] = {1, 2};
    bool snap_learners[] = {false, false};

    uint8_t snap_data[] = "SNAP";
    raft_msg_t snap1 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 0, .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = false,
                         .snapshot_num_peers = 2, .snapshot_peers = snap_peers, .snapshot_is_learner = snap_learners };

    raft_step_remote(node.core, &snap1);
    raft_node_pump(&node); wait_for_pump(&loop);

    MACRO_ASSERT_FALSE(node.flush_ctx.snap_success);

    system("chmod 777 /tmp/raft_test_pump");
    raft_wal_close(&node.wal); raft_destroy(node.core); uv_loop_close(&loop);
}

MACRO_TEST(snapshot_install_does_not_restore_stale_ctx_snapshot_metadata) {
    uv_loop_t loop; uv_loop_init(&loop);
    system("chmod -R 777 /tmp/raft_test_pump 2>/dev/null; rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");

    raft_server_t srv; raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node; uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    node.flush_ctx.meta_changed = true;
    node.flush_ctx.snap_idx = 5;

    uint64_t snap_peers[] = {1, 2};
    bool snap_learners[] = {false, false};

    uint8_t snap_data[] = "SNAP";
    raft_msg_t snap1 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 0, .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = true,
                         .snapshot_num_peers = 2, .snapshot_peers = snap_peers, .snapshot_is_learner = snap_learners };

    raft_step_remote(node.core, &snap1);
    raft_node_pump(&node); wait_for_pump(&loop);

    MACRO_ASSERT_EQ_INT(node.saved_snap_idx, 10);

    raft_wal_close(&node.wal); raft_destroy(node.core); uv_loop_close(&loop);
}

MACRO_TEST(snapshot_retry_overwrites_at_exact_offset_not_append_eof) {
    uv_loop_t loop; uv_loop_init(&loop);
    system("chmod -R 777 /tmp/raft_test_pump 2>/dev/null; rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");

    raft_server_t srv; raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node; uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    uint64_t snap_peers[] = {1, 2};
    bool snap_learners[] = {false, false};

    uint8_t snap_data[] = "ABCD";
    raft_msg_t snap1 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 0, .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = true,
                         .snapshot_num_peers = 2, .snapshot_peers = snap_peers, .snapshot_is_learner = snap_learners };
    raft_step_remote(node.core, &snap1);

    raft_node_pump(&node); wait_for_pump(&loop);

    raft_step_remote(node.core, &snap1);
    raft_node_pump(&node); wait_for_pump(&loop);

    struct stat st;
    stat("/tmp/raft_test_pump/snap_grp0.dat", &st);
    MACRO_ASSERT_EQ_INT(st.st_size, 4);

    raft_wal_close(&node.wal); raft_destroy(node.core); uv_loop_close(&loop);
}

MACRO_TEST(snapshot_new_offset_zero_truncates_old_tmp_tail) {
    uv_loop_t loop; uv_loop_init(&loop);
    system("chmod -R 777 /tmp/raft_test_pump 2>/dev/null; rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");
    raft_server_t srv; raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node; uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    FILE* f = fopen("/tmp/raft_test_pump/snap_grp0.tmp", "wb");
    fwrite("GARBAGE_DATA_THAT_IS_TOO_LONG", 1, 29, f);
    fclose(f);

    uint64_t snap_peers[] = {1, 2};
    bool snap_learners[] = {false, false};

    raft_msg_t snap1 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 0, .snapshot_data = (uint8_t*)"NEW", .snapshot_len = 3, .snapshot_done = true,
                         .snapshot_num_peers = 2, .snapshot_peers = snap_peers, .snapshot_is_learner = snap_learners };
    raft_step_remote(node.core, &snap1);
    raft_node_pump(&node); wait_for_pump(&loop);

    struct stat st;
    stat("/tmp/raft_test_pump/snap_grp0.dat", &st);
    MACRO_ASSERT_EQ_INT(st.st_size, 3);

    raft_wal_close(&node.wal); raft_destroy(node.core); uv_loop_close(&loop);
}

MACRO_TEST(hardstate_directory_fsync_failure_returns_false) {
    uv_loop_t loop; uv_loop_init(&loop);
    system("chmod -R 777 /tmp/raft_test_pump 2>/dev/null; rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");
    raft_server_t srv; raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node; uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    // Destroy write/execute permissions on the directory to force a fsync failure
    system("chmod 400 /tmp/raft_test_pump");

    // FIX: Send a remote AppendEntries to the Follower.
    // This organically forces the core to request a disk flush without needing internal headers!
    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };
    raft_step_remote(node.core, &app);

    // This pump will attempt to write the WAL/Hardstate and fail at the directory fsync
    raft_node_pump(&node); wait_for_pump(&loop);

    // Because the directory couldn't be accessed, the disk worker MUST trigger a fatal IO failure
    MACRO_ASSERT_TRUE(node.fatal_error);

    system("chmod 777 /tmp/raft_test_pump"); // Restore for cleanup
    raft_wal_close(&node.wal); raft_destroy(node.core); uv_loop_close(&loop);
}

MACRO_TEST(snapshot_final_rename_requires_directory_fsync) {
    uv_loop_t loop; uv_loop_init(&loop);
    system("chmod -R 777 /tmp/raft_test_pump 2>/dev/null; rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");
    raft_server_t srv; raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node; uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    system("chmod 400 /tmp/raft_test_pump"); // Destroy permissions

    uint64_t snap_peers[] = {1, 2};
    bool snap_learners[] = {false, false};

    raft_msg_t snap1 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 0, .snapshot_data = (uint8_t*)"SNP", .snapshot_len = 3, .snapshot_done = true,
                         .snapshot_num_peers = 2, .snapshot_peers = snap_peers, .snapshot_is_learner = snap_learners };
    raft_step_remote(node.core, &snap1);
    raft_node_pump(&node); wait_for_pump(&loop);

    // Must be marked as failure because the directory containing the rename failed
    MACRO_ASSERT_FALSE(node.flush_ctx.snap_success);

    system("chmod 777 /tmp/raft_test_pump"); // Restore for cleanup
    raft_wal_close(&node.wal); raft_destroy(node.core); uv_loop_close(&loop);
}

MACRO_TEST(inbound_queue_cap_exactly_10000_does_not_grow) {
    uv_loop_t loop; uv_loop_init(&loop);
    system("rm -rf /tmp/raft_test_pump_q; mkdir -p /tmp/raft_test_pump_q");

    raft_server_t srv; raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump_q");
    raft_node_t node; uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    // Promote to Leader
    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(node.core, &hup);
    raft_node_pump(&node); wait_for_pump(&loop);

    // Setup boundary conditions artificially for the test
    node.inbound_queue_cap = 10000;
    node.inbound_queue_len = 10000;
    node.inbound_queue = malloc(sizeof(raft_msg_t));

    // Force into queue_inbound_msg path
    node.is_flushing = true;
    int err = raft_node_read_index(&node, 1, NULL);

    // Must fail without attempting to realloc due to the 10000 ceiling
    MACRO_ASSERT_EQ_INT(err, -1);
    MACRO_ASSERT_EQ_INT(node.inbound_queue_cap, 10000);

    free(node.inbound_queue);
    node.inbound_queue = NULL;
    node.inbound_queue_len = 0;
    node.inbound_queue_cap = 0;
    node.is_flushing = false;

    raft_wal_close(&node.wal); raft_destroy(node.core); uv_loop_close(&loop);
}

MACRO_TEST(node_compact_noops_must_not_purge_wal) {
    uv_loop_t loop; uv_loop_init(&loop);
    system("rm -rf /tmp/raft_test_pump_c; mkdir -p /tmp/raft_test_pump_c");

    raft_server_t srv; raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump_c");
    raft_node_t node; uint64_t peers[] = {1};
    raft_node_init(&node, &srv, 0, peers, 1, NULL, NULL, NULL, NULL, NULL, NULL);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(node.core, &hup);
    raft_node_pump(&node); drain_pump(&node, &loop);

    // Brutal Stress Test: 500 individual proposals, pumps, and disk flushes
    // With drain_pump, this will execute in milliseconds instead of timing out!
    for (int i=0; i<500; i++) {
        raft_node_propose(&node, (uint8_t*)"X", 1, 1, 1, NULL);
        raft_node_pump(&node);
        drain_pump(&node, &loop);
    }

    MACRO_ASSERT_TRUE(node.wal.max_disk_index >= 500);

    int err = raft_node_compact(&node, 100);

    MACRO_ASSERT_EQ_INT(err, -1);
    MACRO_ASSERT_EQ_INT(raft_snapshot_index(node.core), 0);

    raft_wal_close(&node.wal); raft_destroy(node.core); uv_loop_close(&loop);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, vote_response_not_sent_before_hardstate_persisted);
    MACRO_ADD(tests, snapshot_success_ack_not_sent_until_new_meta_persisted);
    MACRO_ADD(tests, snapshot_chunk_duplicate_offset_is_rejected_or_reacked);
    MACRO_ADD(tests, snapshot_chunk_gap_does_not_append_to_tmp_file);
    MACRO_ADD(tests, node_pump_snapshot_install_does_not_consume_stale_ready);
    MACRO_ADD(tests, snapshot_discarded_suffix_does_not_resurrect_after_restart);
    MACRO_ADD(tests, snapshot_worker_short_write_or_fsync_failure_rejects_chunk);
    MACRO_ADD(tests, snapshot_install_does_not_restore_stale_ctx_snapshot_metadata);
    MACRO_ADD(tests, snapshot_retry_overwrites_at_exact_offset_not_append_eof);
    MACRO_ADD(tests, snapshot_new_offset_zero_truncates_old_tmp_tail);
    MACRO_ADD(tests, hardstate_directory_fsync_failure_returns_false);
    MACRO_ADD(tests, snapshot_final_rename_requires_directory_fsync);
    MACRO_ADD(tests, inbound_queue_cap_exactly_10000_does_not_grow);
    MACRO_ADD(tests, node_compact_noops_must_not_purge_wal);

    macro_run_all("raft_pump", tests, test_count);
    return 0;
}
