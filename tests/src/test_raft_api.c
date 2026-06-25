// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <uv.h>
#include "a-raft-node/raft_server.h"
#include "the-macro-library/macro_test.h"

// Helper to gracefully close all active libuv handles
static void close_walk_cb(uv_handle_t* handle, void* arg) {
    (void)arg;
    if (!uv_is_closing(handle)) {
        uv_close(handle, NULL);
    }
}

MACRO_TEST(raft_create_rejects_self_id_zero) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(0, peers, 2);
    MACRO_ASSERT_TRUE(r == NULL);
}

MACRO_TEST(raft_create_rejects_peer_id_zero) {
    uint64_t peers[] = {0, 3};
    raft_t* r = raft_create(1, peers, 2);
    MACRO_ASSERT_TRUE(r == NULL);
}

MACRO_TEST(raft_node_init_rejects_group_id_out_of_bounds) {
    uv_loop_t loop;
    uv_loop_init(&loop);

    raft_server_t srv;
    // Safely isolated test directory
    raft_server_init(&srv, &loop, 1, 10, "/tmp/raft_api_bounds");

    raft_node_t node;
    uint64_t peers[] = {1, 2}; // FIX: Include self in topology

    // Group ID 10 is out of bounds (max is 9)
    raft_node_init(&node, &srv, 10, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);
    MACRO_ASSERT_TRUE(node.core == NULL);

    free(srv.groups);
    uv_loop_close(&loop);
}

MACRO_TEST(node_propose_oversized_payload_returns_error_without_fatal) {
    uv_loop_t loop;
    uv_loop_init(&loop);

    // Completely isolate and wipe the test environment so the node boots fresh
    system("rm -rf /tmp/raft_api_oversize");
    system("mkdir -p /tmp/raft_api_oversize");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_api_oversize");

    raft_node_t node;
    uint64_t peers[] = {1, 2}; // FIX: Include self in topology
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    // Become Leader (Assuming fresh boot, term is 0)
    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(node.core, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(node.core, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(node.core, &vote);

    // Propose an 11MB payload (exceeds RAFT_MAX_FRAME_SIZE)
    uint32_t size = 11 * 1024 * 1024;
    uint8_t* huge = malloc(size);

    int err = raft_node_propose(&node, huge, size, 1, 1, NULL);

    MACRO_ASSERT_EQ_INT(err, RAFT_ERR_QUEUE_FULL);
    MACRO_ASSERT_FALSE(node.fatal_error);
    MACRO_ASSERT_FALSE(raft_has_fatal_error(node.core));

    free(huge);

    // Clean up
    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    free(srv.groups);

    // Drain and close internal node timers safely
    uv_walk(&loop, close_walk_cb, NULL);
    uv_run(&loop, UV_RUN_DEFAULT);
    uv_loop_close(&loop);
}

MACRO_TEST(raft_node_init_rejects_too_many_initial_peers) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_api_toomany");

    raft_node_t node;
    uint64_t massive_peers[100];
    for (int i=0; i<100; i++) massive_peers[i] = i+1;

    // RAFT_MAX_PEERS is 64. Passing 100 should fail safely.
    raft_node_init(&node, &srv, 0, massive_peers, 100, NULL, NULL, NULL, NULL, NULL, NULL);
    MACRO_ASSERT_TRUE(node.core == NULL);
    MACRO_ASSERT_TRUE(node.fatal_error == true);

    free(srv.groups);
    uv_loop_close(&loop);
}

MACRO_TEST(node_propose_single_allocation_no_leak_and_queue_failure_checked) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("rm -rf /tmp/raft_api_alloc; mkdir -p /tmp/raft_api_alloc");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_api_alloc");

    raft_node_t node;
    uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    // Bootstrap Leader
    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(node.core, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(node.core, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(node.core, &vote);

    // Force the inbound queue to reach max capacity intentionally
    node.inbound_queue_cap = 10001;
    node.inbound_queue_len = 10001;
    node.is_flushing = true; // Simulating a stalled I/O flush blocking new local events

    uint8_t payload[] = "VALID_DATA";
    int err = raft_node_propose(&node, payload, 10, 1, 1, NULL);

    // Assert that the proposal safely bails out with NOMEM when queue hits hard limit
    MACRO_ASSERT_EQ_INT(err, RAFT_ERR_NOMEM);
    MACRO_ASSERT_FALSE(node.fatal_error);

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    free(srv.groups);
    uv_loop_close(&loop);
}

MACRO_TEST(node_propose_null_payload_with_nonzero_len_returns_error) {
    raft_node_t node = {0};
    // Send a payload of length 10, but provide a NULL pointer
    int err = raft_node_propose(&node, NULL, 10, 1, 1, NULL);
    MACRO_ASSERT_EQ_INT(err, -1);
}

MACRO_TEST(read_index_queue_failure_returns_error) {
    raft_node_t node = {0};
    node.is_flushing = true;
    node.inbound_queue_cap = 10000;
    node.inbound_queue_len = 10000; // Queue is completely full

    int err = raft_node_read_index(&node, 1, NULL);
    MACRO_ASSERT_EQ_INT(err, -1); // Must fail cleanly
}

static bool contains(uint64_t* arr, size_t len, uint64_t target) {
    for (size_t i = 0; i < len; i++) {
        if (arr[i] == target) return true;
    }
    return false;
}

MACRO_TEST(node_init_remote_only_topology_is_rejected_or_converted) {
    uv_loop_t loop; uv_loop_init(&loop);
    system("chmod -R 777 /tmp/raft_test_api 2>/dev/null; rm -rf /tmp/raft_test_api; mkdir -p /tmp/raft_test_api");

    raft_server_t srv; raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_api");

    raft_node_t node;
    uint64_t remote_only_peers[] = {2, 3}; // Missing physical node '1'

    raft_node_init(&node, &srv, 0, remote_only_peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    MACRO_ASSERT_FALSE(node.fatal_error);

    uint64_t full_peers[3];
    bool learners[3];

    // Use the ext bounds to retrieve both full ID array and learner mappings
    size_t count = raft_peers_ext(node.core, full_peers, learners, 3);

    MACRO_ASSERT_EQ_INT(count, 3);
    MACRO_ASSERT_TRUE(contains(full_peers, count, 1)); // Engine automatically injected self
    MACRO_ASSERT_TRUE(contains(full_peers, count, 2));
    MACRO_ASSERT_TRUE(contains(full_peers, count, 3));

    raft_wal_close(&node.wal); raft_destroy(node.core); uv_loop_close(&loop);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, raft_create_rejects_self_id_zero);
    MACRO_ADD(tests, raft_create_rejects_peer_id_zero);
    MACRO_ADD(tests, raft_node_init_rejects_group_id_out_of_bounds);
    MACRO_ADD(tests, node_propose_oversized_payload_returns_error_without_fatal);
    MACRO_ADD(tests, raft_node_init_rejects_too_many_initial_peers);
    MACRO_ADD(tests, node_propose_single_allocation_no_leak_and_queue_failure_checked);
    MACRO_ADD(tests, node_propose_null_payload_with_nonzero_len_returns_error);
    MACRO_ADD(tests, read_index_queue_failure_returns_error);
    MACRO_ADD(tests, node_init_remote_only_topology_is_rejected_or_converted);

    macro_run_all("raft_api", tests, test_count);
    return 0;
}
