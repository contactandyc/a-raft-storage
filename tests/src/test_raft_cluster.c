// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>
#include "a-raft-node/raft_server.h"
#include "the-macro-library/macro_test.h"

static int passed = 0;
static int cluster_state = 0;
static uv_timer_t check_timer;
static raft_server_t servers[3];

static raft_node_t* nodes[3];

static void close_walk_cb(uv_handle_t* handle, void* arg) {
    (void)arg;
    if (!uv_is_closing(handle)) uv_close(handle, NULL);
}

static int dummy_apply_cb(void* ctx, const raft_entry_t* entry, uint64_t current_term) {
    (void)ctx;
    (void)entry;
    (void)current_term;
    return RAFT_APPLY_OK;
}

static void on_test_check(uv_timer_t* handle) {
    (void)handle;

    static int ticks = 0;
    ticks++;
    if (ticks > 150) {
        uv_stop(uv_default_loop());
        return;
    }

    uint64_t c1 = raft_commit_index(nodes[0]->core);
    uint64_t c2 = raft_commit_index(nodes[1]->core);
    uint64_t c3 = (cluster_state != 2) ? raft_commit_index(nodes[2]->core) : 0;

    raft_node_t* leader = NULL;
    for (int i = 0; i < 3; i++) {
        if (cluster_state != 2 && raft_state(nodes[i]->core) == RAFT_STATE_LEADER) {
            leader = nodes[i]; break;
        }
    }

    if (cluster_state == 0 && leader) {
        printf("\n[Stage 1] Leader elected! Proposing payload 1...\n");
        raft_node_propose(leader, (const uint8_t*)"PAYLOAD_1", 9, 1, 1, NULL);
        cluster_state = 1;
    }
    else if (cluster_state == 1) {
        if (c1 >= 2 && c2 >= 2 && c3 >= 2) {
            printf("[Stage 2] Payload 1 committed. Logically isolating Node 3...\n");
            servers[2].network_isolated = true;
            raft_node_propose(leader, (const uint8_t*)"PAYLOAD_2", 9, 1, 2, NULL);
            cluster_state = 2;
        }
    }
    else if (cluster_state == 2) {
        if (c1 >= 3 && c2 >= 3) {
            printf("[Stage 3] Payload 2 committed by survivors. Healing network for Node 3...\n");
            servers[2].network_isolated = false;
            cluster_state = 3;
        }
    }
    else if (cluster_state == 3) {
        if (c3 >= 3) {
            printf("[Stage 4] Node 3 reconnected to the mesh and synced Payload 2! SUCCESS.\n");
            passed = 1;
            uv_stop(uv_default_loop());
        }
    }
}

MACRO_TEST(cluster_tcp_crash_recovery_and_resync) {
    uv_loop_t* loop = uv_default_loop();

    system("rm -rf /tmp/raft_test_node*");
    system("mkdir -p /tmp/raft_test_node1 /tmp/raft_test_node2 /tmp/raft_test_node3");

    MACRO_ASSERT_EQ_INT(raft_server_init(&servers[0], loop, 1, 1, "/tmp/raft_test_node1"), 0);
    MACRO_ASSERT_EQ_INT(raft_server_init(&servers[1], loop, 2, 1, "/tmp/raft_test_node2"), 0);
    MACRO_ASSERT_EQ_INT(raft_server_init(&servers[2], loop, 3, 1, "/tmp/raft_test_node3"), 0);

    MACRO_ASSERT_EQ_INT(raft_server_listen(&servers[0], "127.0.0.1", 18081), 0);
    MACRO_ASSERT_EQ_INT(raft_server_listen(&servers[1], "127.0.0.1", 18082), 0);
    MACRO_ASSERT_EQ_INT(raft_server_listen(&servers[2], "127.0.0.1", 18083), 0);

    nodes[0] = calloc(1, sizeof(raft_node_t));
    nodes[1] = calloc(1, sizeof(raft_node_t));
    nodes[2] = calloc(1, sizeof(raft_node_t));

    uint64_t full_cluster[] = {1, 2, 3};
    raft_node_init(nodes[0], &servers[0], 0, full_cluster, 3, dummy_apply_cb, NULL, NULL, NULL, NULL, NULL);
    raft_node_init(nodes[1], &servers[1], 0, full_cluster, 3, dummy_apply_cb, NULL, NULL, NULL, NULL, NULL);
    raft_node_init(nodes[2], &servers[2], 0, full_cluster, 3, dummy_apply_cb, NULL, NULL, NULL, NULL, NULL);

    raft_server_connect(&servers[0], "127.0.0.1", 18082, 2);
    raft_server_connect(&servers[0], "127.0.0.1", 18083, 3);
    raft_server_connect(&servers[1], "127.0.0.1", 18083, 3);

    uv_timer_init(loop, &check_timer);
    uv_timer_start(&check_timer, on_test_check, 100, 100);

    uv_run(loop, UV_RUN_DEFAULT);

    uv_walk(loop, close_walk_cb, NULL);
    uv_run(loop, UV_RUN_DEFAULT);

    raft_wal_close(&nodes[0]->wal); raft_wal_close(&nodes[1]->wal); raft_wal_close(&nodes[2]->wal);
    raft_destroy(nodes[0]->core); raft_destroy(nodes[1]->core); raft_destroy(nodes[2]->core);

    free(nodes[0]); free(nodes[1]); free(nodes[2]);

    MACRO_ASSERT_TRUE(passed);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, cluster_tcp_crash_recovery_and_resync);
    macro_run_all("raft_cluster", tests, test_count);
    return 0;
}
