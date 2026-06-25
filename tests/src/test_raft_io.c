// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define RAFT_TESTING 1
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include "a-raft-node/raft_io.h"
#include "a-raft-storage/raft_wal.h"
#include "the-macro-library/macro_test.h"

static void cleanup_wal_files(const char* base_path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base_path);
    system(cmd);
}

MACRO_TEST(io_save_and_boot) {
    const char* wal_path = "/tmp/raft_test_wal";
    cleanup_wal_files(wal_path);

    uint64_t peers[] = {1, 2, 3};
    bool learners[] = {false, false, false};
    raft_wal_t wal;

    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, wal_path, 16, 4), 0);

    raft_t* core = raft_io_boot(&wal, 1, peers, learners, 3, 0, 0, 0, 0, 0, 0);
    MACRO_ASSERT_TRUE(core != NULL);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(core, &hup);

    raft_msg_t pv_res = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(core, &pv_res);
    raft_advance_all_for_tests_only(core);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(core, &vote);

    raft_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"HELLO", .data_len = 5 };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_step_local(core, &prop);

    raft_ready_t ready = raft_get_ready(core);
    MACRO_ASSERT_TRUE(ready.num_entries_to_save > 0);

    raft_io_save(&wal, &ready);
    raft_advance_all_for_tests_only(core);

    uint64_t saved_term = raft_term(core);
    uint64_t saved_last_index = raft_last_index(core);
    uint64_t saved_commit = raft_commit_index(core);

    raft_destroy(core);
    raft_wal_close(&wal);

    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, wal_path, 16, 4), 0);

    raft_t* recovered_core = raft_io_boot(&wal, 1, peers, learners, 3, saved_term, 1, saved_commit, 0, 0, 0);

    MACRO_ASSERT_TRUE(recovered_core != NULL);
    MACRO_ASSERT_EQ_INT(raft_term(recovered_core), saved_term);
    MACRO_ASSERT_EQ_INT(raft_last_index(recovered_core), saved_last_index);

    ready = raft_get_ready(recovered_core);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 0);

    raft_destroy(recovered_core);
    raft_wal_close(&wal);
    cleanup_wal_files(wal_path);
}

MACRO_TEST(io_boot_with_purged_wal) {
    const char* wal_path = "/tmp/raft_test_wal_purged";
    cleanup_wal_files(wal_path);

    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, wal_path, 1, 2), 0);

    uint8_t dummy[1024] = {0};
    for (uint64_t i = 1; i <= 2000; i++) {
        raft_wal_append(&wal, 1, i, 0, 0, 0, dummy, 1024);
    }
    raft_wal_flush_batch(&wal);

    raft_wal_purge_head(&wal, 1000);
    raft_wal_close(&wal);

    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, wal_path, 1, 2), 0);

    uint64_t peers[] = {1, 2, 3};
    bool learners[] = {false, false, false};

    // FIX: A purged WAL mathematically requires a baseline snapshot to connect its sequence to!
    // Passing snapshot_index = 1000, snapshot_term = 1.
    raft_t* core = raft_io_boot(&wal, 1, peers, learners, 3, 1, 0, 1000, 1000, 1000, 1);
    MACRO_ASSERT_TRUE(core != NULL);

    MACRO_ASSERT_TRUE(raft_last_index(core) == 2000);

    raft_destroy(core);
    raft_wal_close(&wal);
    cleanup_wal_files(wal_path);
}

MACRO_TEST(raft_io_boot_uses_rebased_wal_offsets_after_purge) {
    const char* wal_path = "/tmp/raft_test_io_rebase";
    cleanup_wal_files(wal_path);

    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, wal_path, 1, 0), 0);

    // Expand the WAL significantly
    uint8_t dummy[1024] = {0};
    for (uint64_t i = 1; i <= 2000; i++) {
        raft_wal_append(&wal, 1, i, 0, 0, 0, dummy, 1024);
    }
    raft_wal_flush_batch(&wal);

    // Trigger sliding window base change
    raft_wal_purge_head(&wal, 1000);
    raft_wal_close(&wal);

    // Re-initialize to verify the new base is picked up by recovery
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, wal_path, 1, 0), 0);
    MACRO_ASSERT_TRUE(wal.offset_base_index > 1);

    uint64_t peers[] = {1, 2, 3};
    bool learners[] = {false, false, false};

    // The boot function MUST rely on first_index and successfully skip missing indices
    raft_t* core = raft_io_boot(&wal, 1, peers, learners, 3, 1, 0, 1000, 1000, 1000, 1);
    MACRO_ASSERT_TRUE(core != NULL);
    MACRO_ASSERT_EQ_INT(raft_last_index(core), 2000);

    raft_destroy(core);
    raft_wal_close(&wal);
    cleanup_wal_files(wal_path);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, io_save_and_boot);
    MACRO_ADD(tests, io_boot_with_purged_wal);
    MACRO_ADD(tests, raft_io_boot_uses_rebased_wal_offsets_after_purge);

    macro_run_all("raft_io_layer", tests, test_count);
    return 0;
}
