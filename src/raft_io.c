// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-raft-node/raft_io.h"
#include <stdio.h>
#include <stdlib.h>

raft_t* raft_io_boot(raft_wal_t* wal, uint64_t node_id,
                          uint64_t* loaded_peers, bool* loaded_learners, size_t num_peers,
                          uint64_t saved_term, uint64_t saved_vote, uint64_t saved_commit, uint64_t saved_applied,
                          uint64_t snapshot_index, uint64_t snapshot_term) {

    // PHASE 12: Strict Boot-Time Invariant Assertions
    if (saved_applied < snapshot_index || saved_commit < saved_applied) {
        fprintf(stderr, "[FATAL] Boot invariant failed: snapshot_index <= applied_index <= commit_index violated.\n");
        return NULL;
    }
    if (snapshot_index > 0 && snapshot_term == 0) {
        fprintf(stderr, "[FATAL] Boot invariant failed: valid snapshot lacks a corresponding snapshot_term.\n");
        return NULL;
    }
    if (wal->max_disk_index > 0 && saved_commit > wal->max_disk_index && saved_commit != snapshot_index) {
        fprintf(stderr, "[FATAL] Boot invariant failed: commit_index exceeds WAL bounds.\n");
        return NULL;
    }

    // FIX 1: Guard against math overflow before appending + 1
    if (snapshot_index == UINT64_MAX) return NULL;
    uint64_t start_idx = snapshot_index + 1;

    // FIX 1: Jump directly to the rebased offset, skipping all purged records in O(1) time
    uint64_t first_idx = raft_wal_first_index(wal);
    if (start_idx < first_idx) start_idx = first_idx;

    size_t total_entries = 0;
    raft_entry_t* entries = NULL;

    if (start_idx <= wal->max_disk_index && wal->max_disk_index > 0) {
        total_entries = wal->max_disk_index - start_idx + 1;
        entries = calloc(total_entries, sizeof(raft_entry_t));
        if (!entries) return NULL;

        for (uint64_t i = start_idx; i <= wal->max_disk_index; i++) {
            uint64_t term, cid, cseq; uint8_t type; uint8_t* payload = NULL; uint32_t len = 0;
            size_t arr_idx = i - start_idx;

            if (raft_wal_read_entry(wal, i, &term, &type, &cid, &cseq, &payload, &len)) {
                entries[arr_idx].term = term;
                entries[arr_idx].index = i;
                entries[arr_idx].type = (entry_type_t)type;
                entries[arr_idx].client_id = cid;
                entries[arr_idx].client_seq = cseq;
                entries[arr_idx].data = payload;
                entries[arr_idx].data_len = len;
            } else {
                for(size_t j = 0; j < arr_idx; j++) if (entries[j].data) free(entries[j].data);
                free(entries);
                return NULL;
            }
        }
    }

    raft_t* core = raft_restore(node_id, loaded_peers, loaded_learners, num_peers,
                                          saved_term, saved_vote, saved_commit, saved_applied,
                                          snapshot_index, snapshot_term, entries, total_entries);

    if (total_entries > 0 && entries) {
        for (size_t i = 0; i < total_entries; i++) if (entries[i].data) free(entries[i].data);
        free(entries);
    }
    return core;
}

bool raft_io_save(raft_wal_t* wal, raft_ready_t* ready) {
    if (ready->num_entries_to_save == 0) return true;
    uint64_t first_idx = ready->entries_to_save[0].index;
    if (first_idx <= wal->max_disk_index) raft_wal_truncate_tail(wal, first_idx);

    for (size_t i = 0; i < ready->num_entries_to_save; i++) {
        raft_entry_t* e = &ready->entries_to_save[i];
        if (raft_wal_append(wal, e->term, e->index, (uint8_t)e->type, e->client_id, e->client_seq, e->data, e->data_len) != 0) return false;
    }
    return raft_wal_flush_batch(wal) == 0;
}
