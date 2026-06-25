// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef RAFT_IO_H
#define RAFT_IO_H

#include "a-raft-core/raft.h"
#include "a-raft-storage/raft_wal.h"

// PHASE 7: Ingest completely verified snap and membership states
raft_t* raft_io_boot(raft_wal_t* wal, uint64_t node_id,
                          uint64_t* loaded_peers, bool* loaded_learners, size_t num_peers,
                          uint64_t saved_term, uint64_t saved_vote, uint64_t saved_commit, uint64_t saved_applied,
                          uint64_t snapshot_index, uint64_t snapshot_term);

bool raft_io_save(raft_wal_t* wal, raft_ready_t* ready);

#endif // RAFT_IO_H
