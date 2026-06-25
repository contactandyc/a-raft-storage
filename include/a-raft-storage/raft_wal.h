// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef RAFT_WAL_H
#define RAFT_WAL_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#define RAFT_WAL_MAGIC 0x52414654 // "RAFT"
#define RAFT_WAL_SEG_HEADER_SIZE 20

// Expanded Frame: 4(crc) + 4(len) + 8(term) + 8(index) + 1(type) + 8(cid) + 8(cseq) = 41 bytes
#define RAFT_WAL_FRAME_HEADER_SIZE 41

typedef struct {
    uint64_t seg_id;
    uint32_t offset;
} raft_wal_loc_t;

typedef struct {
    char base_dir[512];
    uint64_t segment_size_bytes;

    uint64_t current_seg_id;
    uint64_t oldest_seg_id;
    char** standby_paths;
    uint32_t standby_count;
    uint32_t max_standby;

    int active_fd;
    uint64_t file_offset;
    uint64_t max_disk_index;

    int read_fd;
    uint64_t read_seg_id;

    raft_wal_loc_t* offsets;
    uint64_t offsets_cap;
    uint64_t offset_base_index; // The absolute raft index that maps to offsets[0]

    uint8_t* batch_buf;
    uint32_t batch_len;
    uint32_t batch_cap;

} raft_wal_t;

// O(1) Rebased Origin Fetcher
uint64_t raft_wal_first_index(raft_wal_t* wal);

int  raft_wal_init(raft_wal_t* wal, const char* dir, uint64_t segment_size_mb, uint32_t max_standby);

// Deduplication is durably stored for exactly-once recovery
int  raft_wal_append(raft_wal_t* wal, uint64_t term, uint64_t index, uint8_t type, uint64_t client_id, uint64_t client_seq, const uint8_t* payload, uint32_t len);

int  raft_wal_flush_batch(raft_wal_t* wal);

// Extractor handles reading sequence headers for historical resync
int  raft_wal_read_entry(raft_wal_t* wal, uint64_t target_index, uint64_t* out_term, uint8_t* out_type, uint64_t* out_cid, uint64_t* out_cseq, uint8_t** out_payload, uint32_t* out_len);

int  raft_wal_truncate_tail(raft_wal_t* wal, uint64_t truncate_from_index);
int  raft_wal_purge_head(raft_wal_t* wal, uint64_t safe_checkpoint_index);
void raft_wal_close(raft_wal_t* wal);

// ============================================================================
// ENTERPRISE STORAGE FEATURES (Phase 3)
// ============================================================================

// Scans the active and sealed segments to detect bit-rot. Returns 0 if healthy, or the index of the first corrupted frame.
uint64_t raft_wal_verify_log_integrity(raft_wal_t* wal);

// Generates a `.meta` file containing a CRC32 checksum of the `.dat` snapshot file
int raft_wal_create_snapshot_manifest(const char* base_dir, uint64_t group_id, uint64_t snap_idx, uint64_t snap_term);

// Verifies the `.dat` file matches the checksum in the `.meta` file before allowing it to load
int raft_wal_verify_snapshot_manifest(const char* base_dir, uint64_t group_id, uint64_t* out_idx, uint64_t* out_term);

#endif // RAFT_WAL_H
