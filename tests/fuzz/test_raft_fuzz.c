// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include "a-raft-library/raft_core.h"

typedef struct {
    const uint8_t* data;
    size_t size;
    size_t cursor;
} fuzz_reader_t;

static uint64_t fuzz_read_u64(fuzz_reader_t* f) {
    if (f->cursor + 8 > f->size) return 0;
    uint64_t val;
    memcpy(&val, f->data + f->cursor, 8);
    f->cursor += 8;
    return val;
}

static uint8_t fuzz_read_u8(fuzz_reader_t* f) {
    if (f->cursor + 1 > f->size) return 0;
    return f->data[f->cursor++];
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size < 64) return 0; // Need enough bytes to generate disk + network data

    fuzz_reader_t f = { .data = data, .size = size, .cursor = 0 };

    raft_core_t* r = NULL;
    uint64_t peers[] = {2, 3};

    // COIN FLIP: How do we boot the node?
    uint8_t boot_mode = fuzz_read_u8(&f) % 2;

    if (boot_mode == 0) {
        // Clean Boot
        r = raft_core_create(1, peers, 2);
    } else {
        // Disk Restore Boot
        uint64_t disk_term = fuzz_read_u64(&f);
        uint64_t disk_vote = fuzz_read_u64(&f);
        uint64_t disk_commit = fuzz_read_u64(&f); // FIXED: Generate fuzzed commit index
        size_t num_disk_entries = (fuzz_read_u8(&f) % 5) + 1; // 1 to 5 entries

        raft_entry_t* disk_entries = calloc(num_disk_entries, sizeof(raft_entry_t));

        for (size_t i = 0; i < num_disk_entries; i++) {
            disk_entries[i].term = fuzz_read_u64(&f);
            disk_entries[i].index = fuzz_read_u64(&f);
            disk_entries[i].type = fuzz_read_u8(&f) % 3;

            size_t payload_len = fuzz_read_u8(&f) % 8;
            disk_entries[i].data_len = payload_len;
            if (payload_len > 0 && f.cursor + payload_len <= f.size) {
                disk_entries[i].data = malloc(payload_len);
                memcpy(disk_entries[i].data, f.data + f.cursor, payload_len);
                f.cursor += payload_len;
            }
        }

        // FIXED: Call the updated signature with disk_commit
        r = raft_core_restore(1, peers, 2, disk_term, disk_vote, disk_commit, disk_entries, num_disk_entries);

        // Cleanup the temporary disk structures whether restore succeeded or failed
        for (size_t i = 0; i < num_disk_entries; i++) {
            if (disk_entries[i].data) free(disk_entries[i].data);
        }
        free(disk_entries);
    }

    // If restore failed (because the fuzzer generated mathematically invalid logs), we just exit safely.
    if (!r) return 0;

    // --- PHASE 2: FUZZ THE NETWORK ---
    raft_msg_t msg = {0};
    msg.type = fuzz_read_u8(&f) % 7;
    msg.to = fuzz_read_u8(&f) % 4;
    msg.from = fuzz_read_u8(&f) % 4;
    msg.reject = fuzz_read_u8(&f) % 2;

    msg.term = fuzz_read_u64(&f);
    msg.log_term = fuzz_read_u64(&f);
    msg.index = fuzz_read_u64(&f);
    msg.commit = fuzz_read_u64(&f);

    if (msg.type == MSG_APPEND_ENTRIES || msg.type == MSG_PROPOSE) {
        msg.num_entries = fuzz_read_u8(&f) % 4;
        if (msg.num_entries > 0) {
            msg.entries = calloc(msg.num_entries, sizeof(raft_entry_t));
            for (size_t i = 0; i < msg.num_entries; i++) {
                msg.entries[i].term = fuzz_read_u64(&f);
                msg.entries[i].type = fuzz_read_u8(&f) % 3;

                size_t remaining = f.size - f.cursor;
                size_t chunk = msg.num_entries - i > 0 ? remaining / (msg.num_entries - i) : 0;

                msg.entries[i].data_len = chunk;
                if (chunk > 0) {
                    msg.entries[i].data = malloc(chunk);
                    memcpy(msg.entries[i].data, f.data + f.cursor, chunk);
                    f.cursor += chunk;
                }
            }
        }
    }

    raft_core_step(r, &msg);
    raft_core_advance_all(r);

    if (msg.entries) {
        for (size_t i = 0; i < msg.num_entries; i++) {
            if (msg.entries[i].data) free(msg.entries[i].data);
        }
        free(msg.entries);
    }

    raft_core_destroy(r);
    return 0;
}
