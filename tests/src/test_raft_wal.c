// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "a-raft-library/raft_wal.h"
#include "the-macro-library/macro_test.h"

#define TEST_WAL_DIR "wal_test_data"

static void clear_test_dir() {
    system("rm -rf " TEST_WAL_DIR);
}

MACRO_TEST(wal_init_sets_fds_to_minus_one_on_failure) {
    raft_wal_t wal;
    clear_test_dir();
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 0, 0), -1);
    MACRO_ASSERT_EQ_INT(wal.active_fd, -1);
    MACRO_ASSERT_EQ_INT(wal.read_fd, -1);
}

MACRO_TEST(wal_max_standby_zero_is_valid) {
    raft_wal_t wal;
    clear_test_dir();
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 1, 0), 0);
    raft_wal_close(&wal);
}

MACRO_TEST(wal_rejects_old_magic_or_old_endian_format) {
    clear_test_dir();
    mkdir(TEST_WAL_DIR, 0755);

    int fd = open(TEST_WAL_DIR "/0000000001.wal", O_RDWR | O_CREAT, 0644);
    uint8_t bad_hdr[20] = {0};
    pwrite(fd, bad_hdr, 20, 0);
    close(fd);

    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 1, 0), -1);
}

MACRO_TEST(wal_append_accepts_index_1_on_fresh_wal) {
    raft_wal_t wal;
    clear_test_dir();
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 1, 0), 0);
    MACRO_ASSERT_EQ_INT(raft_wal_append(&wal, 1, 1, 0, 0, 0, (uint8_t*)"A", 1), 0);
    raft_wal_close(&wal);
}

MACRO_TEST(wal_append_rejects_index_2_on_fresh_empty_wal) {
    raft_wal_t wal;
    clear_test_dir();
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 1, 0), 0);
    MACRO_ASSERT_EQ_INT(raft_wal_append(&wal, 1, 2, 0, 0, 0, (uint8_t*)"A", 1), -1);
    raft_wal_close(&wal);
}

MACRO_TEST(wal_append_rejects_noncontiguous_index) {
    clear_test_dir();
    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 1, 0), 0);
    MACRO_ASSERT_EQ_INT(raft_wal_append(&wal, 1, 1, 0, 0, 0, (uint8_t*)"A", 1), 0);
    MACRO_ASSERT_EQ_INT(raft_wal_append(&wal, 1, 10, 0, 0, 0, (uint8_t*)"B", 1), -1);
    raft_wal_close(&wal);
}

MACRO_TEST(wal_rejects_frame_len_larger_than_segment_remaining) {
    clear_test_dir();
    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 1, 0), 0);

    raft_wal_append(&wal, 1, 1, 0, 0, 0, (uint8_t*)"X", 1);
    raft_wal_flush_batch(&wal);

    uint8_t bad_len[4] = {0x00, 0x50, 0x00, 0x00};
    pwrite(wal.active_fd, bad_len, 4, 20 + 4);
    raft_wal_close(&wal);

    raft_wal_t wal2;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal2, TEST_WAL_DIR, 1, 0), 0);
    MACRO_ASSERT_EQ_INT(wal2.max_disk_index, 0);
    raft_wal_close(&wal2);
}

MACRO_TEST(wal_detects_crc_corruption_in_old_sealed_segment_as_fatal) {
    clear_test_dir();
    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 1, 0), 0);

    uint8_t* big = calloc(1, 400000);
    raft_wal_append(&wal, 1, 1, 0, 0, 0, big, 400000);
    raft_wal_append(&wal, 1, 2, 0, 0, 0, big, 400000);

    // FIX: Add a third 400KB append to exceed the 1MB segment limit!
    // This forces the WAL to rotate, permanently sealing Segment 1.
    raft_wal_append(&wal, 1, 3, 0, 0, 0, big, 400000);

    raft_wal_flush_batch(&wal);
    raft_wal_close(&wal);
    free(big);

    int fd = open(TEST_WAL_DIR "/0000000001.wal", O_RDWR);
    uint8_t garbage = 0x99;
    pwrite(fd, &garbage, 1, 50); // Corrupt historical Segment 1
    close(fd);

    raft_wal_t wal2;
    // Because Segment 1 is no longer the tail, the engine will strictly abort!
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal2, TEST_WAL_DIR, 1, 0), -1);
}

MACRO_TEST(wal_reused_standby_file_does_not_resurrect_old_frames) {
    clear_test_dir();
    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 1, 1), 0);

    uint8_t* big = calloc(1, 400000);
    raft_wal_append(&wal, 1, 1, 0, 0, 0, big, 400000);
    raft_wal_append(&wal, 1, 2, 0, 0, 0, big, 400000);
    raft_wal_append(&wal, 1, 3, 0, 0, 0, big, 400000); // Forces segment rotation
    raft_wal_flush_batch(&wal);

    raft_wal_truncate_tail(&wal, 2);
    free(big);

    uint8_t* big2 = calloc(1, 400000);
    raft_wal_append(&wal, 1, 2, 0, 0, 0, big2, 400000);
    raft_wal_append(&wal, 1, 3, 0, 0, 0, big2, 400000);
    raft_wal_flush_batch(&wal);

    raft_wal_close(&wal);
    free(big2);

    raft_wal_t wal2;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal2, TEST_WAL_DIR, 1, 1), 0);
    MACRO_ASSERT_EQ_INT(wal2.max_disk_index, 3);
    raft_wal_close(&wal2);
}

MACRO_TEST(wal_append_rejects_null_payload_with_nonzero_len) {
    clear_test_dir();
    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 1, 0), 0);
    MACRO_ASSERT_EQ_INT(raft_wal_append(&wal, 1, 1, 0, 0, 0, NULL, 100), -1);
    raft_wal_close(&wal);
}

MACRO_TEST(wal_append_rejects_payload_over_wal_max_payload_size) {
    clear_test_dir();
    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 20, 0), 0);
    uint8_t dummy = 0;
    MACRO_ASSERT_EQ_INT(raft_wal_append(&wal, 1, 1, 0, 0, 0, &dummy, 15 * 1024 * 1024), -1);
    raft_wal_close(&wal);
}

MACRO_TEST(wal_truncate_tail_missing_offset_rejected) {
    clear_test_dir();
    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 1, 0), 0);

    raft_wal_append(&wal, 1, 1, 0, 0, 0, (uint8_t*)"A", 1);
    raft_wal_flush_batch(&wal);

    MACRO_ASSERT_EQ_INT(raft_wal_truncate_tail(&wal, 0), -1);
    MACRO_ASSERT_EQ_INT(raft_wal_truncate_tail(&wal, 999), -1);

    wal.max_disk_index = 5;
    MACRO_ASSERT_EQ_INT(raft_wal_truncate_tail(&wal, 2), -1);

    raft_wal_close(&wal);
}

MACRO_TEST(wal_truncate_tail_clears_offset_and_file_offset) {
    clear_test_dir();
    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 1, 0), 0);

    raft_wal_append(&wal, 1, 1, 0, 0, 0, (uint8_t*)"A", 1);
    raft_wal_append(&wal, 1, 2, 0, 0, 0, (uint8_t*)"B", 1);
    raft_wal_append(&wal, 1, 3, 0, 0, 0, (uint8_t*)"C", 1);
    raft_wal_flush_batch(&wal);

    MACRO_ASSERT_TRUE(wal.offsets[3 - wal.offset_base_index].seg_id != 0);

    MACRO_ASSERT_EQ_INT(raft_wal_truncate_tail(&wal, 2), 0);
    MACRO_ASSERT_EQ_INT(wal.max_disk_index, 1);

    MACRO_ASSERT_EQ_INT(wal.offsets[2 - wal.offset_base_index].seg_id, 0);
    MACRO_ASSERT_EQ_INT(wal.offsets[3 - wal.offset_base_index].seg_id, 0);

    raft_wal_close(&wal);
}

MACRO_TEST(wal_read_entry_rejects_bad_output_pointers) {
    clear_test_dir();
    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 1, 0), 0);

    raft_wal_append(&wal, 1, 1, 0, 0, 0, (uint8_t*)"A", 1);
    raft_wal_flush_batch(&wal);

    uint64_t t = 0, c = 0, s = 0; uint8_t ty = 0; uint32_t l = 0; uint8_t* p = NULL;
    (void)t; (void)c; (void)s; (void)ty; (void)l; (void)p;

    MACRO_ASSERT_EQ_INT(raft_wal_read_entry(&wal, 1, NULL, &ty, &c, &s, &p, &l), 0);

    raft_wal_close(&wal);
}

MACRO_TEST(wal_read_entry_rejects_offset_beyond_segment_size) {
    clear_test_dir();
    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 1, 0), 0);

    raft_wal_append(&wal, 1, 1, 0, 0, 0, (uint8_t*)"A", 1);
    raft_wal_flush_batch(&wal);

    wal.offsets[1 - wal.offset_base_index].offset = 2 * 1024 * 1024;

    uint64_t t = 0, c = 0, s = 0; uint8_t ty = 0; uint32_t l = 0; uint8_t* p = NULL;
    (void)t; (void)c; (void)s; (void)ty; (void)l; (void)p;

    MACRO_ASSERT_EQ_INT(raft_wal_read_entry(&wal, 1, &t, &ty, &c, &s, &p, &l), 0);

    raft_wal_close(&wal);
}

MACRO_TEST(wal_truncate_tail_clears_read_cache_for_removed_segment) {
    clear_test_dir();
    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 1, 0), 0);

    uint8_t* big = calloc(1, 400000);
    raft_wal_append(&wal, 1, 1, 0, 0, 0, big, 400000);
    raft_wal_append(&wal, 1, 2, 0, 0, 0, big, 400000);
    raft_wal_flush_batch(&wal);
    free(big);

    uint64_t out_term, out_cid, out_cseq; uint8_t out_type; uint32_t out_len;
    uint8_t* out_payload = NULL;
    raft_wal_read_entry(&wal, 1, &out_term, &out_type, &out_cid, &out_cseq, &out_payload, &out_len);
    if (out_payload) free(out_payload);

    MACRO_ASSERT_TRUE(wal.read_fd >= 0);
    raft_wal_truncate_tail(&wal, 1);
    MACRO_ASSERT_EQ_INT(wal.read_fd, -1);

    raft_wal_close(&wal);
}

MACRO_TEST(wal_rotation_crash_before_header_does_not_brick_recovery) {
    clear_test_dir();
    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 1, 0), 0);
    raft_wal_append(&wal, 1, 1, 0, 0, 0, (uint8_t*)"A", 1);
    raft_wal_flush_batch(&wal);
    raft_wal_close(&wal);

    // Simulate crash mid-rotation using temp files
    int tmp_fd = open(TEST_WAL_DIR "/rotate_0000000002.tmp", O_RDWR | O_CREAT, 0644);
    pwrite(tmp_fd, "GARBAGE", 7, 0);
    close(tmp_fd);

    raft_wal_t wal2;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal2, TEST_WAL_DIR, 1, 0), 0);
    MACRO_ASSERT_EQ_INT(wal2.max_disk_index, 1);
    raft_wal_close(&wal2);
}

MACRO_TEST(wal_close_is_idempotent) {
    clear_test_dir();
    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 1, 0), 0);
    raft_wal_close(&wal);
    raft_wal_close(&wal);
}

MACRO_TEST(wal_rejects_segment_size_over_uint32_offset_limit) {
    clear_test_dir();
    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, TEST_WAL_DIR, 5000, 0), -1);
}

MACRO_TEST(raft_wal_offset_map_shrinks_after_purge) {
    clear_test_dir();
    raft_wal_t wal;
    raft_wal_init(&wal, TEST_WAL_DIR, 1, 0);

    // FIX: Use a 500-byte payload so 5,000 entries = ~2.5MB.
    // This forces the WAL to rotate through 3 physical segment files.
    uint8_t dummy[500] = {0};
    for (int i=1; i<=5000; i++) {
        raft_wal_append(&wal, 1, i, 0, 0, 0, dummy, 500);
    }
    raft_wal_flush_batch(&wal);

    uint64_t cap_before = wal.offsets_cap;

    // Purge up to index 4900. This safely deletes Segments 1 and 2,
    // advancing the base index and dropping the array capacity.
    raft_wal_purge_head(&wal, 4900);

    MACRO_ASSERT_TRUE(wal.offsets_cap < cap_before);
    raft_wal_close(&wal);
}

MACRO_TEST(raft_wal_read_purged_entry_returns_not_found) {
    clear_test_dir();
    raft_wal_t wal;
    raft_wal_init(&wal, TEST_WAL_DIR, 1, 0);

    uint8_t* big = calloc(1, 400000);
    raft_wal_append(&wal, 1, 1, 0, 0, 0, big, 400000);
    raft_wal_append(&wal, 1, 2, 0, 0, 0, big, 400000);
    raft_wal_append(&wal, 1, 3, 0, 0, 0, big, 400000);
    raft_wal_flush_batch(&wal);
    free(big);

    raft_wal_purge_head(&wal, 2);

    uint64_t t, c, s; uint8_t ty; uint32_t l; uint8_t* p = NULL;
    MACRO_ASSERT_EQ_INT(raft_wal_read_entry(&wal, 1, &t, &ty, &c, &s, &p, &l), 0);
    MACRO_ASSERT_EQ_INT(raft_wal_read_entry(&wal, 3, &t, &ty, &c, &s, &p, &l), 1);
    if(p) free(p);

    raft_wal_close(&wal);
}

MACRO_TEST(raft_wal_truncate_tail_one_past_end_is_noop) {
    clear_test_dir();
    raft_wal_t wal;
    raft_wal_init(&wal, TEST_WAL_DIR, 1, 0);

    raft_wal_append(&wal, 1, 1, 0, 0, 0, (uint8_t*)"X", 1);
    raft_wal_flush_batch(&wal);

    int err = raft_wal_truncate_tail(&wal, 2);
    MACRO_ASSERT_EQ_INT(err, 0);

    raft_wal_close(&wal);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, wal_init_sets_fds_to_minus_one_on_failure);
    MACRO_ADD(tests, wal_max_standby_zero_is_valid);
    MACRO_ADD(tests, wal_rejects_old_magic_or_old_endian_format);
    MACRO_ADD(tests, wal_append_accepts_index_1_on_fresh_wal);
    MACRO_ADD(tests, wal_append_rejects_index_2_on_fresh_empty_wal);
    MACRO_ADD(tests, wal_append_rejects_noncontiguous_index);
    MACRO_ADD(tests, wal_rejects_frame_len_larger_than_segment_remaining);
    MACRO_ADD(tests, wal_detects_crc_corruption_in_old_sealed_segment_as_fatal);
    MACRO_ADD(tests, wal_reused_standby_file_does_not_resurrect_old_frames);
    MACRO_ADD(tests, wal_append_rejects_null_payload_with_nonzero_len);
    MACRO_ADD(tests, wal_append_rejects_payload_over_wal_max_payload_size);
    MACRO_ADD(tests, wal_truncate_tail_missing_offset_rejected);
    MACRO_ADD(tests, wal_truncate_tail_clears_offset_and_file_offset);
    MACRO_ADD(tests, wal_read_entry_rejects_bad_output_pointers);
    MACRO_ADD(tests, wal_read_entry_rejects_offset_beyond_segment_size);
    MACRO_ADD(tests, wal_truncate_tail_clears_read_cache_for_removed_segment);
    MACRO_ADD(tests, wal_rotation_crash_before_header_does_not_brick_recovery);
    MACRO_ADD(tests, wal_close_is_idempotent);
    MACRO_ADD(tests, wal_rejects_segment_size_over_uint32_offset_limit);
    MACRO_ADD(tests, raft_wal_offset_base_advances_after_purge);
    MACRO_ADD(tests, raft_wal_offset_map_shrinks_after_purge);
    MACRO_ADD(tests, raft_wal_read_purged_entry_returns_not_found);
    MACRO_ADD(tests, raft_wal_truncate_tail_one_past_end_is_noop);

    macro_run_all("raft_wal", tests, test_count);
    return 0;
}
