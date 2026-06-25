// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define _GNU_SOURCE
#include "a-raft-storage/raft_wal.h" // STRICT ISOLATION INCLUDE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <time.h>
#include <errno.h>
#include <stdarg.h>

#define WAL_V2_MAGIC 0x57414C32
#define WAL_V2_SEG_HEADER_SIZE 20
#define WAL_V2_FRAME_HEADER_SIZE 41
#define WAL_V2_MAX_PAYLOAD_SIZE (8 * 1024 * 1024)

// -----------------------------------------------------------------------------
// PORTABLE ENDIAN I/O HELPERS
// -----------------------------------------------------------------------------

static void pack_u32(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)(v);
}

static void pack_u64(uint8_t* p, uint64_t v) {
    p[0] = (uint8_t)(v >> 56); p[1] = (uint8_t)(v >> 48);
    p[2] = (uint8_t)(v >> 40); p[3] = (uint8_t)(v >> 32);
    p[4] = (uint8_t)(v >> 24); p[5] = (uint8_t)(v >> 16);
    p[6] = (uint8_t)(v >> 8);  p[7] = (uint8_t)(v);
}

static uint32_t unpack_u32(const uint8_t* p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}

static uint64_t unpack_u64(const uint8_t* p) {
    return ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) | ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
           ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) | ((uint64_t)p[6] << 8)  | (uint64_t)p[7];
}

// -----------------------------------------------------------------------------
// INTERNAL UTILITIES
// -----------------------------------------------------------------------------

static char* strdupf(const char* format, ...) {
    va_list args;

    va_start(args, format);
    int len = vsnprintf(NULL, 0, format, args);
    va_end(args);

    if (len < 0) return NULL;

    char* str = malloc((size_t)len + 1);
    if (!str) return NULL;

    va_start(args, format);
    vsnprintf(str, (size_t)len + 1, format, args);
    va_end(args);

    return str;
}

static uint32_t crc32_update(uint32_t crc, const void *buf, size_t size) {
    const uint8_t *p = buf;
    while (size--) {
        crc ^= *p++;
        for (int i = 0; i < 8; i++) crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
    }
    return crc;
}

static ssize_t safe_pread(int fd, void *buf, size_t count, off_t offset) {
    size_t total = 0;
    while (total < count) {
        ssize_t res = pread(fd, (uint8_t*)buf + total, count - total, offset + total);
        if (res < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (res == 0) break;
        total += res;
    }
    return total;
}

static ssize_t safe_pwrite(int fd, const void *buf, size_t count, off_t offset) {
    size_t total = 0;
    while (total < count) {
        ssize_t res = pwrite(fd, (const uint8_t*)buf + total, count - total, offset + total);
        if (res < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (res == 0) break;
        total += res;
    }
    return total;
}

static bool sync_file(int fd) {
#ifdef __APPLE__
    return fcntl(fd, F_FULLFSYNC, 0) == 0;
#else
    return fdatasync(fd) == 0;
#endif
}

static bool sync_dir(const char* dir_path) {
    int dir_fd = open(dir_path, O_RDONLY);
    if (dir_fd < 0) return false;
    bool ok = sync_file(dir_fd);
    close(dir_fd);
    return ok;
}

static int preallocate_file(int fd, uint64_t size) {
#ifdef __APPLE__
    fstore_t store = {F_ALLOCATECONTIG, F_PEOFPOSMODE, 0, size, 0};
    fcntl(fd, F_PREALLOCATE, &store);
    return ftruncate(fd, size);
#else
    return posix_fallocate(fd, 0, size);
#endif
}

static void get_segment_path(raft_wal_t* wal, uint64_t seg_id, char* out_path) {
    snprintf(out_path, 1024, "%s/%010llu.wal", wal->base_dir, (unsigned long long)seg_id);
}

static bool read_segment_header(int fd, uint64_t expected_seg_id, uint64_t* out_start_index) {
    uint8_t hdr[WAL_V2_SEG_HEADER_SIZE];

    if (safe_pread(fd, hdr, sizeof(hdr), 0) != sizeof(hdr)) return false;

    uint32_t magic = unpack_u32(hdr);
    uint64_t seg_id = unpack_u64(hdr + 4);
    uint64_t start_index = unpack_u64(hdr + 12);

    if (magic != WAL_V2_MAGIC) return false;
    if (expected_seg_id != 0 && seg_id != expected_seg_id) return false;
    if (start_index == 0) return false;

    if (out_start_index) *out_start_index = start_index;
    return true;
}

// -----------------------------------------------------------------------------
// SLIDING WINDOW OFFSETS (MEMORY SAFETY)
// -----------------------------------------------------------------------------

uint64_t raft_wal_first_index(raft_wal_t* wal) {
    return wal ? wal->offset_base_index : 0;
}

static bool wal_offset_slot(raft_wal_t* wal, uint64_t index, uint64_t* out_slot) {
    if (!wal || index == 0) return false;
    if (wal->offset_base_index == 0) return false;
    if (index < wal->offset_base_index) return false;

    uint64_t rel = index - wal->offset_base_index;
    if (rel >= wal->offsets_cap) return false;

    *out_slot = rel;
    return true;
}

static bool ensure_offset_capacity(raft_wal_t* wal, uint64_t index) {
    if (index == 0) return false;

    if (wal->offset_base_index == 0) {
        wal->offset_base_index = index;
    }

    if (index < wal->offset_base_index) {
        return false;
    }

    uint64_t rel_idx = index - wal->offset_base_index;
    if (rel_idx >= wal->offsets_cap) {
        uint64_t new_cap = wal->offsets_cap == 0 ? 1024 : wal->offsets_cap * 2;
        while (rel_idx >= new_cap) {
            if (new_cap > UINT64_MAX / 2) return false;
            new_cap *= 2;
        }

        raft_wal_loc_t* new_offsets = calloc(new_cap, sizeof(raft_wal_loc_t));
        if (!new_offsets) return false;

        if (wal->offsets) {
            memcpy(new_offsets, wal->offsets, wal->offsets_cap * sizeof(raft_wal_loc_t));
            free(wal->offsets);
        }
        wal->offsets = new_offsets;
        wal->offsets_cap = new_cap;
    }
    return true;
}

// -----------------------------------------------------------------------------
// LIFECYCLE & RECOVERY
// -----------------------------------------------------------------------------

int raft_wal_init(raft_wal_t* wal, const char* dir, uint64_t segment_size_mb, uint32_t max_standby) {
    if (!wal) return -1;

    memset(wal, 0, sizeof(raft_wal_t));
    wal->active_fd = -1;
    wal->read_fd = -1;

    if (!dir || segment_size_mb == 0) return -1;

    uint64_t seg_bytes = segment_size_mb * 1024 * 1024;
    if (seg_bytes > UINT32_MAX || seg_bytes < WAL_V2_SEG_HEADER_SIZE + WAL_V2_FRAME_HEADER_SIZE) {
        return -1;
    }

    strncpy(wal->base_dir, dir, sizeof(wal->base_dir) - 1);
    wal->segment_size_bytes = seg_bytes;
    wal->max_standby = max_standby;

    wal->standby_paths = max_standby > 0 ? malloc(sizeof(char*) * max_standby) : NULL;
    if (max_standby > 0 && !wal->standby_paths) goto init_fail;

    wal->batch_cap = 65536;
    wal->batch_buf = malloc(wal->batch_cap);
    if (!wal->batch_buf) goto init_fail;

    if (mkdir(dir, 0755) != 0 && errno != EEXIST) goto init_fail;
    if (!sync_dir(dir)) goto init_fail;

    uint64_t min_seg = UINT64_MAX, max_seg = 0;
    DIR *dp = opendir(dir);
    if (!dp) goto init_fail;

    struct dirent *ep;
    while ((ep = readdir(dp))) {
        if (strncmp(ep->d_name, "standby_", 8) == 0) {
            if (wal->standby_count < wal->max_standby) {
                char* sp = strdupf("%s/%s", dir, ep->d_name);
                if (sp) wal->standby_paths[wal->standby_count++] = sp;
            } else {
                char excess[1024]; snprintf(excess, 1024, "%s/%s", dir, ep->d_name);
                unlink(excess);
            }
        } else if (strstr(ep->d_name, ".wal")) {
            uint64_t seg_id;
            if (sscanf(ep->d_name, "%llu.wal", (unsigned long long*)&seg_id) == 1) {
                if (seg_id < min_seg) min_seg = seg_id;
                if (seg_id > max_seg) max_seg = seg_id;
            }
        }
    }
    closedir(dp);

    if (max_seg == 0) {
        wal->oldest_seg_id = 1;
        wal->current_seg_id = 1;
        wal->file_offset = WAL_V2_SEG_HEADER_SIZE;
        wal->max_disk_index = 0;

        wal->offset_base_index = 1;

        char tmp_path[1024]; snprintf(tmp_path, 1024, "%s/init.tmp", wal->base_dir);
        char final_path[1024]; get_segment_path(wal, 1, final_path);

        int fd = open(tmp_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) goto init_fail;

        if (preallocate_file(fd, wal->segment_size_bytes) != 0) { close(fd); goto init_fail; }

        uint8_t hdr[WAL_V2_SEG_HEADER_SIZE];
        pack_u32(hdr, WAL_V2_MAGIC);
        pack_u64(hdr + 4, wal->current_seg_id);
        pack_u64(hdr + 12, 1);

        if (safe_pwrite(fd, hdr, WAL_V2_SEG_HEADER_SIZE, 0) != WAL_V2_SEG_HEADER_SIZE) { close(fd); goto init_fail; }
        if (!sync_file(fd)) { close(fd); goto init_fail; }

        if (rename(tmp_path, final_path) != 0) { close(fd); goto init_fail; }
        if (!sync_dir(wal->base_dir)) { close(fd); goto init_fail; }

        wal->active_fd = fd;
        return 0;
    }

    wal->oldest_seg_id = min_seg;
    wal->current_seg_id = max_seg;
    uint64_t expected_index = 0;
    bool index_established = false;

    for (uint64_t seg = min_seg; seg <= max_seg; seg++) {
        char path[1024]; get_segment_path(wal, seg, path);
        int fd = open(path, O_RDWR);
        if (fd < 0) goto init_fail;

        uint64_t seg_start_index;
        if (!read_segment_header(fd, seg, &seg_start_index)) {
            close(fd); goto init_fail;
        }

        if (!index_established) {
            expected_index = seg_start_index;
            index_established = true;
            wal->max_disk_index = expected_index - 1;
            wal->offset_base_index = expected_index;
        } else if (seg_start_index != expected_index) {
            close(fd); goto init_fail;
        }

        uint64_t offset = WAL_V2_SEG_HEADER_SIZE;
        bool torn_write = false;

        while (offset < wal->segment_size_bytes) {
            uint8_t header[WAL_V2_FRAME_HEADER_SIZE];
            if (safe_pread(fd, header, WAL_V2_FRAME_HEADER_SIZE, offset) != WAL_V2_FRAME_HEADER_SIZE) break;

            uint32_t stored_crc = unpack_u32(header);
            uint32_t len = unpack_u32(header + 4);
            uint64_t index = unpack_u64(header + 16);

            if (len == 0 && index == 0) break;

            uint64_t frame_total = (uint64_t)WAL_V2_FRAME_HEADER_SIZE + len;
            if (len > WAL_V2_MAX_PAYLOAD_SIZE || frame_total > wal->segment_size_bytes - offset) {
                torn_write = true;
                break;
            }

            if (index == 0 || index <= wal->max_disk_index || index != expected_index) {
                close(fd); goto init_fail;
            }

            uint8_t* payload = NULL;
            if (len > 0) {
                payload = malloc(len);
                if (!payload) {
                    close(fd); goto init_fail;
                }
                if (safe_pread(fd, payload, len, offset + WAL_V2_FRAME_HEADER_SIZE) != len) {
                    torn_write = true;
                    free(payload);
                    break;
                }
            }

            uint32_t computed_crc = crc32_update(~0U, header + 4, WAL_V2_FRAME_HEADER_SIZE - 4);
            if (len > 0) computed_crc = crc32_update(computed_crc, payload, len);
            computed_crc = ~computed_crc;

            if (stored_crc != computed_crc) {
                torn_write = true;
                if (payload) free(payload);
                break;
            }
            if (payload) free(payload);

            if (!ensure_offset_capacity(wal, index)) { close(fd); goto init_fail; }

            uint64_t slot;
            if (wal_offset_slot(wal, index, &slot)) {
                wal->offsets[slot].seg_id = seg;
                wal->offsets[slot].offset = offset;
            }

            wal->max_disk_index = index;
            expected_index++;
            offset += frame_total;
        }

        if (torn_write) {
            if (seg < max_seg) {
                close(fd); goto init_fail;
            }

            wal->current_seg_id = seg;
            wal->active_fd = fd;
            wal->file_offset = offset;

            if (ftruncate(fd, offset) != 0) goto init_fail;
            if (!sync_file(fd)) goto init_fail;

            break;
        }

        if (seg == max_seg) {
            wal->active_fd = fd;
            wal->file_offset = offset;
        } else {
            close(fd);
        }
    }
    return 0;

init_fail:
    raft_wal_close(wal);
    return -1;
}

// -----------------------------------------------------------------------------
// APPEND & ROTATE
// -----------------------------------------------------------------------------

static int raft_wal_rotate(raft_wal_t* wal, uint64_t next_seq) {
    if (!wal || wal->active_fd < 0) return -1;

    close(wal->active_fd);
    wal->active_fd = -1;
    wal->current_seg_id++;

    char tmp_path[1024];
    snprintf(tmp_path, 1024, "%s/rotate_%010llu.tmp", wal->base_dir, (unsigned long long)wal->current_seg_id);
    char final_path[1024];
    get_segment_path(wal, wal->current_seg_id, final_path);

    int fd = -1;

    if (wal->standby_count > 0) {
        uint32_t slot = wal->standby_count - 1;
        char* standby_path = wal->standby_paths[slot];

        if (rename(standby_path, tmp_path) != 0) return -1;

        wal->standby_count--;
        wal->standby_paths[slot] = NULL;
        free(standby_path);

        fd = open(tmp_path, O_RDWR);
        if (fd < 0) return -1;

        if (ftruncate(fd, 0) != 0) { close(fd); return -1; }
    } else {
        fd = open(tmp_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) return -1;
    }

    if (preallocate_file(fd, wal->segment_size_bytes) != 0) { close(fd); return -1; }

    uint8_t hdr[WAL_V2_SEG_HEADER_SIZE];
    pack_u32(hdr, WAL_V2_MAGIC);
    pack_u64(hdr + 4, wal->current_seg_id);
    pack_u64(hdr + 12, next_seq);

    if (safe_pwrite(fd, hdr, WAL_V2_SEG_HEADER_SIZE, 0) != WAL_V2_SEG_HEADER_SIZE) { close(fd); return -1; }
    if (!sync_file(fd)) { close(fd); return -1; }

    if (rename(tmp_path, final_path) != 0) { close(fd); return -1; }
    if (!sync_dir(wal->base_dir)) { close(fd); return -1; }

    wal->active_fd = fd;
    wal->file_offset = WAL_V2_SEG_HEADER_SIZE;
    return 0;
}

int raft_wal_append(raft_wal_t* wal, uint64_t term, uint64_t index, uint8_t type, uint64_t client_id, uint64_t client_seq, const uint8_t* payload, uint32_t len) {
    if (!wal || wal->active_fd < 0 || !wal->batch_buf) return -1;
    if (len > 0 && !payload) return -1;
    if (len > WAL_V2_MAX_PAYLOAD_SIZE) return -1;
    if (index == 0 || index != wal->max_disk_index + 1) return -1;

    uint64_t total_size = (uint64_t)WAL_V2_FRAME_HEADER_SIZE + len;
    if (total_size > wal->segment_size_bytes - WAL_V2_SEG_HEADER_SIZE) return -1;

    if (wal->file_offset + wal->batch_len + total_size > wal->segment_size_bytes) {
        if (raft_wal_flush_batch(wal) != 0) return -1;
        if (raft_wal_rotate(wal, index) != 0) return -1;
    }

    size_t needed = wal->batch_len + total_size;
    if (needed > wal->batch_cap) {
        size_t new_cap = wal->batch_cap;
        while (new_cap < needed) {
            if (new_cap > SIZE_MAX / 2) return -1;
            new_cap *= 2;
        }
        uint8_t *new_buf = realloc(wal->batch_buf, new_cap);
        if (!new_buf) return -1;
        wal->batch_buf = new_buf;
        wal->batch_cap = new_cap;
    }

    uint8_t* ptr = wal->batch_buf + wal->batch_len;

    pack_u32(ptr + 4, len);
    pack_u64(ptr + 8, term);
    pack_u64(ptr + 16, index);
    ptr[24] = type;
    pack_u64(ptr + 25, client_id);
    pack_u64(ptr + 33, client_seq);

    if (len > 0) memcpy(ptr + WAL_V2_FRAME_HEADER_SIZE, payload, len);

    uint32_t crc = crc32_update(~0U, ptr + 4, WAL_V2_FRAME_HEADER_SIZE - 4);
    if (len > 0) crc = crc32_update(crc, payload, len);
    crc = ~crc;

    pack_u32(ptr, crc);

    if (!ensure_offset_capacity(wal, index)) return -1;

    uint64_t slot;
    if (!wal_offset_slot(wal, index, &slot)) return -1;

    wal->offsets[slot].seg_id = wal->current_seg_id;
    wal->offsets[slot].offset = wal->file_offset + wal->batch_len;

    wal->max_disk_index = index;
    wal->batch_len += total_size;
    return 0;
}

int raft_wal_flush_batch(raft_wal_t* wal) {
    if (!wal || wal->active_fd < 0 || !wal->batch_buf) return -1;
    if (wal->batch_len == 0) return 0;

    uint32_t batch_len = wal->batch_len;
    uint64_t offset = wal->file_offset;

    if (safe_pwrite(wal->active_fd, wal->batch_buf, batch_len, offset) != batch_len) {
        return -1;
    }

    if (!sync_file(wal->active_fd)) {
        return -1;
    }

    wal->file_offset += batch_len;
    wal->batch_len = 0;
    return 0;
}

// -----------------------------------------------------------------------------
// READ CACHE
// -----------------------------------------------------------------------------

int raft_wal_read_entry(raft_wal_t* wal, uint64_t target_index, uint64_t* out_term, uint8_t* out_type, uint64_t* out_cid, uint64_t* out_cseq, uint8_t** out_payload, uint32_t* out_len) {
    if (!wal || !out_term || !out_type || !out_cid || !out_cseq || !out_payload || !out_len) return 0;
    if (target_index == 0 || target_index > wal->max_disk_index) return 0;

    uint64_t slot;
    if (!wal_offset_slot(wal, target_index, &slot)) return 0;

    raft_wal_loc_t loc = wal->offsets[slot];
    if (loc.seg_id == 0) return 0;

    if (loc.offset > wal->segment_size_bytes) return 0;
    if (WAL_V2_FRAME_HEADER_SIZE > wal->segment_size_bytes - loc.offset) return 0;

    if (wal->read_seg_id != loc.seg_id) {
        if (wal->read_fd >= 0) {
            close(wal->read_fd);
            wal->read_fd = -1;
            wal->read_seg_id = 0;
        }

        char path[1024]; get_segment_path(wal, loc.seg_id, path);
        int fd = open(path, O_RDONLY);
        if (fd < 0) return 0;

        wal->read_fd = fd;
        wal->read_seg_id = loc.seg_id;
    }

    uint8_t header[WAL_V2_FRAME_HEADER_SIZE];
    if (safe_pread(wal->read_fd, header, WAL_V2_FRAME_HEADER_SIZE, loc.offset) != WAL_V2_FRAME_HEADER_SIZE) return 0;

    uint32_t stored_crc = unpack_u32(header);
    uint32_t len = unpack_u32(header + 4);
    uint64_t term = unpack_u64(header + 8);
    uint64_t frame_index = unpack_u64(header + 16);

    if (frame_index != target_index) return 0;

    uint64_t frame_total = (uint64_t)WAL_V2_FRAME_HEADER_SIZE + len;
    if (len > WAL_V2_MAX_PAYLOAD_SIZE || frame_total > wal->segment_size_bytes - loc.offset) return 0;

    uint8_t type = header[24];
    uint64_t cid = unpack_u64(header + 25);
    uint64_t cseq = unpack_u64(header + 33);
    uint8_t* payload = NULL;

    if (len > 0) {
        payload = malloc(len);
        if (!payload) return 0;
        if (safe_pread(wal->read_fd, payload, len, loc.offset + WAL_V2_FRAME_HEADER_SIZE) != len) {
            free(payload);
            return 0;
        }
    }

    uint32_t computed_crc = crc32_update(~0U, header + 4, WAL_V2_FRAME_HEADER_SIZE - 4);
    if (len > 0) computed_crc = crc32_update(computed_crc, payload, len);
    computed_crc = ~computed_crc;

    if (stored_crc != computed_crc) {
        if (payload) free(payload);
        return 0;
    }

    *out_term = term;
    *out_type = type;
    *out_cid = cid;
    *out_cseq = cseq;
    *out_len = len;
    *out_payload = payload;

    return 1;
}

// -----------------------------------------------------------------------------
// TAIL TRUNCATION (Raft Conflicts)
// -----------------------------------------------------------------------------

int raft_wal_truncate_tail(raft_wal_t* wal, uint64_t truncate_from_index) {
    if (!wal || wal->active_fd < 0) return -1;

    if (truncate_from_index == wal->max_disk_index + 1) return 0;

    if (truncate_from_index == 0 || truncate_from_index > wal->max_disk_index) return -1;

    uint64_t slot;
    if (!wal_offset_slot(wal, truncate_from_index, &slot)) return -1;

    raft_wal_loc_t loc = wal->offsets[slot];
    if (loc.seg_id == 0) return -1;

    if (raft_wal_flush_batch(wal) != 0) return -1;

    if (wal->current_seg_id != loc.seg_id) {
        if (wal->active_fd >= 0) {
            close(wal->active_fd);
            wal->active_fd = -1;
        }

        for (uint64_t bad_seg = wal->current_seg_id; bad_seg > loc.seg_id; bad_seg--) {
            char bad_path[1024]; get_segment_path(wal, bad_seg, bad_path);

            if (wal->read_fd >= 0 && wal->read_seg_id == bad_seg) {
                close(wal->read_fd);
                wal->read_fd = -1;
                wal->read_seg_id = 0;
            }

            if (wal->standby_count < wal->max_standby) {
                char* standby_path = strdupf("%s/standby_%010llu_%u.wal", wal->base_dir, (unsigned long long)bad_seg, wal->standby_count);
                if (standby_path && rename(bad_path, standby_path) == 0) {
                    wal->standby_paths[wal->standby_count++] = standby_path;
                } else {
                    if (standby_path) free(standby_path);
                    if (unlink(bad_path) != 0) return -1;
                }
            } else {
                if (unlink(bad_path) != 0) return -1;
            }
        }
        if (!sync_dir(wal->base_dir)) return -1;

        char path[1024]; get_segment_path(wal, loc.seg_id, path);
        wal->active_fd = open(path, O_RDWR);
        if (wal->active_fd < 0) return -1;
        wal->current_seg_id = loc.seg_id;
    }

    if (wal->read_fd >= 0 && wal->read_seg_id >= loc.seg_id) {
        close(wal->read_fd);
        wal->read_fd = -1;
        wal->read_seg_id = 0;
    }

    if (ftruncate(wal->active_fd, loc.offset) != 0) return -1;
    if (!sync_file(wal->active_fd)) return -1;

    for (uint64_t i = truncate_from_index; i <= wal->max_disk_index; i++) {
        uint64_t s;
        if (wal_offset_slot(wal, i, &s)) {
            wal->offsets[s] = (raft_wal_loc_t){0};
        }
    }

    wal->file_offset = loc.offset;
    wal->max_disk_index = truncate_from_index - 1;
    return 0;
}

// -----------------------------------------------------------------------------
// HEAD PURGING (Garbage Collection & Array Rebase)
// -----------------------------------------------------------------------------

int raft_wal_purge_head(raft_wal_t* wal, uint64_t safe_checkpoint_index) {
    if (!wal) return -1;

    while (wal->oldest_seg_id < wal->current_seg_id) {
        char path[1024]; get_segment_path(wal, wal->oldest_seg_id, path);

        char next_path[1024]; get_segment_path(wal, wal->oldest_seg_id + 1, next_path);
        int next_fd = open(next_path, O_RDONLY);
        if (next_fd < 0) return -1;

        uint64_t next_start_idx;
        bool valid_hdr = read_segment_header(next_fd, wal->oldest_seg_id + 1, &next_start_idx);
        close(next_fd);

        if (!valid_hdr) return -1;

        if (next_start_idx <= safe_checkpoint_index + 1) {
            if (wal->read_fd >= 0 && wal->read_seg_id == wal->oldest_seg_id) {
                close(wal->read_fd);
                wal->read_fd = -1;
                wal->read_seg_id = 0;
            }

            bool removed = false;
            if (wal->standby_count < wal->max_standby) {
                char* standby_path = strdupf("%s/standby_%010llu_%u.wal", wal->base_dir, (unsigned long long)wal->oldest_seg_id, wal->standby_count);
                if (standby_path && rename(path, standby_path) == 0) {
                    wal->standby_paths[wal->standby_count++] = standby_path;
                    removed = true;
                } else {
                    if (standby_path) free(standby_path);
                }
            }

            if (!removed) {
                if (unlink(path) != 0) return -1;
            }

            if (next_start_idx > wal->offset_base_index) {
                uint64_t shift = next_start_idx - wal->offset_base_index;
                if (shift < wal->offsets_cap) {
                    memmove(wal->offsets, wal->offsets + shift, (wal->offsets_cap - shift) * sizeof(raft_wal_loc_t));
                    memset(wal->offsets + (wal->offsets_cap - shift), 0, shift * sizeof(raft_wal_loc_t));
                } else {
                    memset(wal->offsets, 0, wal->offsets_cap * sizeof(raft_wal_loc_t));
                }
                wal->offset_base_index = next_start_idx;

                uint64_t live = wal->max_disk_index >= wal->offset_base_index
                              ? wal->max_disk_index - wal->offset_base_index + 1
                              : 0;

                uint64_t desired = 1024;
                while (desired < live) {
                    if (desired > UINT64_MAX / 2) {
                        desired = live;
                        break;
                    }
                    desired *= 2;
                }

                if (desired < wal->offsets_cap / 2) {
                    if (desired <= SIZE_MAX / sizeof(raft_wal_loc_t)) {
                        raft_wal_loc_t* smaller = calloc(desired, sizeof(*smaller));
                        if (smaller) {
                            memcpy(smaller, wal->offsets, live * sizeof(*smaller));
                            free(wal->offsets);
                            wal->offsets = smaller;
                            wal->offsets_cap = desired;
                        }
                    }
                }
            }

            wal->oldest_seg_id++;
            if (!sync_dir(wal->base_dir)) return -1;
        } else {
            break;
        }
    }
    return 0;
}

void raft_wal_close(raft_wal_t* wal) {
    if (!wal) return;

    if (wal->active_fd >= 0) raft_wal_flush_batch(wal);

    if (wal->active_fd >= 0) { close(wal->active_fd); wal->active_fd = -1; }
    if (wal->read_fd >= 0) { close(wal->read_fd); wal->read_fd = -1; }

    if (wal->standby_paths) {
        for (uint32_t i = 0; i < wal->standby_count; i++) {
            if (wal->standby_paths[i]) free(wal->standby_paths[i]);
        }
        free(wal->standby_paths);
        wal->standby_paths = NULL;
    }
    if (wal->offsets) { free(wal->offsets); wal->offsets = NULL; }
    if (wal->batch_buf) { free(wal->batch_buf); wal->batch_buf = NULL; }

    wal->standby_count = 0;
    wal->batch_len = 0;
    wal->batch_cap = 0;
}

// ============================================================================
// ENTERPRISE STORAGE FEATURES (Phase 3)
// ============================================================================

uint64_t raft_wal_verify_log_integrity(raft_wal_t* wal) {
    if (!wal || wal->max_disk_index == 0) return 0;

    for (uint64_t i = wal->offset_base_index; i <= wal->max_disk_index; i++) {
        uint64_t term, cid, cseq;
        uint8_t type;
        uint8_t* payload = NULL;
        uint32_t len = 0;

        // The read_entry function implicitly checks the frame CRC32.
        // If it fails, we have detected bit-rot on the disk.
        if (!raft_wal_read_entry(wal, i, &term, &type, &cid, &cseq, &payload, &len)) {
            return i;
        }

        if (payload) free(payload);
    }
    return 0; // 0 indicates perfect cryptographic integrity
}

int raft_wal_create_snapshot_manifest(const char* base_dir, uint64_t group_id, uint64_t snap_idx, uint64_t snap_term) {
    char dat_path[512], meta_path[512], tmp_path[512];
    snprintf(dat_path, 512, "%s/snap_grp%llu.dat", base_dir, (unsigned long long)group_id);
    snprintf(meta_path, 512, "%s/snap_grp%llu.meta", base_dir, (unsigned long long)group_id);
    snprintf(tmp_path, 512, "%s/snap_grp%llu.tmp_meta", base_dir, (unsigned long long)group_id);

    FILE* fdat = fopen(dat_path, "rb");
    if (!fdat) return -1;

    uint32_t crc = ~0U;
    uint64_t size = 0;
    uint8_t buf[4096];
    size_t r;
    while ((r = fread(buf, 1, sizeof(buf), fdat)) > 0) {
        crc = crc32_update(crc, buf, r);
        size += r;
    }
    fclose(fdat);
    crc = ~crc;

    FILE* fmeta = fopen(tmp_path, "wb");
    if (!fmeta) return -1;

    uint8_t hdr[32];
    pack_u32(hdr, 0x534E4150); // "SNAP" Magic
    pack_u64(hdr + 4, snap_idx);
    pack_u64(hdr + 12, snap_term);
    pack_u64(hdr + 20, size);
    pack_u32(hdr + 28, crc);

    if (fwrite(hdr, 1, 32, fmeta) != 32) { fclose(fmeta); return -1; }

    // Strict fsync before renaming
    fflush(fmeta);
    fsync(fileno(fmeta));
    fclose(fmeta);

    if (rename(tmp_path, meta_path) != 0) return -1;
    if (!sync_dir(base_dir)) return -1;

    return 0;
}

int raft_wal_verify_snapshot_manifest(const char* base_dir, uint64_t group_id, uint64_t* out_idx, uint64_t* out_term) {
    char dat_path[512], meta_path[512];
    snprintf(dat_path, 512, "%s/snap_grp%llu.dat", base_dir, (unsigned long long)group_id);
    snprintf(meta_path, 512, "%s/snap_grp%llu.meta", base_dir, (unsigned long long)group_id);

    FILE* fmeta = fopen(meta_path, "rb");
    if (!fmeta) return -1;

    uint8_t hdr[32];
    if (fread(hdr, 1, 32, fmeta) != 32) { fclose(fmeta); return -1; }
    fclose(fmeta);

    if (unpack_u32(hdr) != 0x534E4150) return -1; // Magic mismatch

    uint64_t snap_idx = unpack_u64(hdr + 4);
    uint64_t snap_term = unpack_u64(hdr + 12);
    uint64_t expected_size = unpack_u64(hdr + 20);
    uint32_t expected_crc = unpack_u32(hdr + 28);

    FILE* fdat = fopen(dat_path, "rb");
    if (!fdat) return -1;

    uint32_t crc = ~0U;
    uint64_t size = 0;
    uint8_t buf[4096];
    size_t r;
    while ((r = fread(buf, 1, sizeof(buf), fdat)) > 0) {
        crc = crc32_update(crc, buf, r);
        size += r;
    }
    fclose(fdat);
    crc = ~crc;

    if (size != expected_size || crc != expected_crc) return -1; // Checksum or size mismatch!

    if (out_idx) *out_idx = snap_idx;
    if (out_term) *out_term = snap_term;
    return 0;
}
