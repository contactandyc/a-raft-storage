# a-raft-storage

`a-raft-storage` is a high-performance, strictly bounded Write-Ahead Log (WAL) and storage vault designed for consensus engines. 

While optimized for Raft, this library is completely blind to consensus logic. It is purely a segmented, append-only log that guarantees exactly-once recovery, hardware-level fsyncs, and protection against bit-rot.

## Key Features
* **O(1) Array Rebase:** Sliding-window offset mapping allows instant log truncation without memory leaks.
* **Torn-Write Protection:** Strict CRC32 validation on every frame detects and safely truncates mid-write power failures.
* **Online Bit-Rot Detection:** Background checkpointer (`raft_wal_verify_log_integrity`) sweeps sealed segments to detect decaying sectors while the node is live.
* **Cryptographic Manifests:** Generates and verifies `.meta` checksum files to mathematically bind `.dat` state machine snapshots to their Raft term/index.

## Architecture
Data is appended into pre-allocated, fixed-size `.wal` segments. Once a segment reaches capacity, the vault automatically executes a zero-copy rotation to a pre-warmed `standby_` file, ensuring disk allocation latency never blocks the main thread.
