# a-raft-library

`a-raft-library` is an embedded C library that implements the Raft consensus algorithm. It is designed to provide fault-tolerant, replicated state machines for distributed systems.

Unlike monolithic distributed databases, this library is strictly a consensus and replication engine. It handles leader election, log replication, and cluster membership, leaving the actual application of data (the State Machine) up to your specific program.

## Architecture & Features (Why use it)

The library is built on a strictly decoupled architecture. The Raft protocol logic (the "Brain") is completely isolated from the disk, network, and timing systems. A central event pump (`raft_node_pump`) coordinates these components asynchronously using `libuv`.

* **Decoupled Consensus Core:** The Raft logic (`raft_core`) processes inputs and yields a `raft_ready_t` struct containing messages to send, entries to save, and entries to apply. It performs no blocking I/O on its own.
* **Asynchronous Networking:** Built on `libuv`, the server multiplexes multiple distinct Raft groups over a shared pool of TCP peer connections. It handles randomized election timers, heartbeat ticks, and frame serialization.
* **Write-Ahead Log (AWAL):** The disk layer utilizes a highly optimized, lock-free ring buffer for batching writes. Entries are compressed using **LZ4**, tracked via a memory-mapped (`mmap`) index, and flushed to physical media using `fsync`/`fdatasync` to guarantee crash consistency.
* **Fail-Closed HardState Tracking:** Critical Raft metadata (Current Term, Voted For, and Commit Index) is atomically saved to disk using temporary files and POSIX `rename`, combined with parent-directory `fsync`s to prevent split-brain scenarios caused by power failures.
* **Security & Memory Boundaries:** The network codec enforces strict frame size limits (10MB) and entry counts (10,000 max) to prevent memory exhaustion and malicious buffer overflow attacks.

## Usage (How to use it)

The primary entry points are exposed through `raft_server.h`. Because it relies on `libuv`, all operations must run within a standard `uv_loop_t`.

### 1. Initialization and Networking

First, initialize the server, bind it to a local port to listen for incoming peers, and actively connect to known remote peers.

```c
#include <uv.h>
#include "a-raft-library/raft_server.h"

uv_loop_t* loop = uv_default_loop();
raft_server_t server;

// Init server: Node ID 1, max 10 raft groups, storing data in "/var/data"
raft_server_init(&server, loop, 1, 10, "/var/data");

// Listen for incoming Raft RPCs
raft_server_listen(&server, "0.0.0.0", 8080);

// Connect to remote cluster members (Node IDs 2 and 3)
raft_server_connect(&server, "192.168.1.102", 8080, 2);
raft_server_connect(&server, "192.168.1.103", 8080, 3);

```

### 2. Bootstrapping a Raft Node (Group)

A single server can host multiple distinct Raft consensus groups. You initialize a node by assigning it a Group ID and the list of initial cluster peers.

The library automatically checks the WAL and HardState files to recover from previous crashes.

```c
raft_node_t my_node;
uint64_t group_id = 1;
uint64_t initial_peers[] = {1, 2, 3};

// Initialize the node and bind it to Group 1
raft_node_init(&my_node, &server, group_id, initial_peers, 3);

```

### 3. Proposing Data

To append data to the replicated log, you issue a proposal to the node. If the node is the Leader, it will append the entry to its WAL and broadcast it. If the node is a Follower, it will either drop or forward the proposal depending on your application-level routing.

```c
const char* payload = "SET key=value";
uint32_t payload_len = strlen(payload);

// Propose the state change to the cluster
raft_node_propose(&my_node, (const uint8_t*)payload, payload_len);

```

### 4. Running the Event Loop

Because the library uses `libuv`, the engine runs entirely within the event loop.

```c
// Start processing timers, disk I/O, and network frames
uv_run(loop, UV_RUN_DEFAULT);

```

*(Note: To apply the committed entries to your actual application state, you must implement your logic inside the `ready.num_committed_entries` loop located within `raft_node_pump` in `raft_server.c`).*

## Internal Module Layout

If you need to modify the engine or understand its internals, the codebase is structured into the following domains:

* **`raft_core.c`**: The "Brain". A pure state machine that implements the Raft consensus algorithm. It manages terms, votes, log indices, and peer match indices without touching the disk or network.
* **`awal.c`**: The "Disk". Implements the asynchronous Write-Ahead Log. Handles LZ4 compression, block chunking, file truncation, and state recovery.
* **`raft_io.c`**: The Bridge. Wires the Raft Core to the AWAL. Handles safely booting the core from existing disk state and saving new `raft_ready_t` entries.
* **`raft_codec.c`**: The Serializer. Safely packs and unpacks Raft structs into binary byte arrays for network transit.
* **`raft_server.c`**: The "Central Nervous System". Manages the `libuv` TCP connections and timers. It contains `raft_node_pump()`, the critical function that queries the Core, writes to the Disk, sends over the Network, and advances the State Machine.
