## High-level view

Raft is a consensus algorithm for keeping a replicated state machine consistent across a cluster of servers. Each server stores the same ordered log of commands. Once a command is **committed**, every healthy server eventually applies it in the same order, producing the same state.

Raft simplifies consensus by separating the problem into five main areas:

1. **Leader election**: pick one server to coordinate the cluster.
2. **Log replication**: the leader receives client commands and replicates them.
3. **Commitment**: once a log entry is safely stored on a majority, it becomes committed.
4. **Safety**: no two servers may apply different commands at the same log index.
5. **Membership changes and log compaction**: allow the cluster to evolve and avoid unbounded log growth.

Raft has three server roles:

| Role          | Meaning                                             |
| ------------- | --------------------------------------------------- |
| **Follower**  | Passive server. Responds to leaders and candidates. |
| **Candidate** | Temporary role used during elections.               |
| **Leader**    | Handles client writes and log replication.          |

Time is divided into monotonically increasing **terms**. A term is like a logical epoch. At most one leader should be elected per term. If a server sees a higher term, it updates its own term and steps down to follower.

The core rule is: **the leader is the only server that accepts writes, and a write is committed only after it is replicated to a majority of the cluster.**

---

# 1. Core Raft flow

## 1.1 Leader election

Every server starts as a follower. If a follower stops hearing from a leader, it waits for a randomized election timeout. When that timeout expires, it becomes a candidate:

1. Increment its current term.
2. Vote for itself.
3. Send `RequestVote` messages to the other servers.
4. If it receives votes from a majority, it becomes leader.
5. If it sees a higher term, it steps down.
6. If no one wins, another randomized election happens.

A server grants its vote only if:

1. The candidate’s term is at least as new as its own.
2. It has not already voted for someone else in that term.
3. The candidate’s log is at least as up to date as its own.

The “log up-to-date” rule is critical. It prevents a server with an old or incomplete log from becoming leader and overwriting committed entries.

---

## 1.2 Log replication

When a client sends a command, the leader appends it to its own log and sends it to followers.

Each log entry contains:

```text
index, term, command
```

The leader sends followers an `AppendEntries` message containing:

```text
previous log index
previous log term
new entries
leader commit index
```

A follower accepts the new entries only if its log contains the previous index and term. This gives Raft the **log matching property**:

> If two servers have the same entry at the same index and term, then their logs are identical up through that index.

If the follower’s log does not match, it rejects the append. The leader then backs up and retries from an earlier index until it finds a matching prefix. Once the prefix matches, the leader overwrites the follower’s conflicting uncommitted entries.

---

## 1.3 Commitment

A leader considers an entry committed when:

1. The entry is stored on a majority of servers.
2. The entry belongs to the leader’s current term.

Once a current-term entry is committed, all previous entries in the log are also committed.

That second condition is subtle but important. A leader should not directly commit old entries from previous terms merely because they appear on a majority. Instead, once the leader commits one of its own current-term entries, older entries before it become committed indirectly.

This avoids a class of bugs where an entry from an old leader appears to be replicated on a majority but can still be overwritten by a future leader in certain election sequences.

---

## 1.4 Applying entries

Committed entries are applied to the replicated state machine in strict log order.

Each server tracks:

```text
commitIndex: highest log entry known to be committed
lastApplied: highest log entry applied to the state machine
```

A server may have entries in its log that are not yet committed. Those entries must not be applied.

The state machine safety rule is:

> If any server applies a command at log index `N`, no other server may ever apply a different command at index `N`.

That is the heart of Raft’s safety guarantee.

---

# 2. Main safety properties

Raft is built around several invariants.

## Election safety

At most one leader can be elected in a given term.

This is enforced because each server votes at most once per term, and a leader needs a majority. Two different candidates cannot both receive a majority in the same term because majorities overlap.

---

## Leader append-only

A leader never overwrites or deletes entries in its own log.

Leaders only append new entries. Conflicting entries are corrected on followers, not on the leader.

---

## Log matching

If two logs contain an entry with the same index and term, then all earlier entries are identical.

This is enforced by the `prevLogIndex` and `prevLogTerm` check during append.

---

## Leader completeness

If an entry is committed in a given term, then that entry will be present in the logs of all future leaders.

This is enforced by the voting rule: a candidate cannot win unless its log is at least as up to date as the logs of a majority.

---

## State machine safety

No two servers apply different commands at the same log index.

This follows from the previous properties plus the rule that servers only apply committed entries in order.

---

# 3. Conditions and edge cases Raft must handle

## A. Election-related conditions

### 1. No leader exists

Condition: followers stop receiving heartbeats.

Handling: after randomized election timeouts, followers become candidates and request votes.

---

### 2. Split vote

Condition: multiple followers time out at roughly the same time and become candidates.

Handling: randomized election timeouts make repeated split votes unlikely. If no candidate wins, the term advances and another election occurs.

---

### 3. Candidate receives a message from a valid leader

Condition: a candidate receives `AppendEntries` from a leader with a term at least as new as its own.

Handling: candidate steps down to follower.

---

### 4. Server sees a higher term

Condition: any server receives `RequestVote` or `AppendEntries` with a higher term.

Handling: it updates its term, clears its vote for the old term, and becomes follower.

---

### 5. Old leader still believes it is leader

Condition: a former leader is isolated, then later reconnects.

Handling: it sees a higher term from newer servers and steps down. Until it contacts a majority, it cannot commit new entries.

---

### 6. Candidate has stale log

Condition: candidate asks for votes but lacks committed or more recent entries.

Handling: voters reject it if their own logs are more up to date.

“Up to date” is usually judged by:

1. Higher last log term wins.
2. If terms are equal, higher last log index wins.

---

### 7. One server votes twice after crash

Condition: server crashes after voting, restarts, and forgets its vote.

Handling: `currentTerm` and `votedFor` must be persisted before responding to vote requests.

---

### 8. Leader elected but has not committed anything from its own term

Condition: new leader has old entries but no committed current-term entry.

Handling: leader should usually append a no-op entry at the start of its term. Once that no-op is committed, the leader knows all prior entries are safely committed.

This also helps make linearizable reads safe.

---

## B. Log replication conditions

### 9. Follower is missing entries

Condition: follower is behind the leader.

Handling: leader sends missing entries starting from the follower’s `nextIndex`.

---

### 10. Follower has conflicting entries

Condition: follower contains entries from an old leader that conflict with the new leader’s log.

Handling: follower rejects append until leader finds a matching prefix. Then follower deletes conflicting uncommitted entries and accepts the leader’s entries.

---

### 11. Follower has extra uncommitted entries

Condition: follower accepted entries from an old leader that were never committed.

Handling: new leader overwrites them if they conflict with its own log.

---

### 12. Follower has entries that the leader lacks

Condition: follower is ahead due to previous uncommitted work.

Handling: if those entries are not committed, they may be deleted. A follower’s log is not authoritative; the leader’s log is.

---

### 13. AppendEntries messages arrive out of order

Condition: network reorders replication messages.

Handling: followers use `prevLogIndex` and `prevLogTerm` checks. Old or inconsistent messages are rejected or ignored.

---

### 14. Duplicate AppendEntries

Condition: network retries or duplicates messages.

Handling: appending should be idempotent. If the follower already has the entry, it should not duplicate it.

---

### 15. Leader crashes after appending locally but before replication

Condition: client command exists only on old leader.

Handling: entry is uncommitted and may be lost. Client must retry with the new leader.

---

### 16. Leader crashes after replication but before replying to client

Condition: command may already be committed, but client did not receive confirmation.

Handling: client retries. To avoid duplicate execution, the system should use client request IDs or sequence numbers.

---

### 17. Entry replicated to minority only

Condition: leader writes to itself and one follower in a five-node cluster, then crashes.

Handling: entry is uncommitted and may be overwritten by a new leader.

---

### 18. Entry replicated to majority but from older term

Condition: an old-term entry is stored on a majority, but no current-term entry has been committed by the current leader.

Handling: do not directly commit it by counting replicas. Commit a current-term entry first; previous entries become committed indirectly.

---

### 19. Follower rejects due to missing previous entry

Condition: leader believes follower has an index, but follower does not.

Handling: leader decrements `nextIndex` and retries. Efficient implementations include conflict hints so the leader can jump back faster.

---

### 20. Follower rejects due to previous term mismatch

Condition: follower has the index but with a different term.

Handling: same as above: leader backs up to the last matching prefix, then overwrites conflicting suffix.

---

## C. Commit and apply conditions

### 21. Server applies an uncommitted entry

Condition: implementation applies an entry before it is committed.

Handling: forbidden. Only entries up through `commitIndex` may be applied.

---

### 22. Entries are committed out of order

Condition: later entry is committed before earlier one.

Handling: Raft commits logically in order. If index `N` is committed, all prior entries are also committed.

---

### 23. Follower learns about commit later

Condition: leader has committed an entry, but follower has not yet learned that.

Handling: leader sends `leaderCommit` in heartbeats and append messages. Follower advances its own `commitIndex` to the minimum of leader’s commit index and its own last log index.

---

### 24. Crash after commit but before apply

Condition: server knows an entry is committed, crashes before applying it.

Handling: on restart, it replays committed log entries or restores from snapshot and continues applying from `lastApplied`.

---

### 25. Command applied twice

Condition: server crashes after applying but before recording application progress.

Handling: either persist application progress carefully or make the state machine idempotent using client request IDs.

---

## D. Client interaction conditions

### 26. Client contacts follower

Condition: client sends write to non-leader.

Handling: follower rejects or redirects to known leader.

---

### 27. Client contacts stale leader

Condition: partitioned old leader still thinks it is leader.

Handling: old leader cannot commit without majority. For reads, special care is required; stale leaders must not serve linearizable reads unless they verify authority.

---

### 28. Client retries a command

Condition: client times out and resends the same command.

Handling: Raft-level replication may produce duplicate log entries unless the service layer deduplicates. Use client IDs and monotonically increasing request IDs.

---

### 29. Client receives no response

Condition: leader crashes after committing but before responding.

Handling: client retries. New leader or state machine should detect whether the request has already been executed.

---

### 30. Linearizable reads

Condition: reads must reflect the latest committed write (linearizability is a stronger, real-time guarantee compared to standard serializability, ensuring a read sees any write completed before the read began).

Handling options:

1. Route reads through the leader and make the leader confirm it still has authority via a quorum heartbeat or ReadIndex protocol.
2. Use leader leases, but only with bounded clock drift assumptions.
3. Have the leader commit a no-op entry after election before serving reads.

Plain local reads from a leader are unsafe if the leader may have been deposed without knowing it.

---

## E. Network partition conditions

### 31. Leader partitioned away from majority

Condition: old leader can reach only a minority.

Handling: it cannot commit new entries. The majority side elects a new leader.

---

### 32. Minority partition elects leader

Condition: minority side tries to elect a leader.

Handling: impossible if quorum rules are correct. A leader requires majority votes.

---

### 33. Network heals with two apparent leaders

Condition: old leader and new leader coexist briefly.

Handling: terms resolve this. The leader with the older term steps down after receiving a message with the newer term.

---

### 34. Reordered messages from older terms

Condition: old RPCs arrive after a new term has started.

Handling: reject messages with stale terms.

---

### 35. Message loss

Condition: heartbeats, votes, or append messages are lost.

Handling: Raft retries. Safety does not depend on reliable delivery, only eventual communication with a majority.

---

## F. Persistence and crash recovery conditions

### 36. Server crashes and restarts

Condition: server loses volatile state.

Handling: persistent state must include at least:

```text
currentTerm
votedFor
log entries
snapshot metadata
```

Volatile state can be reconstructed:

```text
commitIndex
lastApplied
nextIndex
matchIndex
```

Some implementations persist additional state for performance or exactly-once semantics.

---

### 37. Crash after receiving log entry but before persisting it

Condition: follower responds success before durable write.

Handling: unsafe. A server must persist log entries before acknowledging replication.

---

### 38. Crash after voting but before persisting vote

Condition: server grants vote, crashes, then grants another vote in same term.

Handling: unsafe. Persist vote before responding.

---

### 39. Disk corruption or data loss

Condition: persistent log is corrupted.

Handling: Raft by itself assumes stable storage. Production systems need checksums, snapshots, replication repair, and possibly operator intervention.

---

## G. Snapshot and log compaction conditions

### 40. Log grows without bound

Condition: replicated log becomes too large.

Handling: periodically create snapshots of the state machine and discard log entries covered by the snapshot.

---

### 41. Follower is far behind

Condition: leader no longer has the old log entries needed to catch up the follower.

Handling: typically, servers independently generate snapshots of their own state machines to compact their logs. A leader only sends an `InstallSnapshot` message over the network to a follower if that follower's log is so far behind that it requires entries the leader has already discarded.

---

### 42. Snapshot overlaps existing follower log

Condition: follower receives a snapshot whose last included index is already present.

Handling: follower keeps any suffix that matches the snapshot’s last included term/index and discards older covered entries.

---

### 43. Snapshot is stale

Condition: follower receives an older snapshot than the state it already has.

Handling: ignore or reject it.

---

### 44. Snapshot loses membership information

Condition: snapshot omits latest cluster configuration.

Handling: unsafe. Snapshots must include enough metadata to resume consensus, including last included index, last included term, and relevant membership configuration.

---

### 45. Crash during snapshot installation

Condition: follower crashes halfway through installing snapshot.

Handling: snapshot installation must be atomic or recoverable. Partial snapshots should not replace the current durable state.

---

## H. Membership-change conditions

Changing the set of servers is dangerous because two different configurations might each form separate majorities.

### 46. Adding or removing nodes

Condition: cluster membership changes.

Handling: use **single-server configuration changes** (adding or removing exactly one node at a time) or joint consensus.

Single-server changes are mathematically proven safe, much easier to implement, and are the standard approach in most modern production systems.

In Raft’s original joint consensus approach for multi-node changes, the cluster temporarily uses a combined configuration:

```text
old configuration + new configuration

```

During this phase, entries must be committed by majorities of both configurations. Then the system transitions fully to the new configuration.

---

### 47. Removing the current leader

Condition: leader is removed from the configuration.

Handling: leader steps down once the new configuration is committed and it is no longer a member.

---

### 48. Adding a new empty server

Condition: new server has no log.

Handling: it should catch up before being allowed to affect quorum decisions, or the system should use a staged/non-voting learner role.

---

### 49. Removing too many nodes at once

Condition: configuration change destroys quorum availability.

Handling: avoid large unsafe changes. Prefer one-at-a-time changes or joint consensus with careful quorum checks.

---

### 50. Membership change during network partition

Condition: some servers see old config, others see new config.

Handling: joint consensus ensures any successful commit intersects both configurations, preserving safety.

---

# 4. Important timing assumptions

Raft does not require synchronized clocks for correctness. Safety holds even with arbitrary delays, as long as messages are not corrupted and persistent storage behaves correctly.

However, liveness depends on timing. The usual condition is:

```text
broadcast time << election timeout << mean time between failures
```

Meaning:

1. Heartbeats and replication should usually complete much faster than the election timeout.
2. Election timeout should be long enough to avoid unnecessary elections.
3. But it should be short enough to recover promptly from leader failure.

Common production enhancements include:

* randomized election timeouts;
* pre-vote to avoid disruptive elections by isolated nodes;
* leader check-quorum behavior;
* adaptive heartbeat intervals;
* backoff during repeated election failures.

---

# 5. Making Raft efficient for high transaction throughput

Raft is simple, but naive Raft can be slow. The bottlenecks are usually disk sync, network round trips, leader CPU, serialization, and follower lag.

## 5.1 Batch log entries

Instead of sending one `AppendEntries` RPC per transaction, the leader should batch multiple commands into one append.

This amortizes:

* network overhead;
* disk fsync cost;
* serialization cost;
* quorum acknowledgment cost.

Batching improves throughput, but increases latency if batches wait too long. Production systems usually use size-based and time-based flush thresholds.

Example:

```text
Flush when either:
- batch reaches 256 KB, or
- 1 ms has elapsed, or
- there is an explicit durability barrier.
```

---

## 5.2 Pipeline replication

The leader should not wait for one append round to finish before sending the next. It should keep multiple replication requests in flight to each follower.

Without pipelining:

```text
send batch
wait for majority
send next batch
wait for majority
```

With pipelining:

```text
send batch 1
send batch 2
send batch 3
receive acknowledgments asynchronously
advance commit index as quorums form
```

This is essential for high throughput over nontrivial network latency.

---

## 5.3 Use group commit

Disk fsync is expensive. Instead of syncing every transaction independently, the leader and followers can sync a batch of log entries together.

The safety rule remains:

> Do not acknowledge replication until the relevant log entries are durable.

But many entries can become durable with a single disk flush.

---

## 5.4 Separate append, commit, and apply paths

A high-throughput implementation should decouple:

1. accepting client commands;
2. appending to local log;
3. replicating to followers;
4. advancing commit index;
5. applying committed entries to the state machine;
6. responding to clients.

These stages can run concurrently while preserving log order.

---

## 5.5 Optimize follower catch-up

The basic Raft algorithm backs up `nextIndex` one entry at a time when a follower rejects an append. That can be slow.

Use conflict hints:

```text
rejected term
first index of rejected term
last known index
```

The leader can then jump back by term rather than decrementing one index at a time.

---

## 5.6 Use snapshots for very lagging followers

If a follower is far behind, replaying millions of log entries may be slower than sending a snapshot.

Efficient systems decide between:

```text
send missing log suffix
```

and:

```text
send snapshot
```

based on size, distance behind, and current log retention.

---

## 5.7 Keep slow followers from blocking throughput

A leader only needs a majority. Slow followers should not block normal commits.

The leader should track:

```text
nextIndex[follower]
matchIndex[follower]
replication lag
in-flight bytes
```

Slow followers can receive data at their own pace. The leader should avoid letting one lagging follower consume unbounded memory or network bandwidth.

---

## 5.8 Prefer stable leadership

Frequent elections destroy throughput.

To reduce unnecessary leader changes:

* use well-tuned election timeouts;
* use **pre-vote** (a candidate must check if it can win an election by pinging others before actually incrementing its term, preventing a temporarily partitioned node from disrupting a stable leader when it reconnects);
* use check-quorum;
* avoid GC pauses or long stop-the-world events;
* avoid overloaded leaders;
* avoid heartbeat starvation behind large replication messages.

---

## 5.9 Use efficient read paths

Writes require quorum replication. Reads can be optimized, but only if linearizability is preserved.

Common options:

### Quorum-confirmed leader read

Leader confirms it still controls a majority before serving read. Safe, but costs a quorum round.

### ReadIndex

Leader piggybacks read safety on heartbeats and commit index. Common in production Raft systems.

### Lease read

Leader serves reads locally during a lease interval. Fastest, but depends on bounded clock drift and careful lease management.

### Follower reads

Followers can serve stale reads easily. Linearizable follower reads require coordination with the leader or quorum.

---

## 5.10 Avoid unnecessary fsyncs for reads

Reads should not usually touch the Raft log unless they require a barrier. For linearizable reads, ReadIndex or leases can avoid appending a log entry for every read.

Appending a no-op or read marker for every read is safe but can destroy read throughput.

---

## 5.11 Use learners for scaling replication

Adding many voting members hurts throughput because quorum size grows and the leader has more replication work.

Use non-voting learners for:

* read replicas;
* backup replicas;
* analytics replicas;
* new nodes catching up;
* geographically distant replicas.

They receive the log but do not count toward quorum.

---

## 5.12 Keep the voting group small

Raft works best with small voting groups, usually 3 or 5 nodes.

Tradeoff:

| Cluster size | Can tolerate | Majority size | Throughput impact   |
| ------------ | -----------: | ------------: | ------------------- |
| 3            |    1 failure |             2 | High throughput     |
| 5            |   2 failures |             3 | Moderate throughput |
| 7            |   3 failures |             4 | Lower throughput    |

Larger clusters increase fault tolerance but reduce write efficiency.

For very large systems, the usual approach is not one huge Raft group. Instead, use many small Raft groups, often called shards or partitions.

---

## 5.13 Shard the workload

One Raft group has one leader, so write throughput is bounded by that leader and its quorum.

To scale horizontally, split data into many independent Raft groups:

```text
shard 1 -> Raft group A
shard 2 -> Raft group B
shard 3 -> Raft group C
...
```

Each group has its own leader. Leaders can be spread across machines, allowing aggregate throughput to scale.

This is how many distributed databases use Raft-like consensus at high scale.

---

## 5.14 Reduce command size

The Raft log should contain compact commands, not huge payloads when avoidable.

For large values:

* store payload externally and replicate references, if safe;
* chunk large entries;
* compress batches;
* avoid mixing huge entries with latency-sensitive small entries.

Large entries can cause head-of-line blocking.

---

## 5.15 Manage backpressure

The leader needs backpressure when clients submit commands faster than the cluster can replicate or apply them.

Track limits such as:

```text
max uncommitted log bytes
max in-flight append bytes
max client queue size
max apply backlog
```

Without backpressure, the leader can run out of memory or create enormous recovery delays.

---

# 6. Practical Raft design checklist

A robust Raft implementation should address at least these conditions:

1. Persist `currentTerm`, `votedFor`, and log entries before acknowledging relevant RPCs.
2. Use randomized election timeouts.
3. Reject stale-term RPCs.
4. Step down immediately on observing a higher term.
5. Allow only one vote per term.
6. Enforce candidate log up-to-date checks.
7. Use `prevLogIndex` and `prevLogTerm` for append consistency.
8. Delete only conflicting uncommitted follower entries.
9. Commit entries only after majority replication.
10. Directly commit only current-term entries as leader.
11. Apply entries only after commitment and strictly in index order.
12. Deduplicate client retries at the service layer.
13. Prevent stale leaders from serving unsafe reads.
14. Use a safe read protocol: ReadIndex, quorum read, or carefully implemented lease read.
15. Use snapshots to compact logs.
16. Make snapshot installation atomic.
17. Include configuration metadata in snapshots.
18. Use joint consensus or another safe method for membership changes.
19. Use learners for new or non-voting replicas.
20. Avoid letting slow followers block the majority path.
21. Use batching, pipelining, and group commit for throughput.
22. Tune election and heartbeat timing to avoid needless leadership churn.
23. Apply backpressure when replication or application falls behind.
24. Use checksums and storage integrity mechanisms for production durability.
25. Monitor leader changes, replication lag, commit latency, disk fsync latency, and apply backlog.

---

# 7. The essential mental model

Raft is safe because every committed decision is protected by a majority, and every future majority must overlap with that majority. The election rules ensure that a future leader cannot be missing committed entries. The log-matching rules ensure that followers converge to the leader’s log. The commit rules ensure that only stable entries reach the state machine.

Raft is efficient when the implementation treats consensus as a streaming replicated log rather than a sequence of isolated transactions: batch aggressively, pipeline replication, group disk commits, keep the voting set small, shard when necessary, and use safe optimized read paths.
