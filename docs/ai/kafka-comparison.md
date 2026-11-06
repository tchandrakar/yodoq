# From lease-based job queue to "Kafka-like": architecture comparison

_Status: Analysis only · Author: solution-architect · Date: 2026-11-06_

This document compares the current `solution/queue.go` implementation
against Apache Kafka's architecture and lays out what would actually be
required to evolve this project toward a Kafka-like distributed log. It is
a planning artifact, not an implementation task — no code changes are
proposed or made here.

## 1. What Kafka provides that this project doesn't

The current `JobQueue` is a **single-process, in-memory, mutable work
queue** with lease-based at-least-once delivery. Kafka is a **distributed,
durable, append-only log** with pull-based consumption. They solve
different problems, and the differences are structural, not incremental:

| Property | This project (`JobQueue`) | Kafka |
|---|---|---|
| Storage | `map[string]*job` + `[]string order`, RAM only, lost on process exit | Append-only segment files per partition, fsync'd to disk, retained for a configured time/size |
| Unit of work | A job that is leased, acked, and then **marked completed** (effectively removed from active work) | An immutable record in a log; consumers track an **offset** into the log, nothing is deleted on read |
| Delivery mechanism | Lease: a job is handed to one worker for a TTL; if not acked, it becomes re-leasable to anyone | Pull: each consumer in a group independently fetches from its assigned partition(s) starting at its committed offset |
| Parallelism | A single mutex-guarded linear scan of `order`; effectively one logical "queue" | N partitions per topic, each independently append/consumed; partitions are the unit of parallelism |
| Ordering | Best-effort by insertion order (`order` slice, O(n) scan can reassign any id) | Strict ordering only *within* a partition; none across partitions |
| Durability/availability | None — single process, no replication, no disk | Each partition is replicated across brokers (leader + followers); ISR (in-sync replica) set and `acks` config trade latency for durability |
| Consumer coordination | Implicit — any worker can call `Lease`; no concept of a named consumer group | Consumer groups with a group coordinator broker; partitions are assigned to group members, rebalanced on membership change |
| Offset/position tracking | None — "progress" is job state (pending/leased/completed) stored per job | Per-(group, partition) committed offset, itself stored in a Kafka topic (`__consumer_offsets`) |
| Retention | None — completed jobs stay in the map forever (unbounded memory growth; no history) | Time- or size-based retention of the log; consumers can replay history by resetting offsets |
| Transport | Direct in-process Go method calls (library) | A binary client-server protocol over TCP; producers/consumers are separate processes/machines from brokers |
| Throughput design | Mutex + linear scan, fine at the scale of a test harness | Sequential disk writes, zero-copy sendfile for fetches, batching/compression — designed for very high sustained throughput |

The core conceptual gap: this is a **queue** (work disappears once done),
Kafka is a **log** (work/events persist; "done" is just a cursor position
that a consumer owns). That distinction drives almost every other
difference below.

## 2. Concrete changes required to move toward the Kafka model

### 2.1 Persistence: WAL / segment files
Today everything lives in `q.jobs` / `q.order` in RAM; a process restart
loses all state. To get durability you need:
- An append-only **write-ahead log** per partition (a sequence of
  length-prefixed records on disk, or an embedded engine such as
  BoltDB/Badger/an LSM tree, or a hand-rolled segment-file format like
  Kafka's own `.log`/`.index` files).
- A **recovery path on startup**: replay the WAL to rebuild in-memory
  state (or drop in-memory state entirely and read-through from disk).
- Fsync/flush policy decisions: every write vs. batched vs. OS page cache
  only — this is the durability/latency knob.

**Trade-off:** durability requires disk I/O on the write path, which is
the single biggest latency hit relative to today's pure in-memory mutex
section. Group commit / batching recovers throughput at the cost of
slightly higher per-record latency and more complex buffering logic.

### 2.2 Partitioning strategy
Kafka parallelism comes from splitting a topic into N partitions, each an
independent ordered log, generally keyed by a hash of a message key
(`hash(key) % numPartitions`), or round-robin when there's no key.
Applied here, "jobs" would need to become "topics" containing partitioned
job streams:
- Decide what a job's "key" is (e.g. a tenant ID, entity ID, or the job
  type) so that related jobs land on the same partition and preserve
  relative order.
- Replace the single `order []string` with **N independent order
  structures**, one per partition, each with its own lock (or an
  actor/goroutine-per-partition model) to get real parallelism instead of
  one global mutex.
- `Lease`-equivalent (fetch) becomes partition-scoped: a worker/consumer
  reads from a specific partition at a specific offset, not "the next
  available job anywhere."

**Trade-off:** you gain parallelism and horizontal scalability, but lose
global ordering — ordering is now only guaranteed within a partition.
Choosing the partition key is an application-level decision with
long-term consequences (repartitioning existing data is expensive).

### 2.3 Replication
A single partition on a single disk is a durability and availability
single point of failure. Kafka replicates each partition across multiple
brokers with one elected leader (all writes/reads go through the leader;
followers replicate asynchronously or semi-synchronously, tracked via an
ISR set). To add this:
- Introduce the concept of **multiple broker processes/nodes**.
- A **replication protocol**: leader appends to its own log, ships the
  same records to followers, and only acknowledges a write to the
  producer once it meets the configured durability bar (`acks=1` =
  leader-only, `acks=all` = full ISR).
- **Leader election and failover**, typically via a consensus protocol.
  Kafka itself moved from a separate ZooKeeper-based controller to
  **KRaft** (Raft built into the brokers) for exactly this. In this
  codebase you'd similarly need Raft (e.g. via `hashicorp/raft` or
  `etcd/raft`) or an external coordinator to decide partition leadership
  and detect/handle broker failure.

**Trade-off:** this is the single biggest complexity jump in the whole
roadmap — distributed consensus, network partitions, split-brain
avoidance, and replica catch-up/truncation logic. It buys real
availability (tolerate node loss without data loss) at a large
engineering and operational cost.

### 2.4 Network layer
`JobQueue` today is called in-process via direct Go method calls
(`q.Enqueue(...)`, `q.Lease(...)`). A distributed system requires
producers and consumers to be separate processes, possibly on separate
machines, from the brokers holding the log. This means:
- Defining a **wire protocol** (gRPc/protobuf is the pragmatic modern
  choice; Kafka itself has a bespoke binary protocol for performance).
- A **server** process that accepts connections, authenticates/authorizes
  if needed, and dispatches to the partition-owning logic.
- A **client library** that replaces direct method calls with RPCs,
  handles connection pooling, retries, and discovering which broker is
  the current leader for a given partition (metadata requests).

**Trade-off:** every operation now has network latency and failure modes
(timeouts, partial failures, retries-causing-duplicates) that don't exist
in an in-process library call. This also changes testability — the fake
`Clock` abstraction stays useful, but tests now need a way to stand up an
in-process server+client pair rather than just calling methods on a
struct.

### 2.5 Consumer groups and offset management
This is the most fundamental semantic change. Today:
- A worker calls `Lease`, gets *the next available* job, and the queue
  mutates that job's state. There's no "which worker should read what" —
  it's a free-for-all over one shared pending set.

Kafka instead:
- Has named **consumer groups**; each partition within a topic is
  assigned to exactly one consumer *within* a group at a time (not one
  overall — different groups independently and fully re-read the same
  log).
- Tracks a **per-(group, partition) committed offset** — a position in an
  immutable log, not a mutable per-record state.
- Rebalances partition assignment when group membership changes (a
  consumer joins/leaves/crashes), coordinated by a group coordinator
  broker using a heartbeat/session-timeout protocol.

To move this codebase there you would need to:
- Replace per-job `state`/`leaseWorker`/`leaseExpiresAt` fields with a
  per-(group, partition) offset counter.
- Replace "any worker can lease any pending job" with "partitions are
  assigned to specific consumers in a group," requiring a rebalance
  protocol (even a simple range/round-robin static assignment to start).
- Replace the mutate-on-ack model (`Ack` → `stateCompleted`) with a
  commit-offset model (`Commit(group, partition, offset)`), where the
  record itself is never touched — only the cursor moves.
- Decide the **at-least-once vs. at-most-once vs. exactly-once** boundary
  explicitly: Kafka's default is at-least-once (commit after processing);
  exactly-once requires idempotent producers/transactions, a substantial
  additional protocol layer.

**Trade-off:** the lease TTL model is actually *simpler* to reason about
for single-attempt work distribution (no two consumers permanently own
the same partition) but doesn't scale horizontally the way partition
assignment does, and it has no replay capability. Offset-based consumption
gives you replay, multiple independent consumer groups reading the same
data, and partition-level parallelism, at the cost of needing explicit
rebalancing and offset-commit bookkeeping.

## 3. What should NOT change / can be reused vs. what's fundamentally different

### Reusable as-is or with minor adaptation
- **`Clock` interface** — directly reusable. A distributed system needs
  time abstraction even more (lease/session timeouts, log retention
  checks), and keeping it injectable is exactly how Kafka-like systems
  make their failure-detection logic testable.
- **The lease concept, but repurposed** — leases don't disappear, they
  move down a layer. Kafka brokers don't lease *records* to consumers,
  but they do use lease/session-timeout patterns internally (consumer
  group session timeouts, controller/leader leases in KRaft). The
  lease-expiry-and-reassignment logic already written here
  (`Lease`/expiry check in `Lease`) is conceptually the same mechanism
  used for *partition* reassignment on consumer failure, just applied one
  level up.
- **Crash-recovery semantics (bounded duplication under drops/crashes)**
  — this requirement doesn't go away, it's still what at-least-once
  delivery means. The test harness's crash/drop-ack simulation is a valid
  way to validate a future offset-commit model too (commit can be lost or
  duplicated exactly like an ack can today).
- **Single-writer-per-partition locking discipline** — the current
  "one mutex guards one ordered structure" pattern is the right shape,
  just needs to become one mutex (or actor) *per partition* instead of
  one global mutex over one global `order` slice.

### Fundamentally different, not an incremental change
- **Mutable job records vs. immutable log** — `Ack` currently *mutates*
  a job to `stateCompleted`; Kafka never mutates or deletes a record on
  consumption. This is not a refactor, it's a different data model:
  completed-vs-pending state must be replaced entirely by "consumer
  offset vs. log tail," and the job map (`map[string]*job`) replaced by
  an append-only sequence addressed by integer offset, not by job ID
  lookup.
- **Single logical queue vs. partitioned log** — there is no way to bolt
  partitions onto the existing `order []string` scan; it needs a
  fundamentally different indexing structure (per-partition offset
  arithmetic, not map+slice scanning).
- **In-process library vs. client-server** — this is an API-contract
  change, not just an implementation detail: `JobQueue` stops being a Go
  type callers import and becomes a network service with a
  protocol-versioned wire format.
- **No-consensus vs. consensus-based replication** — there is nothing in
  the current design to evolve; leader election/replication is new
  machinery built from scratch (or via an existing Raft library), not an
  extension of the mutex-guarded map.

## 4. Phased roadmap

| Phase | Scope | What it buys | Cost / complexity |
|---|---|---|---|
| **Phase 0 (today)** | In-memory lease queue, single process | Simple, correct, fast for the stated grading goal (bounded duplication, zero loss, in-process) | N/A — baseline |
| **Phase 1: Durable WAL** | Add an append-only log (segment files or embedded KV/LSM store) behind the existing `JobQueue` API; replay on startup | Survives process restart/crash without data loss; first real step toward "log" semantics | Moderate — need a log format, fsync policy, and a recovery/replay path; no API change yet |
| **Phase 2: Network server + client** | Wrap `JobQueue` in a server process exposing Enqueue/Lease/Ack/Fail (or Produce/Fetch/Commit) over gRPC or a custom protocol; provide a thin client library | Decouples producers/workers from the broker process; enables multiple machines/languages to participate | Significant — new protocol, connection handling, auth (if needed), and all the failure modes (timeouts, retries, partial writes) that a library call never had |
| **Phase 3: Partitioning** | Split a topic into N independently-locked, independently-logged partitions; add key→partition hashing; change consumption to be partition-scoped (fetch-by-offset rather than lease-any-pending) | Horizontal scalability and real parallelism; natural point to switch from lease-based delivery to offset-commit delivery per partition | Significant — partition key design is a long-lived data-modeling decision; consumer-side logic changes from "ask for any job" to "track my offset per partition" |
| **Phase 4: Consumer groups + rebalancing** | Introduce named groups, a coordinator that assigns partitions to group members, heartbeats/session timeouts, and offset commit storage | Multiple independent consumer groups can replay the same data; horizontal scaling of consumers with automatic failover | High — rebalance protocol correctness (avoiding duplicate/missed partition ownership during a rebalance) is one of the hardest parts of real Kafka |
| **Phase 5: Replication + consensus** | Leader/follower replicas per partition, ISR tracking, Raft-based (or similar) leader election and failover | Availability and durability survive broker loss, not just process restart | Highest — distributed consensus, split-brain avoidance, replica catch-up/truncation; this is most of the engineering effort in real-world Kafka/Raft systems |

Each phase is deliberately staged so the system is left in a coherent,
testable state: Phase 1 alone already strictly improves on today's
"everything lost on crash" behavior with no API change; Phases 2–3 are
where the project stops being a library and starts being a distributed
system; Phases 4–5 are where it starts resembling Kafka's actual
guarantees, at a cost of consensus-level complexity that dwarfs
everything built so far.
