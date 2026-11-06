# Spec: Kafka-inspired internal redesign of JobQueue

_Status: Refined · Refined by: solution-architect · Date: 2026-11-06_

> **Note on provenance:** no doc-writer stage ran for this task; there was no
> prior `impact.md`/draft `spec.md`. This spec was written directly by the
> solution-architect from the task brief (itself very detailed) and the
> read-through of `solution/queue.go` and `main.go`. It is marked "Refined"
> because it already incorporates the clarifications below rather than
> leaving them as open questions; treat it as the spec of record for this
> task going forward.

## Goal

Redesign the *internal* data structures and concurrency model of
`solution.JobQueue` to borrow Kafka's good ideas (partitioning, log/offset
addressing, avoiding linear scans, low lock contention) while keeping the
queue's existing mutable lease/ack/fail semantics and its public Go API
byte-for-byte identical, so `main.go` (unmodifiable) keeps passing all three
levels with zero lost jobs and bounded duplication.

## Functional requirements

- **FR-1**: `solution.New`, `Enqueue`, `Lease`, `Ack`, `Fail`, `Stats`, and
  the types `LeasedJob`, `Stats`, `Clock` keep their exact current
  signatures and field names. No new exported symbols that `main.go` would
  need (it can't be changed) to compile.
- **FR-2**: `Enqueue` still returns a unique string ID immediately, is
  non-blocking, and the returned ID remains an opaque string as far as
  `main.go` is concerned (it never parses job IDs).
- **FR-3**: `Lease(workerID, leaseMS)` still returns `(nil, nil)` when no
  job is currently leasable, and otherwise atomically assigns exactly one
  job to `workerID` for `leaseMS` from `Clock.Now()`.
- **FR-4**: `Ack`/`Fail` keep validating current-lease ownership
  (`workerID` must match the live lease holder) and return an error
  otherwise, exactly as today.
- **FR-5**: A job whose lease has expired (i.e. `leaseExpiresAt <= now` and
  it is still in `leased` state) must become leasable again, to any
  worker, without requiring any other call in between (no separate
  "reaper" call from the harness exists).
- **FR-6**: `Stats()` must return accurate aggregate counts of
  pending/leased/completed jobs across the whole queue, consistent with
  `main.go`'s use of it as a drain/progress signal
  (`s.Pending == 0 && s.Leased == 0`).
- **FR-7**: `Lease` must not perform a full O(n) scan of all jobs in the
  queue in the common case; it must use a structure that finds an eligible
  job in sublinear time relative to the total job count.

## Non-functional requirements

- **NFR-1 (compatibility)**: `go build ./...`, `go vet ./...`, and
  `gofmt -l .` must stay clean; stdlib only, no new third-party
  dependencies.
- **NFR-2 (correctness under test)**: all three `main.go` levels keep
  passing with the same guarantees: 0 lost jobs at crash/ack-drop rates
  from 0.1% to 50%, and excess (duplicate) executions bounded roughly
  linearly in the failure rate (the existing Level 2/3 assertions must not
  regress).
- **NFR-3 (concurrency safety)**: the queue must remain safe for concurrent
  use by multiple goroutines even though the current harness happens to
  drive it from a single goroutine (`runUntilDrained`'s tick loop is
  sequential — confirmed by reading `main.go`). This is a defensive
  requirement, not something the harness currently exercises.
- **NFR-4 (incremental delivery)**: every intermediate commit on the way to
  the new design must keep `go run .` green (no half-migrated state that
  fails a level).
- **NFR-5 (no persistence)**: everything stays in-memory, in-process; no
  disk I/O, no network protocol. This would change the exercise's
  execution model (`go run .` as a plain binary) and is explicitly
  out of scope.

## Non-goals (and why)

- **Disk persistence / WAL** — out of scope per NFR-5; moved here from the
  original Kafka-comparison roadmap because it requires a recovery/replay
  path and fsync policy that this single-process exercise has no use for
  and the harness never restarts the process mid-run.
- **Network/client-server protocol, multiple broker processes,
  replication, Raft/consensus leader election** — out of scope; these
  change the public contract from "Go library" to "service," which
  directly violates the fixed-signature constraint (FR-1) and the
  single-process constraint (NFR-5).
- **Consumer groups / partition assignment / offset-commit replacing
  lease-based delivery** — out of scope; `Ack`/`Fail` must keep validating
  lease ownership on a mutable job record (per the task brief's explicit
  instruction that this stays a lease model, not an immutable-log-plus-cursor
  model). "Offset" is reused here only as an internal bookkeeping/addressing
  concept per shard, not as the external delivery model.
- **Bounded dedup/idempotency eviction window for completed jobs** —
  considered (per the task brief) and rejected: the current design already
  remembers every completed job forever, which is what makes duplicate
  `Ack`/`Fail` calls safely rejectable. Evicting old entries to save memory
  would reintroduce a correctness risk (a very late duplicate ack landing
  after eviction) for a problem the harness's finite, in-memory runs don't
  actually have (bounded job counts, no long-running retention need). Not
  worth the risk for no measured benefit in this exercise.

## Open questions resolved here

- "How many shards?" — a small fixed constant internal to the package
  (not exposed, since `New(clock)` can't take a new parameter). See
  architecture.md for the chosen value and reasoning.
- "Does true parallelism matter given `main.go` drives workers from one
  goroutine?" — No, confirmed by reading `main.go`: `runUntilDrained` calls
  `w.Tick()` for each worker in a plain `for` loop on one goroutine, so
  there is no lock contention to relieve today. The redesign's value here
  is algorithmic complexity (removing the O(n) scan) and future-proofing
  for concurrent callers (NFR-3), not contention relief in this specific
  harness. This is stated explicitly in architecture.md so the developer
  doesn't over-engineer for contention that doesn't exist yet.

## Change log

- 2026-11-06 — solution-architect: initial authoring of this spec directly
  from the task brief (no doc-writer stage preceded it); added FR-7
  (sublinear `Lease`) and NFR-3 (concurrency safety) as requirements the
  brief implied but didn't number explicitly; moved WAL/replication/
  network/consumer-groups/dedup-eviction into Non-goals with reasons,
  consistent with the brief's explicit rejection list.
