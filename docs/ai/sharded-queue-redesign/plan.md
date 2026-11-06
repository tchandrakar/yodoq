# Implementation plan: Sharded internal redesign of JobQueue
Branch: feat/sharded-queue-redesign-v2   Base: main (post advanced-challenges merge)

Each step must leave `go build ./...`, `go vet ./...`, `gofmt -l .` (empty
output), `go test ./...`, `go test -race ./...`, and `go run .` (all three
levels, zero lost jobs) green before moving to the next step. Do not
change `main.go`.

Status note (2026-11-06): T1-T5 were implemented on the original
`feat/sharded-queue-redesign` branch, off an old `main` that predated
priority/aging/dedup/options. That branch was paused mid-T6. This plan
restarts clean on current `main` (which already has priority/aging/dedup/
options) and re-does T1-T8, adapting T6-T8 so the sharded design carries
priority/aging/dedup too, per architecture.md's Addendum.

- [x] T1 Port `solution/queue_test.go`'s regression tests (originally
      written against the pre-priority queue) so they pin external
      behavior on top of today's `main`: enqueue/lease/ack, fail-then-
      release, stale-ack rejection, FIFO order, expired-lease reclaim, and
      a trimmed crash+ack-drop sweep (`TestJobQueue_ZeroLossUnderCrashAndAckDrop`).
      No internals touched yet. — files: `solution/queue_test.go` —
      covers: NFR-2, NFR-4.

- [x] T2 Add `solution/expiry_heap.go` (`container/heap` min-heap over
      `*job` by `leaseExpiresAt`), standalone, unused by anything yet. —
      files: `solution/expiry_heap.go` — covers: FR-7 (building block).

- [x] T3 Add `heapIndex`/`seq` fields to `job` and wire `heapIndex`
      through `expiryHeap`'s `Swap`/`Push`/`Pop`. No behavior change
      yet. — files: `solution/queue.go` — covers: FR-7.

- [x] T4/T6 combined: introduce `numShards = 16` shards (`solution/shard.go`),
      each owning its own mutex, job table (`map[int64]*job` keyed by
      per-shard offset), a `map[Priority]*readyQueue` of pending jobs (one
      FIFO per priority level in use — see architecture.md Addendum for
      why a single FIFO no longer suffices once priority/aging exist), and
      an `expiryHeap`. Job IDs become `job-<shard>-<offset>`;
      `parseJobID` routes `Ack`/`Fail` straight to the owning shard.
      `Lease` reclaims each shard's expired leases, peeks each shard's
      best candidate (by `effectivePriority` then global `seq`), and takes
      the global best, retrying on a rare concurrent-take conflict (see
      architecture.md Addendum). This single step replaces the original
      plan's separate non-sharded T4 checkpoint, since main's current
      `Lease` was already a full O(n) priority-aware scan with no heap at
      all (the advanced-challenges work branched before T2-T4 existed) —
      there was no intermediate non-sharded-but-heap-based state worth
      shipping on its own. — files: `solution/shard.go` (new),
      `solution/queue.go` (rewritten), `solution/priority.go` (candidate
      simplified to effectivePriority+seq) — covers: FR-1, FR-2, FR-4,
      FR-6, FR-7.

- [x] T5 Checkpoint: `go run .` all three levels, `lost == 0` at every
      swept rate, bounded excess, unchanged from pre-redesign behavior. —
      files: none (verification only).

- [x] T7 `solution/concurrency_test.go` (already existed from the
      advanced-challenges work) re-verified green under `go test -race
      ./...` against the sharded design — no shared mutable state is ever
      touched outside exactly one shard's mutex per operation, so no new
      race was introduced. — covers: NFR-3.

- [x] T8 Added `TestJobQueue_EnqueueDistributesAcrossShards` and
      `TestJobQueue_StatsAggregatesAcrossShards`; confirmed
      `solution/priority_test.go`, `solution/dedup_test.go`,
      `solution/fairness_test.go` all pass unchanged against the public
      API (two of them needed a small adaptation from direct `q.jobs[id]`
      map access to a new `q.lookupJob(id)` helper, since the flat
      `map[string]*job` no longer exists — see implementation.md). Final
      full pass: `gofmt -l .`, `go vet ./...`, `go build ./...`, `go test
      -race ./...`, `go run .`. — files: `solution/queue_test.go`,
      `solution/fairness_test.go`, `solution/options_test.go` — covers:
      FR-6, FR-2, NFR-2/NFR-4 sign-off.
