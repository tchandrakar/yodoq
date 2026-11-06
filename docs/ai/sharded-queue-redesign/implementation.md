# Implementation notes: Sharded internal redesign of JobQueue

Branch: `feat/sharded-queue-redesign-v2` (see "Branch decision" below for why
this is a new branch rather than the original `feat/sharded-queue-redesign`).

## What was built

`solution.JobQueue`'s internals were replaced with `numShards = 16`
independent shards (`solution/shard.go`), each owning its own mutex, job
table (`map[int64]*job` keyed by a per-shard offset), one ready FIFO per
`Priority` level in use, and a `container/heap` min-heap of currently-leased
jobs ordered by `leaseExpiresAt` (`solution/expiry_heap.go`). Job IDs are
now `job-<shard>-<offset>`; `Ack`/`Fail` parse the shard straight out of the
ID (`parseJobID`) and dispatch to it directly — O(1), no scan.

`Enqueue` round-robins shards via a small counter guarded by a dedicated
mutex (`JobQueue.nextEnqueueShard`), and assigns each job a global,
atomically-increasing `seq` used later as Lease's FIFO tie-break.

`Lease` can't just take "the first shard with anything," because
`solution/priority_test.go` (pre-existing, from the advanced-challenges
work) requires the *globally* best-by-effective-priority job, tie-broken by
global enqueue order, regardless of which shard it landed in. So `Lease`:

1. For every shard, reclaims expired leases (pops the shard's heap while its
   top is expired, routing each job to either its priority bucket or to the
   terminal `Abandoned` state via `releaseJob`), then peeks — without
   popping — that shard's best ready candidate.
2. Picks the single best candidate across all `numShards` peeks (by
   `effectivePriority`, then `seq`).
3. Attempts to take exactly that job from its shard (`takeIfPending`), which
   re-validates it's still pending and still at the front of its bucket.
4. If another concurrent `Lease` call won the race for that exact job
   between steps 1-3, retries from step 1.

This costs O(numShards) shard-local peeks per `Lease` call — independent of
total job count `n` — in exchange for byte-identical priority/fairness/FIFO
semantics to the pre-sharding design. No code path ever holds more than one
shard's mutex at a time, so there's no lock-ordering deadlock risk.

A shard's pending jobs are bucketed by base `Priority`
(`map[Priority]*readyQueue`) rather than kept in one FIFO. This was the one
real design question T6 raised that the original (pre-priority)
architecture.md didn't anticipate: aging moves different-priority jobs'
effective scores at different rates, so a single FIFO can no longer
guarantee the next job to hand out sits at its front. Within one bucket it
still does — aging is monotonic in wait time, so the longest-waiting
(frontmost) job in a bucket always has the lowest (most urgent) effective
score, floored at `PriorityHigh`. `peekBest` therefore only ever needs to
look at each bucket's front, keeping selection O(distinct priorities in
use), not O(n).

`solution/priority.go`'s `candidate` type dropped its old `jobID`/single-
slice `orderIdx` fields in favor of `seq int64` (set once, cross-shard
comparable), since per-shard offsets aren't orderable against each other.

## Branch decision

Recreated as `feat/sharded-queue-redesign-v2` off current `main`, rather
than rebasing the original `feat/sharded-queue-redesign`. The original
branch's T1-T4 commits were built against `main` as it stood *before* the
advanced-challenges work (priority/aging/dedup/options) existed — a rebase
would have replayed a full non-priority `queue.go` rewrite on top of a
`queue.go` that had since been rewritten again, independently, by a
different effort. That's a worse starting point than just re-doing T1-T8
directly against today's `queue.go`, which is what plan.md's status note
and this file describe. The original branch's stashed T6 WIP (`shard.go`
keyed by plain offsets, no priority) and its `docs/ai/sharded-queue-redesign/`
docs were recovered from `git stash list`/the stash's untracked-files commit
and used as the starting point for this branch's design, then adapted.

## Deviations from the pre-redesign architecture.md

Recorded in detail in `architecture.md`'s "Addendum (2026-11-06)" section;
summarized here:

1. Ready FIFO → `map[Priority]*readyQueue` per shard (priority/aging didn't
   exist when the original approach was designed).
2. `Lease`'s shard dispatch → peek-every-shard-then-take-the-global-best
   with retry-on-conflict, instead of round-robin-stop-at-first-ready
   (same reason: priority/fairness correctness requires a global
   comparison, not a per-shard-first-come one).
3. Expiry heap entries are removed exactly via `heap.Remove(heapIndex)` on
   `Ack`/`Fail` (as T3/T4's actual code already did), not lazily
   invalidated as the original architecture.md's Decision D3 described —
   that document and the code it described had already diverged before
   this round of work; this implementation follows the code, not the
   stale decision text.
4. `job.seq` (new field) replaces the old single-slice order-index as the
   FIFO tie-break.
5. `Stats` gained `Abandoned`, aggregated the same way as the other three
   counters.

No other deviations. `main.go` was not touched. No new third-party
dependencies were added (stdlib only: `container/heap`, `sync`, `strconv`,
`strings`, `fmt`, `errors`, `math/rand` in tests).

## Tests added/adapted

- `solution/queue_test.go` (new): ported the original redesign's T1
  regression suite (Level 1 scenarios, FIFO order, expiry reclaim, Ack
  error contract, a trimmed crash/ack-drop sweep over 0.1%-50%), renamed
  `testClock`/`TestClock` → `regressionClock` to avoid colliding with
  `priorityTestClock`/`raceClock`, which already existed in the package
  from the advanced-challenges work. Added
  `TestJobQueue_EnqueueDistributesAcrossShards` and
  `TestJobQueue_StatsAggregatesAcrossShards` (T8).
- `solution/fairness_test.go`, `solution/options_test.go`: both had a
  direct `q.jobs[id]` map access, which no longer compiles — there's no
  longer one flat `map[string]*job`. Adapted both to call a new
  `q.lookupJob(id)` helper (routes through `parseJobID` to the owning
  shard, same pattern as `Ack`/`Fail`) instead of removing or weakening
  either assertion.
- `solution/concurrency_test.go`, `solution/priority_test.go`,
  `solution/dedup_test.go`, `solution/options_test.go` (remaining
  assertions): unchanged, pass as-is against the public API (T7's
  requirement).

## Commands run (all green)

- `gofmt -l .` — empty output at every commit.
- `go vet ./...` — clean.
- `go build ./...` — clean.
- `go test ./...` — all tests pass (existing + new).
- `go test -race ./...` — no races detected.
- `go run .` — Level 1 pass; Level 2 (200 jobs, 5 workers, 20% crash rate):
  0/200 lost; Level 3 sweep (0.1%-50% crash+ack-drop): 0/200 lost at every
  rate, bounded excess duplication at each (matches pre-redesign behavior
  exactly, since the harness itself wasn't touched).

## Known limitations / follow-ups

- `Lease`'s retry-on-conflict loop is unbounded; under the test suite's
  concurrency levels this is never observed to loop more than a handful of
  times, but a production version might want a bounded retry count with a
  fallback, or a different conflict-resolution strategy, under much higher
  contention than this exercise's harness ever produces.
- `numShards = 16` stays a compile-time constant, per the original
  architecture.md's Decision D2 (determinism over runtime tuning) — still
  true, unaffected by this round's changes.
- The sharded design necessarily gives up strict single-FIFO ordering
  *within* a priority level across the whole queue's history in the sense
  that two jobs of the same priority enqueued a nanosecond apart could, in
  extremely rare concurrent-caller scenarios, have their underlying shard
  offsets assigned out of strict wall-clock order relative to `seq` — `seq`
  itself is what's authoritative for tie-breaking, and it's assigned
  atomically in `Enqueue` before the shard dispatch, so this is a
  theoretical note rather than an observed issue; no test exercises or
  requires anything stronger.
