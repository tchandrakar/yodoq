# Implementation plan: Advanced challenges — Priorities, Fairness, Exactly-once-ish
Branch: feat/advanced-challenges   Base: main

Each step must leave `go build ./...`, `go vet ./...`, `gofmt -l .` (no
output), `go test ./...`, and `go run .` all green before moving to the
next step. Risky/foundational work (the regression-proof race test, then
the aging comparator that everything else depends on) comes first.

- [ ] T1 Add a concurrency/race test proving the existing single-mutex
  design already satisfies the Concurrency challenge — no production code
  change. Files: `solution/concurrency_test.go` (new). Tests: `go test
  -race ./solution/...` with many goroutines calling `Enqueue`/`Lease`/
  `Ack`/`Fail`/`Stats` concurrently on one `*JobQueue`; assert final
  `Stats()` counts reconcile (every enqueued job ends up accounted for,
  no panic/race). Covers: FR-7.

- [ ] T2 Introduce the `EnqueueOption` surface (no behavior change yet —
  `Enqueue`'s new `opts ...EnqueueOption` parameter is accepted but not
  yet acted on beyond storing defaults). Files: `solution/options.go`
  (new: `EnqueueOption`, `enqueueOptions`, `WithPriority`,
  `WithMaxAttempts`, `resolveOptions`); `solution/queue.go` (extend
  `Enqueue` signature, call `resolveOptions`, stamp new `job` fields
  `priority`/`enqueuedAt`/`maxAttempts`/`attempts=0`, all otherwise
  unused by `Lease` yet). Tests: existing `go run .` must still pass
  unchanged (proves the signature extension alone is a no-op); add a
  trivial `solution/options_test.go` asserting defaults
  (`PriorityNormal`, unlimited attempts) when no opts are passed. Covers:
  FR-1, FR-2 (partial — types defined), FR-9 (partial — option defined).

- [ ] T3 Implement priority levels + the aging comparator and wire it into
  `Lease`'s selection. Files: `solution/priority.go` (new: `Priority`
  consts, `agingIntervalMS`, `effectivePriority`, `candidate`,
  `betterCandidate`); `solution/queue.go` (`Lease`'s loop now folds every
  eligible job through `betterCandidate` instead of returning the first
  eligible match). Tests: `solution/priority_test.go` — (a) mixed
  priorities, no aging elapsed → High before Normal before Low; (b)
  all-default-priority jobs → order matches today's FIFO exactly
  (regression); (c) sustained High arrivals + one Low job, fake clock
  advanced in `agingIntervalMS` steps → Low is leased within the
  documented bound (`2 * agingIntervalMS`). Covers: FR-2, FR-3, FR-4,
  FR-5 (implicitly — selection still ignores `workerID`), NFR-3, NFR-5.

- [ ] T4 Implement the max-attempts / dedup-window mechanism. Files:
  `solution/dedup.go` (new: `recordAttempt`, `releaseJob`);
  `solution/queue.go` (`stateAbandoned` added to `jobState`; `Lease`
  calls `recordAttempt` when assigning a lease; the Leased→Pending
  transition inside `Lease`'s expiry handling and inside `Fail` both
  route through `releaseJob` and apply its returned state; `Stats` gains
  `Abandoned int` and a `case stateAbandoned` branch). Tests:
  `solution/dedup_test.go` — (a) job with `WithMaxAttempts(k)`, expired
  `k` times → becomes `Abandoned`, visible via `Stats().Abandoned`, never
  leased a `(k+1)`-th time; (b) job with no `WithMaxAttempts`, expired
  many times → stays `Pending`/re-leasable indefinitely (regression,
  proves default is unlimited); (c) `Ack`/`Fail` on an `Abandoned` job
  return the existing "does not hold the current lease" error (no new
  error type). Covers: FR-8 (doc comments added alongside), FR-9, FR-10,
  FR-11, NFR-3.

- [ ] T5 Memory-cost documentation for the attempt-tracking fields —
  no behavior change. Files: doc comments in `solution/dedup.go` and/or
  `solution/queue.go` on the `job` struct, stating the ~32 bytes/job
  overhead (4 extra machine words) and that it's unconditional (present
  whether or not `WithMaxAttempts`/`WithPriority` are used). Optionally
  add a `go test -bench` micro-benchmark (`solution/queue_bench_test.go`)
  measuring `unsafe.Sizeof(job{})` before/after, purely as documentation,
  not a correctness gate. Tests: `go vet ./...`, `gofmt -l .` stay clean;
  if a benchmark is added, `go test -bench=. ./solution/...` runs
  without failing. Covers: FR-12.

- [ ] T6 Fairness integration test combining priorities + sustained load
  across multiple simulated workers, to validate FR-3/FR-5/FR-6 together
  under something closer to the real harness shape (not just isolated
  unit tests). Files: `solution/fairness_test.go` (new) — N fake workers
  repeatedly calling `Lease`/`Ack` against a shared queue seeded with a
  High-priority flood plus a handful of Low-priority jobs and a couple of
  `WithMaxAttempts`-capped jobs that are deliberately never acked; assert
  (a) every Low-priority job is eventually leased within the documented
  bound, (b) no single `workerID` is starved relative to others over many
  rounds (track per-worker lease counts, assert they stay within a
  reasonable ratio of each other), (c) the capped jobs end up `Abandoned`
  and stop consuming scan time/attempts indefinitely. Tests: this file
  itself, run under `go test ./solution/...` (and `-race` as part of the
  normal race-enabled run alongside T1's test). Covers: FR-3, FR-5, FR-6
  (as an integration-level demonstration of the "argued, not built"
  rationale), FR-7 (incidentally, by running under `-race`).

- [ ] T7 Full-repo regression pass: run `go run .` and confirm Level 1,
  2, and 3 output is unchanged (zero jobs lost at every rate, duplication
  bounded as before) now that `main.go`'s unmodified call sites exercise
  the new default-priority/unlimited-attempts code paths. Files: none
  (verification step only) — if any regression is found, fix it in a
  follow-up commit before continuing, don't fold the fix silently into
  T3/T4's commits. Tests: `go run .` (manual inspection of Level
  1/2/3 pass/fail/warn lines), `go test ./...`, `go vet ./...`,
  `gofmt -l .`. Covers: NFR-1, NFR-3.

- [ ] T8 Update `README.md`'s "Advanced challenges" section: mark
  Concurrency, Priorities, Fairness, and Exactly-once-ish as implemented,
  each with a one-line pointer to where (`solution/concurrency_test.go`
  for Concurrency; `solution/priority.go` + `solution/queue.go`'s `Lease`
  for Priorities/Fairness; `solution/dedup.go` + `Stats.Abandoned` for
  Exactly-once-ish). Explicitly keep Persistence and Observability listed
  as deferred: Persistence with a pointer to
  `docs/ai/kafka-comparison.md` §2.1 (durability requires a WAL/disk
  layer, contradicts the in-memory constraint); Observability noted as a
  separate, currently-paused UI/event-bus effort (no doc pointer needed
  beyond that note, since no observability doc exists yet). Files:
  `README.md`. Tests: none (docs-only); `gofmt -l .`/`go vet ./...`
  unaffected. Covers: documentation completeness for this task's scope.
