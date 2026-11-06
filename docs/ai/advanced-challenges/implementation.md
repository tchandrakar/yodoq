# Implementation: Advanced challenges — Priorities, Fairness, Exactly-once-ish

_Status: Complete · Author: developer · Branch: `feat/advanced-challenges` · Base: `main`_

## What was built

Following `architecture.md` and `plan.md`, with two user-approved amendments
applied on top (see below):

- **`solution/options.go`** (new): `EnqueueOption` (`WithPriority`,
  `WithMaxAttempts`, `resolveOptions`) and `QueueOption`
  (`WithAgingIntervalMS`, `resolveQueueOptions`).
- **`solution/priority.go`** (new): `Priority` levels (`PriorityHigh`,
  `PriorityNormal`, `PriorityLow`), `effectivePriority` (aging math),
  `candidate`/`betterCandidate` (the selection comparator `Lease` folds every
  eligible job through).
- **`solution/dedup.go`** (new): `recordAttempt`, `releaseJob` — the
  attempt-tracking / abandon-on-exhaustion logic.
- **`solution/queue.go`** (modified): `New` and `Enqueue` gained variadic
  trailing options; `job` gained `priority`, `enqueuedAt`, `attempts`,
  `maxAttempts`; `jobState` gained `stateAbandoned`; `Stats` gained
  `Abandoned int`; `Lease`'s scan now picks the best eligible job by
  effective priority (instead of the first), records an attempt on the
  winner, and abandons expired jobs that have exhausted `maxAttempts`;
  `Fail` routes its state transition through `releaseJob`.
- Tests: `solution/concurrency_test.go`, `solution/options_test.go`,
  `solution/priority_test.go`, `solution/dedup_test.go`,
  `solution/fairness_test.go`.
- `README.md`: "Status", "Architecture" (including the Mermaid diagram), and
  "Advanced challenges" sections updated to describe and mark Concurrency,
  Priorities, Fairness, and Exactly-once-ish as implemented.

`main.go` was not modified, as required.

## Amendments applied (user-approved, on top of architecture.md/plan.md)

1. **Aging rate made configurable, not a fixed constant.** `architecture.md`
   proposed `agingIntervalMS` as a package-level `const`. Per the caller's
   explicit instruction, this was changed to a per-queue, configurable value:
   `New(clock Clock, opts ...QueueOption) *JobQueue` plus
   `func WithAgingIntervalMS(ms int64) QueueOption`, defaulting to 2000ms
   (`defaultAgingIntervalMS`) when omitted or when `ms <= 0`. `New(clock)`
   (main.go's only call site, zero opts) keeps compiling and behaving
   identically. `effectivePriority` takes `agingIntervalMS` as a parameter
   rather than reading a package constant.
2. **Exactly-once-ish = bound re-leases.** Implemented exactly as
   `architecture.md`/`plan.md` designed: opt-in `WithMaxAttempts(k)`, default
   `0` = unlimited = today's behavior, exhausted jobs move to a new terminal
   `stateAbandoned`, surfaced via the additive `Stats.Abandoned` field. No
   "did it actually run" execution signal is required or assumed — see the
   honesty note in `solution/dedup.go`'s doc comment on `releaseJob`.

No other deviations from `architecture.md`/`plan.md` were needed; the design
mapped onto the code cleanly as proposed.

## Tests added

| File | Covers |
|---|---|
| `solution/concurrency_test.go` | FR-7 — many goroutines calling Enqueue/Lease/Ack/Fail/Stats concurrently under `-race`; final `Stats()` reconciles |
| `solution/options_test.go` | `resolveOptions`/`resolveQueueOptions` defaults and overrides; `New(clock)` with zero opts matches documented defaults |
| `solution/priority_test.go` | FR-2–FR-5: mixed-priority ordering with no aging, FIFO-preservation regression for equal priority, the deterministic aging/starvation bound, and `effectivePriority` unit cases |
| `solution/dedup_test.go` | FR-9–FR-11: max-attempts exhaustion via expiry and via `Fail` → `Abandoned`; unlimited (default) never abandons; `Ack`/`Fail` on an abandoned job return the existing lease error; `releaseJob` decision table |
| `solution/fairness_test.go` | FR-3/FR-5/FR-6 together: sustained High flood + Low jobs + capped jobs never acked, across 4 simulated workers — Low jobs leased within a bounded number of rounds, lease counts stay fair across workers, capped jobs abandon at exactly `maxAttempts` |

## Commands run (final pass)

- `gofmt -l .` — clean (no output).
- `go vet ./...` — clean (no output).
- `go build ./...` — OK.
- `go test ./...` — `ok yodoq/solution` (all unit/integration tests pass).
- `go test -race ./...` — `ok yodoq/solution`, no data races reported.
- `go run .` — Level 1, 2, 3 all pass: zero jobs lost at every rate (0% through 50% crash + ack-drop), duplication bounded the same way as before this change (identical numbers to the pre-change baseline at every tested rate, confirming NFR-3's regression requirement).

## Known limitations / follow-ups

- `Lease` remains an O(n) scan per call (unchanged from before this task,
  and explicitly out of scope per `architecture.md`'s "Alternatives
  considered" — that's the paused Kafka-inspired redesign's concern, not
  this one's).
- The "exactly-once-ish" guarantee bounds *leases*, not *executions* — a
  worker that successfully processes a job but whose `Ack` is dropped still
  consumes an attempt on the next (unnecessary) re-lease. This is
  documented in `solution/dedup.go` and is the correct, honest
  interpretation given what the queue can observe (see spec FR-8).
- Retry fairness (FR-6) has no dedicated penalty/backoff scheduler, by
  design — it is bounded by existing lease-TTL pacing plus the opt-in
  max-attempts cap, and exercised in `solution/fairness_test.go`.
- No new third-party dependencies were introduced; stdlib only.
