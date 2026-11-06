# Spec: Advanced challenges — Priorities, Fairness, Exactly-once-ish

_Status: Draft · Author: solution-architect · Date: 2026-11-06_

## 0. Scope and framing

This spec covers exactly three of the README's six "Advanced challenges":
**Priorities**, **Fairness**, **Exactly-once-ish**. It targets `main` branch's
current, simple, single-mutex `solution/queue.go` — not the paused
`feat/kafka-inspired-redesign` branch. Design must bolt onto today's
implementation and must not assume or require the sharded/paused redesign.

### Non-goals (explicit)

- **Persistence** — out of scope. Contradicts the project's in-memory,
  single-process constraint (see `docs/ai/kafka-comparison.md` §2.1, which
  already establishes that durability requires a WAL/disk layer this
  exercise deliberately doesn't have). Not designed here.
- **Observability** — out of scope. That is the paused UI/event-bus effort.
  Not designed here.
- **Concurrency** — judged already satisfied by the existing design:
  `JobQueue` guards all mutable state (`jobs`, `order`, `nextID`) behind a
  single `sync.Mutex` acquired at the top of every exported method
  (`Enqueue`, `Lease`, `Ack`, `Fail`, `Stats` — see `solution/queue.go:69-166`).
  No method reads or writes queue state outside the lock. This is already
  correct for "many parallel callers," just not optimized for throughput
  (that optimization is the paused Kafka-inspired redesign, a separate,
  later decision). The only gap is **proof**: there is no test today that
  exercises concurrent callers under Go's race detector. FR-7 below closes
  that gap without changing any production code.
- **Distributed/Kafka-like** — already covered by `docs/ai/kafka-comparison.md`;
  not revisited here.

## 1. Functional requirements

### Priorities

- **FR-1**: `Enqueue` must accept an optional priority at call time without
  breaking any existing call site. Concretely: extend the signature to
  `Enqueue(payload []byte, opts ...EnqueueOption) (string, error)` and add
  `func WithPriority(p Priority) EnqueueOption`. Existing calls
  `q.Enqueue([]byte(...))` (zero opts) must keep compiling and behaving
  exactly as today.
- **FR-2**: There are exactly three priority levels: `PriorityHigh`,
  `PriorityNormal`, `PriorityLow`. A job enqueued with no `WithPriority`
  option gets `PriorityNormal`.
- **FR-3**: `Lease` must prefer ready jobs (pending, or leased with an
  expired lease) at a higher priority over ready jobs at a lower priority,
  *except* that a low-priority job's effective priority rises the longer it
  has waited ("aging"), so that sustained high-priority load cannot starve
  it forever. The exact aging mechanism and bound are an architecture
  decision (see `architecture.md`), but the requirement is: **a pending
  low-priority job is guaranteed to be served within a bounded amount of
  wait time / bounded number of aging steps, independent of how much
  higher-priority work keeps arriving**, as long as the queue is making any
  progress at all (workers are calling `Lease`/`Ack`/`Fail`).
- **FR-4**: Among jobs of equal *effective* priority, tie-break by original
  insertion order (today's FIFO behavior), so that when every job in the
  queue shares the same priority (as is always true for `main.go`, which
  never passes a priority), `Lease`'s observable behavior is identical to
  today's.

### Fairness

- **FR-5 (worker fairness)**: No single calling worker should be
  systematically favored over another when multiple workers repeatedly call
  `Lease`. The existing scan-for-first-eligible-job approach already treats
  all callers identically (the result depends only on queue state and
  `now`, never on which worker asked); priority/aging must preserve this —
  the selection function must not take `workerID` into account when
  choosing *which* job to hand out, only when stamping the lease. No new
  per-worker bookkeeping is required; this requirement is satisfied by
  construction and must be stated/tested, not re-engineered.
- **FR-6 (retry fairness)**: A job that keeps failing or crashing and being
  re-leased must not be able to monopolize queue throughput at other jobs'
  expense beyond what is already bounded by (a) the existing lease-TTL pacing
  (a job can be re-leased at most once per lease period, which already
  rate-limits how often any single job can be retried) and (b) the new
  opt-in max-attempts bound introduced for Exactly-once-ish (FR-8..FR-11).
  No additional retry-fairness machinery (e.g. a separate penalty/backoff
  scheduler) is in scope — introducing one would be unrequested, unbounded
  complexity for a risk that is already pace-limited by lease TTL and
  count-limited by max-attempts when the caller opts in.
- **FR-7 (concurrency proof)**: Add a unit test
  (`solution/concurrency_test.go`, written in the developer stage) that runs
  many goroutines concurrently calling `Enqueue`, `Lease`, `Ack`, `Fail`, and
  `Stats` on a shared `*JobQueue`, run under `go test -race`. The test must
  pass with no data races and must assert no job is silently lost or
  double-counted during the concurrent run (every enqueued job ends up
  `Completed` or `Pending`/`Leased`, counts reconcile against `Stats`).
  This formally closes out the Concurrency challenge without any production
  code change.

### Exactly-once-ish (bounded re-lease / dedup window)

- **FR-8**: The queue cannot know whether a worker actually *executed* a
  job — only that it was *leased*. The dedup/exactly-once-ish guarantee is
  therefore defined in terms of **leases**, not executions: "a job can be
  (re-)leased at most `K` times" is the enforceable, honest version of the
  README's "run more than `K` times." This must be stated explicitly in
  `architecture.md` and in code comments so a future reader doesn't assume
  the queue can bound actual executions.
- **FR-9**: `Enqueue` must accept an optional per-job attempt cap:
  `func WithMaxAttempts(k int) EnqueueOption`. `k <= 0` (including the
  default, when the option is omitted) means **unlimited** — identical to
  today's behavior. `k >= 1` means the job may be leased at most `k` times
  total; once a lease for the `k`-th attempt expires (or is explicitly
  failed) without an intervening `Ack`, the job must stop being offered by
  `Lease` and transition to a new terminal state, `Abandoned`, instead of
  going back to `Pending`.
- **FR-10**: `Stats` must gain a new field, `Abandoned int`, counting jobs
  in that terminal state. This is additive-only: `main.go` only ever reads
  named fields off a `Stats` value returned by `q.Stats()` (`s.Pending`,
  `s.Leased` in `runUntilDrained`); it never constructs a `Stats{}` literal
  with positional fields and never iterates/serializes the struct. Adding
  `Abandoned` is therefore backward compatible and requires no change to
  `main.go`.
- **FR-11**: Because `main.go` never calls `WithMaxAttempts` (all its jobs
  are unlimited-attempts), no job can ever become `Abandoned` in levels
  1–3's existing test flows. Levels 1–3 must therefore behave identically to
  today — this must be proven by `go run .` still reporting zero lost jobs
  at every rate after the change (a regression check, not new behavior).
- **FR-12**: Document the memory cost of the attempt-tracking machinery
  (two extra `int` fields per in-flight job: `attempts`, `maxAttempts`) in
  `architecture.md`, with a concrete per-job and at-scale estimate.

## 2. Non-functional requirements

- **NFR-1 (compatibility)**: `go build ./...`, `go vet ./...`, and
  `gofmt -l .` must stay clean; `main.go` must not need any edits.
- **NFR-2 (no new dependencies)**: stdlib only.
- **NFR-3 (behavioral regression safety)**: `go run .` must still report
  zero lost jobs at Level 2 (20% crash) and across all Level 3 rates
  (0.1%–50%), with duplication bounded the same way as today, because every
  job `main.go` enqueues is default-priority (`PriorityNormal`) and
  unlimited-attempts — i.e. the new code paths for aging-comparison and
  attempt-capping are both no-ops under the exact inputs `main.go` produces
  (see FR-4 and FR-11 for why).
- **NFR-4 (code organization)**: the implementation must be split across
  multiple files inside the existing `solution` package (not one growing
  `queue.go`), each with a single clear responsibility. See
  `architecture.md` "Components" / file layout for the concrete split and
  rationale.
- **NFR-5 (determinism for tests)**: priority aging must be computed from
  the injected `Clock`, never `time.Now()`, so unit tests (including the
  starvation/aging test) can be deterministic using the existing
  `TestClock`-style fake.

## 3. Open questions resolved here

- *Is a specific duplication-bound/dedup policy expected for the base
  solution, or only for the advanced challenge?* (carried over from
  `docs/ai/analysis.md` open questions) — resolved: only for the advanced
  challenge, and only when a caller opts in via `WithMaxAttempts`; the base
  solution's unlimited-retry behavior is preserved exactly as the default.
- *How many priority levels?* Resolved: 3 (`High`/`Normal`/`Low`) — enough
  to satisfy "priority levels" (plural) from the README without the
  complexity of an open-ended int scale that would need its own
  normalization/validation rules.

## 4. How success will be judged

- **Unchanged harness levels**: `go run .` — Level 1, 2, 3 must all pass
  exactly as they do on `main` today (zero jobs lost; duplication bounded).
  This is the primary regression gate.
- **New unit tests** (written in the developer stage, enumerated here so
  the architecture can be designed to make them straightforward):
  1. A **race test** (`go test -race ./solution/...`) proving Concurrency
     (FR-7).
  2. A **priority-ordering test**: among several ready jobs at mixed
     priorities with no aging elapsed, `Lease` returns `High` before
     `Normal` before `Low`.
  3. A **starvation/aging test**: with a sustained stream of `High`-priority
     jobs arriving faster than they're drained, a single `Low`-priority job
     enqueued at `t=0` must be leased within a bounded, documented number of
     aging steps/time units — not "eventually, probabilistically," but
     deterministically bounded given the fake clock.
  4. A **FIFO-preservation test**: with all jobs at the same (default)
     priority, `Lease` order matches today's insertion-order scan exactly
     (regression proof for NFR-3/FR-4).
  5. A **max-attempts test**: a job enqueued with `WithMaxAttempts(k)` that
     is leased and left to expire `k` times becomes `Abandoned` (visible via
     `Stats().Abandoned`) and is never leased a `(k+1)`-th time; a job
     enqueued without the option behaves as unlimited under the same
     expiry pattern.
  6. A **memory note**, not a test: a short benchmark or back-of-envelope
     calculation (documented in `architecture.md`/code comments) of bytes
     added per job by the attempt-tracking fields.

## Change log

- 2026-11-06 (solution-architect): initial draft. No prior spec.md existed
  for this task slug; this file was authored directly at the design stage
  per the explicit task instructions, combining the normally-separate
  doc-writer and solution-architect spec steps into one. Scope was fixed
  to exactly Priorities/Fairness/Exactly-once-ish per the task's
  non-negotiable instruction; Persistence/Observability/Distributed were
  moved to Non-goals with reasons; Concurrency was scoped down to "add a
  race test" since the mutex-based design already satisfies it.
