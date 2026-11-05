# Repository Analysis
_Commit: (no commits yet) · Date: 2026-11-05_

## Summary
YODOQ ("Your Own Don't-Drop Queue") is a small, single-module Go exercise. It defines the interface for an in-memory, crash-tolerant job queue (`solution.JobQueue`) that must be implemented, plus a self-contained harness (`main.go`) simulating worker crashes/dropped acks and grading reliability. No HTTP/DB layer — everything runs via `go run .`. The queue implementation is currently entirely stubbed (`errors.New("not implemented")`), so all harness levels fail.

## Tech stack
| Area | Technology | Evidence |
|---|---|---|
| Language | Go 1.22 (module); built here with local Go 1.26.1 | `go.mod` |
| Module | `yodoq`, zero external dependencies | `go.mod` |
| Runtime | Single `main` package, no web framework/DB | `main.go` |
| Test strategy | Custom in-process harness, not `go test` | `main.go:400-407` |
| VCS | Git repo, `main` branch, zero commits, all files untracked | `git log`, `git status` |

## Commands
- Build: `go build ./...` — verified OK.
- Run (this is the graded test harness): `go run .` — verified; prints Level 1/2/3 pass/fail/warn output.
- Format check: `gofmt -l .` — verified; currently flags `main.go` (misaligned comments around `info/successf/errf/warn`, `main.go:138-141`).
- Vet: `go vet ./...` — verified clean, no output.
- Unit tests: none exist (`go test ./...` would report no test files); grading is solely via `go run .`.

## Structure
```
yodoq/
├── go.mod            module yodoq, go 1.22
├── main.go           test harness: Worker, FailureSimulator, ExecutionLedger, TestClock, level1/2/3, main()
├── README.md         exercise spec, architecture, success criteria
├── .gitignore        ignores /yodoq binary, *.test, *.out, .DS_Store
└── solution/
    └── queue.go      JobQueue stub — the only file implementers are expected to edit
```

## Modules
- **`main` package** (`main.go`, 408 lines): simulation/grading harness.
  - `TestClock` — manual fake clock implementing `solution.Clock` (`main.go:16-19`).
  - `ExecutionLedger` — out-of-band ground truth of actual executions, used for grading, independent of queue state (`main.go:21-55`).
  - `FailureSimulator` — RNG-driven crash/ack-drop decisions (`main.go:58-66`).
  - `Worker` — simulates lease → process → (maybe crash) → (maybe drop ack) → ack (`main.go:70-107`).
  - `runUntilDrained` — drives workers in rounds, advances fake clock 2000ms/round so expired leases recycle; caps at 200,000 iterations (`main.go:112-132`).
  - `level1`/`level2`/`level3` — the three graded scenarios (`main.go:169-398`).
  - Depends only on `yodoq/solution`.
- **`solution` package** (`solution/queue.go`, 88 lines): the implementation surface.
  - `Clock` interface — time abstraction so harness can fast-forward (`solution/queue.go:14-16`).
  - `LeasedJob`, `Stats` — return types (`solution/queue.go:6-23`).
  - `JobQueue` — struct with only a `clock` field; `Enqueue`, `Lease`, `Ack`, `Fail`, `Stats` are all unimplemented stubs (`solution/queue.go:31-88`).
  - Depends only on stdlib `errors`. No locking/concurrency primitives yet, despite the README listing concurrency as an advanced (optional) challenge.

## Key flows
1. **Entry point**: `main()` → intro banner → `level1()` → `level2()` → `level3()` sequentially (`main.go:400-407`).
2. **Level 1 (correctness, no failures)**: `Enqueue → Lease → Ack`; verifies acked jobs aren't re-leasable; batch enqueue/lease/ack of 10 jobs; `Fail` → re-lease to a different worker; stale-ack rejection after manually advancing the clock past lease expiry (`main.go:169-262`).
3. **Level 2 (crash recovery)**: 200 jobs, 5 workers, `CrashRate=0.20`, driven via `runUntilDrained`; asserts `ExecutionLedger.Lost == 0` and `Excess ≤ 2 × jobCount × crashRate` (`main.go:265-325`).
4. **Level 3 (stress sweep)**: same pattern across crash+ack-drop rates `0.1%` → `50%`, again requiring zero lost jobs (`main.go:328-398`).
5. **Core race implementers must handle** (`Worker.Tick`, `main.go:80-107`): lease a job → advance clock → with prob `CrashRate` abandon without recording/acking (lease must expire and be reclaimed) → else record execution in ledger → with prob `AckDropRate` drop the ack silently (queue must notice stale lease, expire it, re-lease, producing a bounded duplicate already counted in the ledger).

## Data & integrations
- No external services/DB/network — all state in-memory, scoped per `JobQueue` instance per level.
- Persistence / exactly-once are explicitly out of scope for the base solution (optional "Advanced challenges" in `README.md:70-79`).
- Time is injected via `Clock`; implementers must use `q.clock.Now()`, never `time.Now()` directly inside `solution/queue.go`.

## Conventions
- Go-standard doc comments (`// Name ...`) preceding every exported type/function.
- Harness ignores expected/irrelevant errors with `_ =` (e.g. `main.go:105`, `216`) but checks errors explicitly in assertions.
- Logging via small custom emoji-prefixed helpers `info/successf/errf/warn` (`main.go:138-141`), not a logging library.
- No commit history to infer style from yet (repo has zero commits).
- `gofmt -l .` currently flags `main.go` — run `gofmt -w main.go` before committing.

## Risks & hotspots
1. **Core logic entirely unimplemented** — `solution/queue.go` has no working `Enqueue/Lease/Ack/Fail/Stats`; every harness level fails with `"not implemented"` (`solution/queue.go:47-88`). This is the primary work item.
2. **No concurrency primitives** — `JobQueue` has only a `clock` field; correct pending/leased/completed state transitions and lease-expiry comparisons against `clock.Now()` are needed even for single-threaded correctness; true concurrency safety is an optional extension per the README.
3. **No unit tests for `solution`** — correctness is checked only indirectly via the probabilistic harness (deterministic only where RNG is seeded, e.g. `main.go:275`, `354`). Consider adding `solution/queue_test.go` with deterministic edge cases.
4. **Repo has zero commits** — everything is untracked (`?? .gitignore`, `?? README.md`, `?? go.mod`, `?? main.go`, `?? solution/`); no diff/history available yet.
5. **Formatting drift** — `gofmt -l .` flags `main.go`; fix before first commit.
6. **No CI** — no `.github/workflows`, `Makefile`, or Dockerfile; build/test is manual via `go run .`.

## Open questions
- Is a concurrent/multi-goroutine `JobQueue` required, or is single-threaded sufficient for grading (the harness itself calls `Worker.Tick` sequentially)? README lists "Concurrency" only under optional "Advanced challenges."
- Should real Go unit tests be added under `solution/`, or is `go run .` the sole grading mechanism for this exercise?
- Is a specific duplication-bound/dedup-window policy expected for the base solution, or only for the "Exactly-once-ish" advanced challenge?

**Paths referenced:**
- `/Users/tchandrakar/Desktop/development/yodoq/go.mod`
- `/Users/tchandrakar/Desktop/development/yodoq/main.go`
- `/Users/tchandrakar/Desktop/development/yodoq/solution/queue.go`
- `/Users/tchandrakar/Desktop/development/yodoq/README.md`
- `/Users/tchandrakar/Desktop/development/yodoq/.gitignore`
