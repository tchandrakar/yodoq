# YODOQ

Welcome to **Y**our **O**wn **D**on't-**D**rop **Q**ueue.

A small Go exercise: build a reliable job queue that doesn't lose work even when workers crash and acks vanish in transit. It doesn't compete with Sidekiq or Temporal — it's your own.

## Status

The reference solution in [`solution/`](solution/) is implemented and passes all three harness levels (`go run .`): zero jobs lost at every crash/ack-drop rate from 0% to 50%, with duplication bounded roughly linearly to the failure rate. It also implements the Concurrency, Priorities, Fairness, and Exactly-once-ish advanced challenges — see "Advanced challenges" below.

It's a single-process, in-memory `JobQueue`, internally partitioned into `numShards` (16) independent shards (a sharded internal redesign — see `docs/ai/sharded-queue-redesign/`):

- Each shard owns its own `sync.Mutex`, a `map[int64]*job` keyed by a per-shard offset, one ready FIFO per priority level in use, and a `container/heap` min-heap of currently-leased jobs ordered by lease expiry — no shard ever needs another shard's lock, and no operation scans every job
- A job's external ID (`job-<shard>-<offset>`) encodes its shard, so `Ack`/`Fail` route straight to the owning shard
- Each job tracks state (`pending` / `leased` / `completed` / `abandoned`), its current lease holder, lease-expiry time, priority, enqueue time, attempt count, and a global sequence number used to break FIFO ties across shards
- `Lease` peeks every shard's best-by-*effective*-priority candidate (base priority minus wait-time-based aging, floored at `PriorityHigh`), picks the single globally best one tie-broken by enqueue order, and takes it from its shard — retrying if a concurrent `Lease` call won the same job first. This degenerates to exactly "first pending-or-lease-expired job, globally" when every job shares the same priority, which is always true for `main.go`
- `Ack`/`Fail` validate the caller still holds the current lease before mutating state; releasing a lease (via `Fail` or expiry) routes through an attempt-exhaustion check that can move a job to the terminal `abandoned` state instead of back to `pending`
- Time comes from the injected `Clock`, so the test harness can fast-forward past lease expiries without sleeping
- The whole design is proven race-free under many concurrent callers via `go test -race ./...` (`solution/concurrency_test.go`) — no code path ever holds more than one shard's lock at a time

## Getting started

You will implement a single-process, in-memory job queue with the following guarantees:

1. **Basic operations**: enqueue, lease, ack, fail
2. **Crash tolerance**: when a worker leases a job and dies, the job is eventually re-leased to someone else
3. **Bounded duplication**: under increasing crash + dropped-ack rates, the queue never *loses* work and keeps duplicate executions reasonably bounded

YODOQ ships with a test harness that runs three levels of escalating failure injection and grades how reliable your queue actually is.

## Architecture

- **`solution.JobQueue`** (`solution/queue.go`): core type and operations — `New`, `Enqueue`, `Lease`, `Ack`, `Fail`, `Stats`; routes each call to the right shard(s)
- **`shard`** (`solution/shard.go`): one partition's mutex, job table, per-priority ready FIFOs, and expiry heap; `numShards`/`parseJobID` also live here
- **`expiryHeap`** (`solution/expiry_heap.go`): `container/heap` min-heap of `*job` by `leaseExpiresAt`, used to reclaim expired leases without scanning
- **`solution.EnqueueOption` / `solution.QueueOption`** (`solution/options.go`): functional options — `WithPriority`, `WithMaxAttempts` on `Enqueue`; `WithAgingIntervalMS` on `New`. All default to today's behavior when omitted
- **Priority & aging** (`solution/priority.go`): `Priority` levels (`PriorityHigh` / `PriorityNormal` / `PriorityLow`) and the effective-priority/aging comparator `Lease` uses to pick the best eligible job across shards
- **Attempt tracking & dedup** (`solution/dedup.go`): per-job attempt counting and the Leased→Pending/Abandoned decision made whenever a lease is released (expiry or `Fail`)
- **`solution.Clock`**: abstraction over `time.Now()` so the harness can fast-forward
- **`Worker`** (`main.go`): virtual job consumer that may crash mid-process or have its acks dropped
- **`FailureSimulator`** (`main.go`): parameterizes crash and ack-drop rates
- **`ExecutionLedger`** (`main.go`): out-of-band ground truth of how many times each job *actually* ran — the harness's source of truth for grading
- **`TestHarness`** (`main.go`): three levels of tests

```mermaid
flowchart LR
    subgraph JobQueue["solution.JobQueue (thin router)"]
        direction LR
        S0["shard 0\nmutex + jobs map[int64]*job\nready FIFO per priority + expiryHeap"]
        S1["shard 1 ..."]
        SN["shard 15"]
    end

    subgraph Helpers["pure helpers, called under one shard's lock"]
        P["priority.go\neffectivePriority, betterCandidate\n(aging comparator)"]
        D["dedup.go\nrecordAttempt, releaseJob\n(attempt tracking / abandon)"]
    end

    C["Clock\n(Now)"] -.-> JobQueue

    E["Enqueue(payload, opts...)\nWithPriority / WithMaxAttempts\n(round-robin shard)"] --> S0

    W1["Worker"] -- "Lease(workerID, leaseMS)" --> JobQueue
    JobQueue -- "peek every shard's best,\npick global best, take it" --> P
    JobQueue -- "record attempt /\ncheck exhaustion" --> D
    JobQueue -- "LeasedJob{ID, Payload, LeaseExpiresAt}" --> W1
    W1 -- "Ack(workerID, jobID)\n(parseJobID -> shard)" --> JobQueue
    W1 -. "Fail(workerID, jobID)\nor crash → lease expires" .-> JobQueue

    JobQueue -- "re-lease on expiry\n(or Abandoned if maxAttempts exhausted)" --> W2["Worker (different one)"]
```

`Lease` reclaims each shard's expired leases, peeks (without popping) each shard's best-scoring ready job — by *effective* priority at `now` (base priority minus wait-time-based aging, floored at `PriorityHigh`) — picks the single globally best candidate across all shards, tie-broken by global enqueue order, and takes it from its owning shard (retrying if a concurrent `Lease` call won the race for that exact job). This is exactly today's "first eligible job, globally" behavior whenever every job shares the same priority, so a crashed worker's job still becomes re-leasable without any explicit failure signal; a job that has exhausted its `WithMaxAttempts` cap is abandoned instead of re-leased.

## Implementation Levels

### Level 1: Basic queue operations

- Implement `Enqueue`, `Lease`, `Ack`, `Fail`, `Stats`
- Acked jobs must not be re-leased
- Failed jobs must be re-leased
- Acks from a worker who no longer holds the lease (e.g. lease expired) must be rejected

### Level 2: Crash recovery

- When a worker leases a job and never acks (because it "crashed"), the queue must re-lease it after the lease expires
- Run with 20% crash rate — every job must run *at least once*
- No jobs lost forever

### Level 3: Reliability under stress

- Sweep crash + dropped-ack rates from 0.1% to 50%
- Goal: zero lost jobs, with duplicate-execution count bounded by the failure rate (not unbounded)

## Prerequisites

- Go 1.22+
- Node.js 18+ and npm (only needed for the browser dashboard, step 5 below)

## Running it, step by step

🎥 **[Watch a short demo](docs/media/playground-demo.mp4)** of the playground server and web dashboard running live — jobs enqueued, simulated workers crashing/dropping acks, lease reclaims highlighted in amber, live filtering, and zero jobs lost/abandoned at the end. Click the link, GitHub plays it inline.

1. **Run the core queue + grading harness** (no extra setup needed):
   ```bash
   go run .
   ```
   This is `main.go` running the fixed Level 1/2/3 scenarios against `solution.JobQueue` directly and printing pass/fail text — the same thing it's always done.

2. **Run the test suite**, including the concurrency/race checks:
   ```bash
   go test -race ./...
   ```

3. **Start the interactive playground server** in one terminal and leave it running:
   ```bash
   go run ./cmd/playground
   ```
   By default it listens on `:8080` and logs its endpoints: `POST /api/jobs` (enqueue), `POST /api/workers` / `DELETE /api/workers/{id}` (start/stop a simulated worker), `GET /api/stats`, `POST /api/scenarios` (run a scripted scenario), and a live event feed over WebSocket at `ws://localhost:8080/ws`.

4. **Push some jobs and workers at it** from another terminal, e.g.:
   ```bash
   curl -X POST localhost:8080/api/jobs -d '{"payload":"hello"}'
   curl -X POST localhost:8080/api/workers -d '{"id":"worker-1","crashRate":0.2,"ackDropRate":0.1}'
   curl -X POST localhost:8080/api/scenarios -d '{"name":"demo","jobCount":200,"workerCount":5,"leaseMs":1000,"processingTimeMs":100,"crashRate":0.2,"ackDropRate":0.1}'
   ```

5. **Watch it live in a browser**, in another terminal:
   ```bash
   cd web
   npm install
   npm run dev
   ```
   Open the URL Vite prints (usually `http://localhost:5173`), enter the playground server's address (`localhost:8080`), and connect — you'll see a live stats strip, an in-flight-leases table, and a filterable event feed updating as jobs move through the queue.

6. **Or watch it from the terminal instead**, pointed at the same running server:
   ```bash
   go run ./cmd/tui -addr=ws://localhost:8080/ws
   ```

7. **Replay the fixed grading scenarios live** (same behavior as `go run .`, but streamed over the same protocol as steps 5/6, so either UI can watch it):
   ```bash
   go run ./cmd/harness-replay
   ```
   It serves on `ws://localhost:8081/ws` by default — connect the web dashboard or TUI to that address instead of `:8080`.

Each piece is independent: the web dashboard and TUI are both just WebSocket clients, so either one works against either server (`cmd/playground` or `cmd/harness-replay`), and neither server nor UI is required to run `go run .` or `go test ./...`.

## Test results — what to look at

- **Jobs lost**: count of jobs whose processing function never completed. **This must be 0**. A queue that loses work is not a queue.
- **Excess executions**: count of duplicate runs (a job that ran 3 times contributes 2 excess). Should grow roughly linearly with failure rate, not exponentially.
- **Duplication rate**: total executions / job count − 1. A reasonable solution stays under ~2× the input failure rate.
- **Effective throughput**: how quickly the queue drains under failure.

## Success criteria

- Pass all Level 1 correctness tests
- Zero jobs lost at Level 2 (20% crash rate)
- Zero jobs lost across all Level 3 failure rates (up to 50%)
- Duplication remains bounded — pathological solutions can run a single job hundreds of times

## Advanced challenges

Once you have a working solution, consider:

- **Exactly-once-ish** — implemented: `WithMaxAttempts(k)` on `Enqueue` bounds how many times a job can be (re-)leased; once the `k`-th lease is given up without an intervening `Ack`, the job moves to a new terminal `Abandoned` state instead of back to `Pending`, surfaced via `Stats().Abandoned`. This bounds *leases*, not *executions* — the honest guarantee the queue can actually make. See `solution/dedup.go` and `solution/options.go`.
- **Priorities** — implemented: `WithPriority(p)` on `Enqueue` sets one of `PriorityHigh` / `PriorityNormal` / `PriorityLow`; `Lease` picks the best eligible job by *effective* priority, which ages toward `PriorityHigh` the longer a job waits (configurable via `WithAgingIntervalMS` on `New`), bounding worst-case starvation for low-priority jobs. See `solution/priority.go` and `Lease` in `solution/queue.go`.
- **Fairness** — implemented: worker fairness holds by construction (job selection in `Lease` never reads `workerID`, only queue state and `now`); retry fairness is bounded by lease-TTL pacing plus the opt-in max-attempts cap above. Both are exercised together in `solution/fairness_test.go`.
- **Concurrency** — implemented: the existing single-mutex design is proven race-free under many concurrent callers running `Enqueue`/`Lease`/`Ack`/`Fail`/`Stats` in `solution/concurrency_test.go` (`go test -race ./...`).
- **Persistence**: how would you survive a queue-process restart? What's the minimum on-disk footprint?
- **Observability** — implemented: a non-invasive `observer.Queue` wrapper publishes one event per call outcome (plus inferred lease-reclaim events) to a bus, served live over WebSocket to a terminal UI and a browser dashboard, without any change to `solution/queue.go`. See "Live observability tools" below.

## Live observability tools

A non-invasive observability layer sits beside `solution.JobQueue`, never inside it: `observer.Queue` (`observer/wrapper.go`) wraps a `*solution.JobQueue`, forwards every call unchanged, and publishes one `observer.Event` per call outcome (enqueue, lease granted, lease reclaimed after expiry, ack, fail, rejections, stats snapshots) to a bounded, non-blocking `observer.Bus` (`observer/bus.go`). Lease-reclaim is inferred purely from the wrapper's own call-sequence bookkeeping — `solution/queue.go` is untouched. Two binaries serve that event stream over one shared WebSocket protocol (`internal/wsserver`), and two independent clients consume it.

Each piece runs independently; both UI clients need one of the two servers already running.

- **`go run .`** — the original grading harness. Unaffected: same code path (now via `internal/harness`, extracted verbatim from `main.go`), same output.
- **`go run ./cmd/playground`** — interactive server. REST API (`POST /api/jobs`, `POST`/`DELETE /api/workers/{id}`, `GET /api/stats`, `POST /api/scenarios`, `POST /api/scenarios/{id}/replay`) plus a live WebSocket feed at `ws://localhost:8080/ws` (flags: `-addr`, `-backlog`).
- **`go run ./cmd/harness-replay`** — scripted replay. Runs the fixed Level 1/2/3 scenarios (identical literals/seeds to `go run .`) against a wrapped queue, serving the same protocol on `ws://localhost:8081/ws` (flags: `-addr`, `-backlog`, `-levels`, `-exit-after`).
- **`go run ./cmd/tui -addr=ws://localhost:8080/ws`** — terminal dashboard (bubbletea/bubbles/lipgloss). Connects to either server above; shows a connection-status header, the latest `stats_snapshot` counts, and a scrollable, color-coded event feed (lease reclaims in amber, rejections in red, time-to-expiry on lease rows).
- **`cd web && npm install && npm run dev`** — browser dashboard (React + TypeScript + Vite, its own `package.json`/toolchain, entirely separate from the Go module). Enter the server's `host:port` in the page (default `localhost:8080`) to connect; shows a stats strip with sparklines, an in-flight-leases table, and a filterable event feed.

New dependencies (`github.com/gorilla/websocket`, `github.com/charmbracelet/{bubbletea,bubbles,lipgloss}`) are confined to `cmd/playground`, `cmd/harness-replay`, `cmd/tui`, and `internal/wsserver` — `solution/`, `internal/harness`, and the root `main` package stay dependency-free.
