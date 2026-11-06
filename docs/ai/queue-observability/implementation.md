# Implementation: Live observability for YODOQ's JobQueue

Branch: `feat/queue-observability`  Base: `main`
Spec/architecture/plan: `./architecture.md`, `./plan.md` (no separate `spec.md` existed in this repo for this task; architecture.md and plan.md were the source of truth).

## What was built

All nine plan steps (T1–T9) landed, one commit each:

- **T1** — `internal/harness` (new package): `TestClock`, `ExecutionLedger`, `FailureSimulator`, `Worker`, `RunUntilDrained` moved verbatim out of `main.go`; `Worker.Queue` generalized to a new `harness.Queue` interface. Added `ScenarioSpec`/`RunScenario` generalizing level2/level3's enqueue+worker+drain logic. `main.go` now calls into `harness.*` with identical literals. Verified `go run .`'s stdout byte-identical before/after via `diff`.
- **T2** — `observer` package: `Kind`/`Event`/`Envelope` wire types (`observer/event.go`) and `Bus` (`observer/bus.go`): bounded backlog ring buffer, per-subscriber bounded channel (256), drop-newest backpressure with a `Dropped` counter.
- **T3** — `observer.Queue` (`observer/wrapper.go`): wraps `*solution.JobQueue`, forwards every call, publishes one event per outcome. Lease-reclaim is inferred purely from the wrapper's own shadow map (never from `solution.JobQueue` internals).
- **T4** — `internal/wsserver` (backlog-then-live-stream WebSocket handler) and `cmd/playground` (REST API + simulated workers reusing `harness.Worker`/`Tick`). Introduced the `github.com/gorilla/websocket` dependency.
- **T5** — `cmd/harness-replay`: scripted replay of the fixed Level 1/2/3 scenarios against a wrapped queue, same WS protocol on `:8081`, `-levels`/`-exit-after` flags.
- **T6** — `web/` (React + TypeScript + Vite) scaffold: WS client, raw reverse-chronological event feed, host:port input.
- **T7** — `cmd/tui` (bubbletea/bubbles/lipgloss) scaffold: WS client with fixed-delay reconnect, scrollable `bubbles/viewport` event feed, connection-status header.
- **T8** — Polish: web gets `StatsStrip` (sparklines), `LeasesTable` (derived client-side from lease/ack/fail events, amber/red by time-to-expiry), and a `kind`/`jobId`/`workerId`/`scenario` filter on the feed; TUI gets `stats_snapshot` counts in its header and color-coded (`green`/`amber`/`red`) time-to-expiry on lease rows.
- **T9** — this file, plus a "Live observability tools" section in the root `README.md` describing how to run each piece (factual, describing what was built and verified, no "planned" language).

## Deviations from architecture.md, and why

1. **`harness.Worker.Clock`: `*TestClock` → `SimClock` interface.** The architecture doc specifies `Worker.Clock *TestClock` in its illustrative `internal/harness` interface listing, but also says `cmd/playground`'s simulated workers should reuse `harness.Worker`/`Tick` with "a real `solution.Clock` backed by `time.Now()`, not `harness.TestClock`." Those two statements are inconsistent as written: a field typed concretely `*TestClock` cannot hold a wall-clock implementation. Resolved by introducing a small `harness.SimClock` interface (`Now() int64`, `Advance(deltaMS int64)`) and changing `Worker.Clock`'s type to it. `*TestClock` still satisfies it (zero change to the fixed-level/replay code paths, confirmed by the T1 golden-output check continuing to pass), and `cmd/playground`'s `realClock` (wall-clock-backed, `Advance` a no-op) satisfies it too — letting the playground's simulated workers genuinely reuse `Worker.Tick` instead of a second copy of the crash/ack-drop logic, which was the actual intent of that section. `RunUntilDrained`'s `clock *TestClock` parameter was left concrete, since the fast-forwarding drain loop it implements is specific to the fixed-scenario/replay path and is never used by the playground's open-ended workers.
2. **`cmd/playground`'s simulated-worker RNG is not seeded deterministically.** The architecture doc doesn't specify this; `newSeededRand()` seeds from wall-clock time since a human-driven interactive session has no natural "same seed" semantics for ad hoc workers (determinism for playground use is provided separately, as the doc specifies, via `POST /api/scenarios`'s `ScenarioSpec.Seed`).
3. **`level2`'s "enqueue error aborts immediately" behavior became "skip and continue" (matching level3's existing pattern) once both call `harness.RunScenario`.** `solution.Enqueue` never actually returns an error in the current implementation, so this is a behavior change only in the hypothetical case Enqueue starts failing — confirmed not to affect the byte-identical `go run .` output in practice. Unifying both levels behind one `RunScenario` was what the plan asked for ("lifting level2/level3's inner logic verbatim... generalized"); this is the one place "verbatim" and "generalized" were in slight tension, resolved in favor of one shared implementation.
4. **No separate `spec.md` existed for this task.** The task's docs directory only had `architecture.md` and `plan.md`; those were treated as the complete source of truth, consistent with the instructions given for this task.

## Tests added

- `internal/harness/harness_test.go`: `TestRunScenario_MatchesLevel2Literals`, `TestGoRunOutputIsByteIdentical` (runs `go run .` as a subprocess and checks it completes and prints the expected banner — a CI-friendly companion to the manual byte-diff performed at each step).
- `observer/bus_test.go`: `TestBus_PublishFanOutToAllSubscribers`, `TestBus_DropsNewestWhenSubscriberFull`, `TestBus_BacklogDeliveredOnSubscribe`, `TestBus_UnsubscribeStopsDelivery`.
- `observer/wrapper_test.go`: `TestWrapper_FirstLeaseIsGranted`, `TestWrapper_ReLeaseAfterExplicitFailIsGranted`, `TestWrapper_ReLeaseAfterExpiryIsReclaimed`, `TestWrapper_AckRejectedPublishesErrorEvent`, `TestWrapper_StatsAlwaysPublishesSnapshot`, `TestWrapper_ConcurrentWorkersNoRace` (passes under `-race`).
- `internal/wsserver/wsserver_test.go`: `TestWsserver_BacklogThenLiveStream`.
- `cmd/playground/server_test.go`: `TestPlayground_EnqueueJobProducesEnqueuedEvent`, `TestPlayground_StartWorkerProducesLeaseEvents`, `TestPlayground_UnknownWorkerDeleteReturns404`, `TestPlayground_ScenarioReplayIsDeterministic`.
- `cmd/harness-replay/server_test.go`: `TestReplay_ThreeLevelsEmitExpectedEventCounts` (asserts every major event kind appears and `scenario_finished` fires exactly 9 times: level1 + level2 + level3's 7 rates).
- `web/`, `cmd/tui`: no automated tests (per plan, out of scope for this plan) — `npm run build` and `go build ./cmd/tui` are the acceptance bar, both green.

## Commands run (final pass) and results

- `gofmt -l .` → empty (clean) for all Go code.
- `go vet ./...` → clean.
- `go build ./...` → succeeds.
- `go test -race ./...` → all packages pass (`solution`, `internal/harness`, `observer`, `internal/wsserver`, `cmd/playground`, `cmd/harness-replay`; `cmd/tui` and root `main` have no test files).
- `go run .` → stdout byte-identical to the pre-change capture (`diff` empty), at every step from T1 onward.
- `cd web && npm run build` → succeeds (`tsc -b && vite build`).
- `go list -m all` → confirms `github.com/gorilla/websocket` and the three `charmbracelet/*` modules are the only additions to `go.mod`; `go.mod`'s Go toolchain line was bumped from `go 1.22` to `go 1.24.2` by `go mod tidy` picking up these dependencies' own minimum-version requirements — `solution/`, `internal/harness`, and the root `main` package themselves still import no third-party package.

## End-to-end verification performed manually

- Started `cmd/playground`, POSTed a job and a worker via `curl`, and read the resulting `enqueued`/`stats_snapshot`/`lease_granted`/`ack` frames with a standalone Go WebSocket test client — confirmed backlog replay on connect and live streaming thereafter.
- Started `cmd/harness-replay -exit-after` and confirmed its printed `lost`/`excess`/`total` numbers for level2/level3 match `go run .`'s numbers for the same seeds.
- Ran the actual `web/` dev server against a running `cmd/playground` in the browser tool: confirmed live connection status, backlog-then-live event delivery, a `POST /api/scenarios` run updating the stats sparklines and the event feed in real time (`Completed` reaching the job count, `lease_reclaimed` rows highlighted amber), the `kind`/`jobId`/etc. filter narrowing the feed correctly, and the in-flight-leases table correctly showing empty once a quickly-acked job's `lease_granted`/`ack` pair both arrived.
- `cmd/tui`: `go build`/`go vet` pass, and the WS dial+backlog-read mechanics (identical to what `client.go` does) were exercised against a running `cmd/harness-replay` via a standalone test client. Full interactive terminal rendering could not be screen-captured in this sandbox's non-interactive shell (no real TTY available to the agent), so the bubbletea `Update`/`View` logic was verified by code review and by the fact it shares the exact same `envelopeMsg`/event-rendering data path already covered by the `observer`/`wsserver` tests, rather than by a human-watched terminal session.

## Known limitations / follow-ups

- `cmd/tui`'s interactive rendering has no automated test and was not visually smoke-tested in this environment (see above) — a human running `go run ./cmd/tui -addr=ws://localhost:8080/ws` against a live `cmd/playground` is the recommended first manual check before relying on it.
- `cmd/playground`'s ad hoc simulated workers are not individually deterministic (by design — see Deviation 2); only `POST /api/scenarios` runs are reproducible.
- The web dashboard's in-flight-leases table and stats sparklines are entirely client-derived from the event stream; a client that misses events (per the bus's drop-newest policy) will show a stale leases table until the next `stats_snapshot` resynchronizes the stat tiles — the leases table itself has no independent resync signal today, which matches the architecture's documented Risk 2 but is worth flagging for anyone who wants a stronger guarantee later.
- No automated frontend tests were added, per the plan's explicit scope (`npm run build` is the bar).
