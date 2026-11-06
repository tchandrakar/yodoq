# Implementation plan: Live observability for YODOQ's JobQueue
Branch: feat/queue-observability   Base: main

Each Go-side step must leave `go build ./...`, `go vet ./...`,
`gofmt -l .` (empty output), and `go test ./...` green before moving on.
Frontend steps (T6) must leave `npm run build` (inside `web/`) green. Do not
modify `solution/queue.go`'s behavior at any step (NFR-1). T1 is the one step
that touches `main.go`, and it must leave `go run .`'s stdout byte-identical
to today's — verify with a captured-output diff, not just eyeballing.

Parallelization: **T1 → T2 → T3 → T4 are strictly sequential** (each is the
foundation the next is built on: extraction before wrapper, wrapper before
server, server before replay-mode reuse). Once T4 lands (the WS protocol is
real and testable), **T6 (browser dashboard) and T7 (TUI) are independent of
each other and can be built in parallel by two different developer agents**,
both against the `cmd/playground` server T4 already produced. T5 (replay
binary) can also proceed in parallel with T6/T7 once T4 lands, since it only
depends on T1–T4, not on either UI. T8 (stat visualization polish) depends on
T6 and T7 both existing. T9 (docs) is last and depends on everything.

- [ ] T1 Extract `main.go`'s simulation engine into `internal/harness`
      with zero behavior change: move `TestClock`, `ExecutionLedger`,
      `FailureSimulator`, `Worker`, `runUntilDrained` verbatim into
      `internal/harness/harness.go`, generalizing `Worker.Queue`'s type
      from `*solution.JobQueue` to a new `harness.Queue` interface
      (`Enqueue`/`Lease`/`Ack`/`Fail`/`Stats`, matching `solution.JobQueue`'s
      signatures exactly — satisfied structurally, no change needed on the
      `solution` side). Add `internal/harness/scenario.go` with
      `ScenarioSpec` and `RunScenario`, built by lifting level2/level3's
      inner job-building + worker-building + `runUntilDrained` + ledger
      logic verbatim (same literals: `jobCount=200`, `workerCount=5`,
      `LeaseMS=1000`, `ProcessingTimeMS=100`, same RNG seed formulas).
      Update `main.go` to import `internal/harness`, keep
      `level1`/`level2`/`level3`/`intro`/`section`/logging helpers in
      `main.go`, with `level2`/`level3` now calling `harness.RunScenario`.
      Capture `go run .`'s full stdout *before* this change
      (`go run . > /tmp/before.txt`) and *after*
      (`go run . > /tmp/after.txt`), and diff them — must be empty. — files:
      `internal/harness/harness.go` (new), `internal/harness/scenario.go`
      (new), `internal/harness/harness_test.go` (new, at least
      `TestRunScenario_MatchesLevel2Literals`), `main.go` (changed: imports
      + level2/level3 bodies only) — tests: new harness tests, `go run .`
      output diff, `go test ./...` — covers: FR-3, FR-9, NFR-2.

- [ ] T2 Add the `observer` package's event model and bus, with no
      dependency on `solution` beyond types already public
      (`solution.Stats`, `solution.LeasedJob`, `solution.Clock`): `Kind`
      constants + `Event`/`Envelope` structs in `observer/event.go`; `Bus`
      (ring-buffer backlog, per-subscriber bounded channel, drop-newest
      policy, `Dropped` counter) in `observer/bus.go`. No wrapper yet. —
      files: `observer/event.go` (new), `observer/bus.go` (new),
      `observer/bus_test.go` (new) — tests:
      `TestBus_PublishFanOutToAllSubscribers`,
      `TestBus_DropsNewestWhenSubscriberFull`,
      `TestBus_BacklogDeliveredOnSubscribe`, `TestBus_UnsubscribeStopsDelivery`
      — covers: FR-1, FR-10, NFR-4, NFR-6.

- [ ] T3 Add `observer.Queue` (`observer/wrapper.go`), wrapping
      `*solution.JobQueue` and publishing events per the architecture's
      exact `Lease`/`Ack`/`Fail`/`Enqueue`/`Stats` logic, including the
      shadow-map lease-reclaim inference guarded by its own `sync.Mutex`.
      Write the inference tests first against a fake clock (reuse
      `internal/harness.TestClock` or a tiny local fake — either is fine
      since `observer` only needs `solution.Clock`'s one-method interface)
      so correctness doesn't depend on wall-clock timing. — files:
      `observer/wrapper.go` (new), `observer/wrapper_test.go` (new) —
      tests: `TestWrapper_FirstLeaseIsGranted`,
      `TestWrapper_ReLeaseAfterExplicitFailIsGranted`,
      `TestWrapper_ReLeaseAfterExpiryIsReclaimed`,
      `TestWrapper_AckRejectedPublishesErrorEvent`,
      `TestWrapper_StatsAlwaysPublishesSnapshot`,
      `TestWrapper_ConcurrentWorkersNoRace` (run with `go test -race ./...`)
      — covers: FR-1, FR-2, NFR-1, NFR-3.

- [ ] T4 Add `internal/wsserver` (bus-to-WebSocket handler: backlog then
      live stream, `Envelope{V:1,...}` JSON frames) and `cmd/playground`
      (REST handlers for `POST /api/jobs`, `POST /api/workers`,
      `DELETE /api/workers/{id}`, `GET /api/stats`, `POST /api/scenarios`,
      `POST /api/scenarios/{id}/replay`, `GET /ws`; simulated workers are
      `harness.Worker` goroutines ticking against a shared
      `observer.Queue`). This is the step that introduces the
      `github.com/gorilla/websocket` dependency — run `go mod tidy`
      afterward and confirm it's additive only (root `go.mod`'s existing
      zero-dep status for `solution`/`internal/harness`/root `main` is
      unaffected; only `cmd/playground` and `internal/wsserver` import it).
      — files: `internal/wsserver/wsserver.go` (new),
      `internal/wsserver/wsserver_test.go` (new), `cmd/playground/main.go`
      (new), `cmd/playground/api.go` (new),
      `cmd/playground/server_test.go` (new), `go.mod`/`go.sum` (updated) —
      tests: `TestWsserver_BacklogThenLiveStream`,
      `TestPlayground_EnqueueJobProducesEnqueuedEvent`,
      `TestPlayground_StartWorkerProducesLeaseEvents`,
      `TestPlayground_UnknownWorkerDeleteReturns404`,
      `TestPlayground_ScenarioReplayIsDeterministic` — covers: FR-4, FR-5,
      FR-8, NFR-5, NFR-6.

- [ ] T5 Add `cmd/harness-replay`, reusing `internal/harness.RunScenario`
      (same `ScenarioSpec` literals as `main.go`'s level2/level3, plus
      level1's inline sequence reproduced via direct wrapped-queue calls)
      against an `observer.Queue`, serving the same `internal/wsserver`
      handler on its own port (default `:8081`). Supports `-levels` and
      `-exit-after` flags per architecture.md. — files:
      `cmd/harness-replay/main.go` (new),
      `cmd/harness-replay/server_test.go` (new) — tests:
      `TestReplay_ThreeLevelsEmitExpectedEventCounts` (asserts at least one
      event of each `Kind` appears and `scenario_finished` fires 3 times)
      — covers: FR-3, FR-1. *(Parallelizable with T6/T7 — depends only on
      T1–T4.)*

- [ ] T6 Scaffold the browser dashboard: `npm create vite@latest web --
      --template react-ts`, add `web/.gitignore` (`node_modules`, `dist`),
      wire a WebSocket client (host/port input, default
      `localhost:8080`) that connects to `/ws` and renders the raw
      incoming `Envelope`s in a plain scrolling list first (no stats/table
      yet — prove the connection and protocol work end to end). — files:
      `web/package.json`, `web/vite.config.ts`, `web/src/App.tsx`,
      `web/src/ws.ts`, `web/.gitignore` (all new) — tests: `npm run build`
      succeeds; manual check against a running `cmd/playground` — covers:
      FR-6 (partially — raw feed only). *(Parallelizable with T7 and T5;
      depends only on T4's protocol existing and being stable.)*

- [ ] T7 Scaffold `cmd/tui` (bubbletea): dial the same `/ws` endpoint
      (`-addr` flag, default `ws://localhost:8080/ws`) with
      reconnect-with-backoff, render incoming events into a scrolling
      `bubbles/viewport` list first (no stats header yet — same
      "prove the protocol" milestone as T6). — files: `cmd/tui/main.go`
      (new), `cmd/tui/model.go` (new), `cmd/tui/client.go` (new) — tests:
      compiles (`go build ./cmd/tui`); manual smoke test against a running
      `cmd/playground` and a running `cmd/harness-replay` (both, to prove
      FR-5's "either UI watches either mode") — covers: FR-5, FR-7
      (partially — raw feed only). *(Parallelizable with T6 and T5;
      depends only on T4.)*

- [ ] T8 Polish both UIs to the full view set from architecture.md: web
      gets the stats strip (sparklines) + in-flight-leases table derived
      client-side from `lease_granted`/`lease_reclaimed`/`ack`/`fail`
      events; TUI gets a header line showing the latest `stats_snapshot`
      counts plus color-coded time-to-expiry in its event rows. Add the
      `kind`/`jobId`/`workerId` client-side filter to the web feed. —
      files: `web/src/*` (new components: `StatsStrip.tsx`,
      `LeasesTable.tsx`, `EventFeed.tsx`), `cmd/tui/model.go` (extended) —
      tests: `npm run build`; manual smoke test — covers: FR-1, FR-6, FR-7
      (completing both). *(Depends on T6 and T7 both landing.)*

- [ ] T9 Documentation pass: add a short `docs/ai/queue-observability/README.md`-style
      section (or a top-level `web/README.md` + root `README.md` addendum —
      developer's judgment on placement) covering exactly how to run each
      independent piece: (a) `go test ./...` / `go run .` — unaffected,
      unchanged instructions; (b) `go run ./cmd/playground` — interactive
      mode, REST + WS on `:8080`; (c) `go run ./cmd/harness-replay` —
      replay mode, WS on `:8081`; (d) `go run ./cmd/tui -addr=ws://localhost:8080/ws`
      — TUI client, works against either server; (e) `cd web && npm install
      && npm run dev` — browser dashboard, connects to whichever host:port
      you type in. Explicitly state the one real dependency between pieces:
      both UI clients require a server (playground or replay) already
      running; nothing else depends on anything else. Final full-repo
      check: `gofmt -l .`, `go vet ./...`, `go build ./...`,
      `go test -race ./...`, `go run .` (confirm output still matches T1's
      captured baseline), `cd web && npm run build`. — files: new doc
      file(s) as above — tests: the final full-repo check listed above —
      covers: all FRs (documentation closing the loop), NFR-2 (final
      byte-identical confirmation).
