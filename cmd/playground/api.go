package main

import (
	"encoding/json"
	"fmt"
	mrand "math/rand"
	"net/http"
	"sync"
	"time"

	"yodoq/internal/harness"
	"yodoq/observer"
	"yodoq/solution"
)

// tickInterval is how often a simulated worker goroutine attempts a
// lease+process+ack cycle. Real wall-clock time, not a fast-forwarded fake
// clock, since the playground is open-ended and interactive.
const tickInterval = 50 * time.Millisecond

// newSeededRand returns a new RNG seeded from wall-clock time: the
// playground is driven by a human in real time, so unlike the fixed,
// reproducible harness levels, each simulated worker's failure sequence is
// not expected to be deterministic (determinism for playground scenarios
// is provided separately via ScenarioSpec.Seed — see handleScenario).
func newSeededRand() *mrand.Rand {
	return mrand.New(mrand.NewSource(time.Now().UnixNano()))
}

// realClock implements solution.Clock (and harness.TestClock's one-method
// shape) backed by wall-clock time, since the playground is an open-ended
// interactive session, not a fast-forwarded fixed-scenario run.
type realClock struct{ start time.Time }

func newRealClock() *realClock { return &realClock{start: time.Now()} }
func (c *realClock) Now() int64 {
	return time.Since(c.start).Milliseconds()
}

// Advance is a no-op: wall-clock time already advances on its own between
// Worker.Tick calls (via the goroutine's time.Sleep), so there is nothing
// to fast-forward. This is what makes *realClock satisfy harness.SimClock.
func (c *realClock) Advance(int64) {}

// server holds the playground's shared state: the wrapped queue, the bus,
// and the set of currently-running simulated workers.
type server struct {
	bus   *observer.Bus
	clock *realClock
	queue *observer.Queue

	mu      sync.Mutex
	workers map[string]*simWorker

	scenarioMu sync.Mutex
	scenarios  map[string]harness.ScenarioSpec
	nextRunID  int64
}

type simWorker struct {
	stop chan struct{}
}

func newServer(bus *observer.Bus) *server {
	clock := newRealClock()
	inner := solution.New(clock)
	return &server{
		bus:       bus,
		clock:     clock,
		queue:     observer.Wrap(inner, clock, bus),
		workers:   make(map[string]*simWorker),
		scenarios: make(map[string]harness.ScenarioSpec),
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func withCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// handleEnqueue implements POST /api/jobs.
func (s *server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var body struct {
		Payload string `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	id, err := s.queue.Enqueue([]byte(body.Payload))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"jobId": id})
}

// handleStats implements GET /api/stats.
func (s *server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, s.queue.Stats())
}

// workerSpec is the JSON body for POST /api/workers.
type workerSpec struct {
	ID           string  `json:"id"`
	CrashRate    float64 `json:"crashRate"`
	AckDropRate  float64 `json:"ackDropRate"`
	LeaseMS      int64   `json:"leaseMs"`
	ProcessingMS int64   `json:"processingMs"`
}

// handleStartWorker implements POST /api/workers: spawns a goroutine
// looping a harness.Worker.Tick against the shared wrapped queue, ticking
// on a real-time interval (this is wall-clock now, not a fast-forwarded
// fake clock, since the playground has no "drain and stop" semantics).
func (s *server) handleStartWorker(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var spec workerSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if spec.ID == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}
	if spec.LeaseMS <= 0 {
		spec.LeaseMS = 1000
	}
	if spec.ProcessingMS <= 0 {
		spec.ProcessingMS = 100
	}

	s.mu.Lock()
	if _, exists := s.workers[spec.ID]; exists {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, fmt.Sprintf("worker %q already running", spec.ID))
		return
	}
	sw := &simWorker{stop: make(chan struct{})}
	s.workers[spec.ID] = sw
	s.mu.Unlock()

	wk := &harness.Worker{
		ID:     spec.ID,
		Queue:  s.queue,
		Clock:  s.clock, // wall-clock-backed SimClock; Advance is a no-op (see noOpAdvanceClock)
		Ledger: harness.NewLedger(),
		Failures: &harness.FailureSimulator{
			CrashRate:        spec.CrashRate,
			AckDropRate:      spec.AckDropRate,
			ProcessingTimeMS: spec.ProcessingMS,
			Rng:              newSeededRand(),
		},
		LeaseMS: spec.LeaseMS,
	}
	go s.runWorker(wk, sw)

	writeJSON(w, http.StatusOK, map[string]string{"id": spec.ID})
}

// handleStopWorker implements DELETE /api/workers/{id}.
func (s *server) handleStopWorker(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s.mu.Lock()
	sw, ok := s.workers[id]
	if ok {
		delete(s.workers, id)
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("worker %q not found", id))
		return
	}
	close(sw.stop)
	w.WriteHeader(http.StatusNoContent)
}

// handleScenario implements POST /api/scenarios: runs a ScenarioSpec
// asynchronously against a brand-new, isolated queue (see architecture
// decision D7 — determinism requires starting from a known empty state),
// streaming its events over the same bus everything else uses, tagged with
// ScenarioName so a client can tell them apart.
func (s *server) handleScenario(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var spec harness.ScenarioSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if spec.JobCount <= 0 || spec.WorkerCount <= 0 {
		writeError(w, http.StatusBadRequest, "jobCount and workerCount must be positive")
		return
	}
	if spec.Name == "" {
		spec.Name = "scenario"
	}

	s.scenarioMu.Lock()
	s.nextRunID++
	runID := fmt.Sprintf("run-%d", s.nextRunID)
	s.scenarios[runID] = spec
	s.scenarioMu.Unlock()

	go s.runScenarioAsync(runID, spec)

	writeJSON(w, http.StatusOK, map[string]string{"scenarioRunId": runID})
}

// handleScenarioReplay implements POST /api/scenarios/{runId}/replay: looks
// up the original spec and runs it again, fresh (D7), returning a new run
// id.
func (s *server) handleScenarioReplay(w http.ResponseWriter, r *http.Request, runID string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s.scenarioMu.Lock()
	spec, ok := s.scenarios[runID]
	s.scenarioMu.Unlock()
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("scenario run %q not found", runID))
		return
	}

	s.scenarioMu.Lock()
	s.nextRunID++
	newRunID := fmt.Sprintf("run-%d", s.nextRunID)
	s.scenarios[newRunID] = spec
	s.scenarioMu.Unlock()

	go s.runScenarioAsync(newRunID, spec)

	writeJSON(w, http.StatusOK, map[string]string{"scenarioRunId": newRunID})
}

// runWorker loops Tick on a real-time interval until sw.stop is closed.
func (s *server) runWorker(wk *harness.Worker, sw *simWorker) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sw.stop:
			return
		case <-ticker.C:
			wk.Tick()
		}
	}
}

func (s *server) runScenarioAsync(runID string, spec harness.ScenarioSpec) {
	clock := &harness.TestClock{}
	inner := solution.New(clock)
	wrapped := observer.Wrap(inner, clock, s.bus)

	s.bus.Publish(observer.Event{Kind: observer.KindScenarioStarted, ScenarioName: spec.Name, SimTimeMS: clock.Now()})
	harness.RunScenario(wrapped, clock, spec)
	s.bus.Publish(observer.Event{Kind: observer.KindScenarioFinished, ScenarioName: spec.Name, SimTimeMS: clock.Now()})
}
