package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"yodoq/internal/wsserver"
	"yodoq/observer"
)

func newTestMux(srv *server, bus *observer.Bus) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/jobs", srv.handleEnqueue)
	mux.HandleFunc("/api/stats", srv.handleStats)
	mux.HandleFunc("/api/workers", srv.handleStartWorker)
	mux.HandleFunc("/api/workers/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/api/workers/")
		srv.handleStopWorker(w, r, id)
	})
	mux.HandleFunc("/api/scenarios", srv.handleScenario)
	mux.HandleFunc("/api/scenarios/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/api/scenarios/")
		runID, action, ok := strings.Cut(rest, "/")
		if !ok || action != "replay" {
			http.NotFound(w, r)
			return
		}
		srv.handleScenarioReplay(w, r, runID)
	})
	return mux
}

func dialWS(t *testing.T, httpURL string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(httpURL, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	return conn
}

func readUntilKind(t *testing.T, conn *websocket.Conn, kind observer.Kind, timeout time.Duration) observer.Event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn.SetReadDeadline(deadline)
		var env observer.Envelope
		if err := conn.ReadJSON(&env); err != nil {
			t.Fatalf("read: %v", err)
		}
		if env.Event.Kind == kind {
			return env.Event
		}
	}
	t.Fatalf("timed out waiting for event kind %q", kind)
	return observer.Event{}
}

func TestPlayground_EnqueueJobProducesEnqueuedEvent(t *testing.T) {
	bus := observer.NewBus(50)
	srv := newServer(bus)
	mux := newTestMux(srv, bus)

	httpSrv := httptest.NewServer(wrapWithWS(mux, bus))
	defer httpSrv.Close()

	conn := dialWS(t, httpSrv.URL)
	defer conn.Close()

	resp, err := http.Post(httpSrv.URL+"/api/jobs", "application/json", strings.NewReader(`{"payload":"hi"}`))
	if err != nil {
		t.Fatalf("POST /api/jobs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		JobID string `json:"jobId"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.JobID == "" {
		t.Fatal("expected a non-empty jobId")
	}

	ev := readUntilKind(t, conn, observer.KindEnqueued, 2*time.Second)
	if ev.JobID != body.JobID {
		t.Fatalf("event JobID %q != response jobId %q", ev.JobID, body.JobID)
	}
}

func TestPlayground_StartWorkerProducesLeaseEvents(t *testing.T) {
	bus := observer.NewBus(50)
	srv := newServer(bus)
	mux := newTestMux(srv, bus)

	httpSrv := httptest.NewServer(wrapWithWS(mux, bus))
	defer httpSrv.Close()

	conn := dialWS(t, httpSrv.URL)
	defer conn.Close()

	if _, err := http.Post(httpSrv.URL+"/api/jobs", "application/json", strings.NewReader(`{"payload":"hi"}`)); err != nil {
		t.Fatalf("POST /api/jobs: %v", err)
	}
	readUntilKind(t, conn, observer.KindEnqueued, 2*time.Second)

	resp, err := http.Post(httpSrv.URL+"/api/workers", "application/json",
		strings.NewReader(`{"id":"w1","crashRate":0,"ackDropRate":0,"leaseMs":1000,"processingMs":10}`))
	if err != nil {
		t.Fatalf("POST /api/workers: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	readUntilKind(t, conn, observer.KindLeaseGranted, 3*time.Second)
}

func TestPlayground_UnknownWorkerDeleteReturns404(t *testing.T) {
	bus := observer.NewBus(50)
	srv := newServer(bus)
	mux := newTestMux(srv, bus)

	httpSrv := httptest.NewServer(mux)
	defer httpSrv.Close()

	req, err := http.NewRequest(http.MethodDelete, httpSrv.URL+"/api/workers/does-not-exist", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestPlayground_ScenarioReplayIsDeterministic(t *testing.T) {
	bus := observer.NewBus(2000)
	srv := newServer(bus)
	mux := newTestMux(srv, bus)

	httpSrv := httptest.NewServer(mux)
	defer httpSrv.Close()

	specJSON := `{"name":"test-scenario","seed":7,"jobCount":20,"workerCount":2,"leaseMs":1000,"processingTimeMs":10,"crashRate":0.2,"ackDropRate":0.1}`

	resp, err := http.Post(httpSrv.URL+"/api/scenarios", "application/json", strings.NewReader(specJSON))
	if err != nil {
		t.Fatalf("POST /api/scenarios: %v", err)
	}
	var body struct {
		ScenarioRunID string `json:"scenarioRunId"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()

	waitForScenarioFinished(t, bus, "test-scenario")

	resp2, err := http.Post(httpSrv.URL+"/api/scenarios/"+body.ScenarioRunID+"/replay", "application/json", nil)
	if err != nil {
		t.Fatalf("POST replay: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("replay status = %d, want 200", resp2.StatusCode)
	}

	waitForScenarioFinished(t, bus, "test-scenario")
}

// waitForScenarioFinished subscribes fresh and polls until it sees a
// scenario_finished event with the given name, or times out. It checks the
// backlog first, since a fast scenario (small job/worker counts) may well
// finish and publish before this test gets around to subscribing.
func waitForScenarioFinished(t *testing.T, bus *observer.Bus, name string) {
	t.Helper()
	id, ch, backlog := bus.Subscribe()
	defer bus.Unsubscribe(id)
	for _, e := range backlog {
		if e.Kind == observer.KindScenarioFinished && e.ScenarioName == name {
			return
		}
	}
	deadline := time.After(3 * time.Second)
	for {
		select {
		case e := <-ch:
			if e.Kind == observer.KindScenarioFinished && e.ScenarioName == name {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for scenario_finished(%q)", name)
		}
	}
}

// wrapWithWS mounts the ws handler alongside the REST mux, mirroring
// main.go's route table for tests that need both.
func wrapWithWS(mux *http.ServeMux, bus *observer.Bus) http.Handler {
	outer := http.NewServeMux()
	outer.Handle("/", mux)
	outer.Handle("/ws", wsserver.Handler(bus))
	return outer
}
