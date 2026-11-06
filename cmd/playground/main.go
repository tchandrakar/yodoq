// Command playground is an interactive server for exploring YODOQ's
// JobQueue live: a REST API to enqueue jobs and start/stop simulated
// workers with configurable failure rates, plus a WebSocket event stream
// (the same protocol cmd/harness-replay serves) so a TUI or browser
// dashboard can watch it happen.
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"yodoq/internal/wsserver"
	"yodoq/observer"
)

func main() {
	addr := flag.String("addr", ":8080", "address to bind the HTTP/WebSocket server on")
	backlog := flag.Int("backlog", 500, "number of recent events a late-joining client replays on connect")
	flag.Parse()

	bus := observer.NewBus(*backlog)
	srv := newServer(bus)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/jobs", srv.handleEnqueue)
	mux.HandleFunc("/api/stats", srv.handleStats)
	mux.HandleFunc("/api/workers", srv.handleStartWorker)
	mux.HandleFunc("/api/workers/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/api/workers/")
		if id == "" {
			http.NotFound(w, r)
			return
		}
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
	mux.Handle("/ws", wsserver.Handler(bus))

	log.Printf("playground: listening on %s (REST: /api/jobs, /api/workers, /api/stats, /api/scenarios; WebSocket: /ws)", *addr)
	if err := http.ListenAndServe(*addr, withCORS(mux)); err != nil {
		log.Fatalf("playground: %v", err)
	}
}
