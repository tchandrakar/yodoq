// Command harness-replay runs the fixed Level 1/2/3 scenarios from main.go
// (identical parameters, via internal/harness) against a wrapped,
// event-publishing queue, serving the same WebSocket protocol as
// cmd/playground so a TUI or browser dashboard can watch the replay live.
// Unlike cmd/playground, this is a scripted run, not an interactive one:
// there is no REST API, only /ws.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"yodoq/internal/harness"
	"yodoq/internal/wsserver"
	"yodoq/observer"
	"yodoq/solution"
)

func main() {
	addr := flag.String("addr", ":8081", "address to bind the HTTP/WebSocket server on")
	backlog := flag.Int("backlog", 500, "number of recent events a late-joining client replays on connect")
	levelsFlag := flag.String("levels", "1,2,3", "comma-separated list of levels to run (subset of 1,2,3)")
	exitAfter := flag.Bool("exit-after", false, "exit once all requested levels finish, instead of idling so late joiners can still read the backlog")
	flag.Parse()

	levels, err := parseLevels(*levelsFlag)
	if err != nil {
		log.Fatalf("harness-replay: %v", err)
	}

	bus := observer.NewBus(*backlog)
	mux := http.NewServeMux()
	mux.Handle("/ws", wsserver.Handler(bus))

	srv := &http.Server{Addr: *addr, Handler: mux}
	serverDone := make(chan error, 1)
	go func() { serverDone <- srv.ListenAndServe() }()
	log.Printf("harness-replay: listening on %s (WebSocket: /ws), running levels %v", *addr, levels)

	runLevels(bus, levels)

	if *exitAfter {
		log.Printf("harness-replay: all requested levels finished, exiting (-exit-after)")
		_ = srv.Close()
		return
	}

	log.Printf("harness-replay: all requested levels finished, idling so late-joining clients can still read the backlog (Ctrl-C to exit)")
	<-serverDone
}

func parseLevels(spec string) ([]int, error) {
	var levels []int
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil || n < 1 || n > 3 {
			return nil, fmt.Errorf("invalid level %q (must be 1, 2, or 3)", part)
		}
		levels = append(levels, n)
	}
	if len(levels) == 0 {
		return nil, fmt.Errorf("-levels must name at least one of 1,2,3")
	}
	return levels, nil
}

func runLevels(bus *observer.Bus, levels []int) {
	for _, lvl := range levels {
		switch lvl {
		case 1:
			runLevel1(bus)
		case 2:
			runLevel2(bus)
		case 3:
			runLevel3(bus)
		}
	}
}

// runLevel1 reproduces main.go's level1(): basic correctness, no failures,
// called directly against the wrapped queue (it tests specific
// transitions, not a bulk ScenarioSpec run).
func runLevel1(bus *observer.Bus) {
	const name = "level1"
	bus.Publish(observer.Event{Kind: observer.KindScenarioStarted, ScenarioName: name})
	defer bus.Publish(observer.Event{Kind: observer.KindScenarioFinished, ScenarioName: name})

	clock := &harness.TestClock{}
	q := observer.Wrap(solution.New(clock), clock, bus)

	id, err := q.Enqueue([]byte("hello"))
	if err != nil {
		log.Printf("harness-replay: level1: enqueue failed: %v", err)
		return
	}
	job, err := q.Lease("worker-1", 5000)
	if err != nil || job == nil {
		log.Printf("harness-replay: level1: lease failed: %v", err)
		return
	}
	_ = q.Ack("worker-1", job.ID)
	_, _ = q.Lease("worker-1", 5000) // should be nil: acked job is not re-leasable
	_ = id

	for i := 0; i < 10; i++ {
		_, _ = q.Enqueue([]byte(fmt.Sprintf("job-%d", i)))
	}
	for {
		j, _ := q.Lease("worker-1", 5000)
		if j == nil {
			break
		}
		_ = q.Ack("worker-1", j.ID)
	}

	_, _ = q.Enqueue([]byte("to-fail"))
	j1, _ := q.Lease("worker-1", 5000)
	if j1 != nil {
		_ = q.Fail("worker-1", j1.ID)
		j2, _ := q.Lease("worker-2", 5000)
		if j2 != nil {
			_ = q.Ack("worker-2", j2.ID)
		}
	}

	_, _ = q.Enqueue([]byte("stale"))
	s1, _ := q.Lease("worker-1", 1000)
	if s1 != nil {
		clock.Advance(2000)
		s2, _ := q.Lease("worker-2", 5000)
		if s2 != nil {
			_ = q.Ack("worker-1", s1.ID) // expected to be rejected (stale)
			_ = q.Ack("worker-2", s2.ID)
		}
	}
}

// runLevel2 reproduces main.go's level2(): 200 jobs, 5 workers, 20% crash
// rate, same seed (42).
func runLevel2(bus *observer.Bus) {
	const name = "level2"
	bus.Publish(observer.Event{Kind: observer.KindScenarioStarted, ScenarioName: name})
	defer bus.Publish(observer.Event{Kind: observer.KindScenarioFinished, ScenarioName: name})

	clock := &harness.TestClock{}
	q := observer.Wrap(solution.New(clock), clock, bus)
	spec := harness.ScenarioSpec{
		Name:             name,
		Seed:             42,
		JobCount:         200,
		WorkerCount:      5,
		LeaseMS:          1000,
		ProcessingTimeMS: 100,
		CrashRate:        0.20,
		AckDropRate:      0.0,
	}
	ledger, jobIDs := harness.RunScenario(q, clock, spec)
	log.Printf("harness-replay: level2: lost=%d excess=%d total=%d",
		ledger.Lost(jobIDs), ledger.Excess(jobIDs), ledger.Total())
}

// runLevel3 reproduces main.go's level3(): the crash+ack-drop rate sweep
// from 0.1% to 50%, same per-rate seed formula.
func runLevel3(bus *observer.Bus) {
	const name = "level3"
	rates := []float64{0.001, 0.01, 0.05, 0.10, 0.20, 0.30, 0.50}

	for _, rate := range rates {
		runName := fmt.Sprintf("%s-rate-%.3f", name, rate)
		bus.Publish(observer.Event{Kind: observer.KindScenarioStarted, ScenarioName: runName})

		clock := &harness.TestClock{}
		q := observer.Wrap(solution.New(clock), clock, bus)
		spec := harness.ScenarioSpec{
			Name:             runName,
			Seed:             int64(rate * 10000),
			JobCount:         200,
			WorkerCount:      5,
			LeaseMS:          1000,
			ProcessingTimeMS: 100,
			CrashRate:        rate,
			AckDropRate:      rate,
		}
		ledger, jobIDs := harness.RunScenario(q, clock, spec)
		log.Printf("harness-replay: level3 rate=%.3f: lost=%d excess=%d total=%d",
			rate, ledger.Lost(jobIDs), ledger.Excess(jobIDs), ledger.Total())

		bus.Publish(observer.Event{Kind: observer.KindScenarioFinished, ScenarioName: runName})
	}
}
