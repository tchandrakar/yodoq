package harness

import (
	"fmt"
	mrand "math/rand"
)

// ScenarioSpec parameterizes one harness run (used by both the fixed
// levels in main.go and, later, the playground's "replay this" feature).
type ScenarioSpec struct {
	Name             string
	Seed             int64
	JobCount         int
	WorkerCount      int
	LeaseMS          int64
	ProcessingTimeMS int64
	CrashRate        float64
	AckDropRate      float64
}

// RunScenario builds jobs/workers from spec, drives them against q using
// clock (the same *TestClock instance q was constructed with, so that lease
// -expiry comparisons line up), drains the queue, and returns the resulting
// ledger plus the enqueued job IDs. This is level2/level3's inner logic
// (enqueue N jobs, build M workers, drain, report), generalized so it isn't
// duplicated between main.go and cmd/harness-replay.
func RunScenario(q Queue, clock *TestClock, spec ScenarioSpec) (*ExecutionLedger, []string) {
	ledger := NewLedger()
	fails := &FailureSimulator{
		CrashRate:        spec.CrashRate,
		AckDropRate:      spec.AckDropRate,
		ProcessingTimeMS: spec.ProcessingTimeMS,
		Rng:              mrand.New(mrand.NewSource(spec.Seed)),
	}

	jobIDs := make([]string, 0, spec.JobCount)
	for i := 0; i < spec.JobCount; i++ {
		id, err := q.Enqueue([]byte(fmt.Sprintf("job-%d", i)))
		if err != nil {
			continue
		}
		jobIDs = append(jobIDs, id)
	}
	if len(jobIDs) == 0 {
		return ledger, jobIDs
	}

	workers := make([]*Worker, spec.WorkerCount)
	for i := range workers {
		workers[i] = &Worker{
			ID:       fmt.Sprintf("worker-%d", i),
			Queue:    q,
			Clock:    clock,
			Ledger:   ledger,
			Failures: fails,
			LeaseMS:  spec.LeaseMS,
		}
	}

	RunUntilDrained(q, clock, workers)
	return ledger, jobIDs
}
