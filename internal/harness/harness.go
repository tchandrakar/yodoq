// Package harness holds the simulation engine used to grade a
// solution.JobQueue implementation: a fake clock, a failure injector, a
// simulated worker, an out-of-band execution ledger, and a drain loop. This
// is the same logic that used to live directly in main.go, extracted
// verbatim (zero behavior change) so it can be reused by cmd/harness-replay
// without duplicating it.
package harness

import (
	mrand "math/rand"

	"yodoq/solution"
)

// Queue is the subset of solution.JobQueue's API the simulation needs.
// *solution.JobQueue and *observer.Queue both satisfy it structurally, which
// is what lets cmd/harness-replay drive the exact same Worker/runUntilDrained
// code against a wrapped, event-publishing queue.
type Queue interface {
	Enqueue(payload []byte, opts ...solution.EnqueueOption) (string, error)
	Lease(workerID string, leaseMS int64) (*solution.LeasedJob, error)
	Ack(workerID, jobID string) error
	Fail(workerID, jobID string) error
	Stats() solution.Stats
}

// TestClock implements solution.Clock with manual advancement.
type TestClock struct{ now int64 }

func (c *TestClock) Now() int64            { return c.now }
func (c *TestClock) Advance(deltaMS int64) { c.now += deltaMS }

// ExecutionLedger is the harness's out-of-band ground truth: how many times
// did each job's processing function actually run? This is independent of
// what the queue thinks, and is the basis for grading.
type ExecutionLedger struct{ counts map[string]int }

func NewLedger() *ExecutionLedger              { return &ExecutionLedger{counts: map[string]int{}} }
func (l *ExecutionLedger) Record(jobID string) { l.counts[jobID]++ }

func (l *ExecutionLedger) Lost(allIDs []string) int {
	n := 0
	for _, id := range allIDs {
		if l.counts[id] == 0 {
			n++
		}
	}
	return n
}

func (l *ExecutionLedger) Excess(allIDs []string) int {
	n := 0
	for _, id := range allIDs {
		if l.counts[id] > 1 {
			n += l.counts[id] - 1
		}
	}
	return n
}

func (l *ExecutionLedger) Total() int {
	n := 0
	for _, c := range l.counts {
		n += c
	}
	return n
}

// FailureSimulator parameterizes how often workers misbehave.
type FailureSimulator struct {
	CrashRate        float64
	AckDropRate      float64
	ProcessingTimeMS int64
	Rng              *mrand.Rand
}

func (f *FailureSimulator) ShouldCrash() bool   { return f.Rng.Float64() < f.CrashRate }
func (f *FailureSimulator) ShouldDropAck() bool { return f.Rng.Float64() < f.AckDropRate }

// Worker simulates a job consumer that may crash mid-process or have its
// acks lost in transit.
type Worker struct {
	ID       string
	Queue    Queue
	Clock    *TestClock
	Ledger   *ExecutionLedger
	Failures *FailureSimulator
	LeaseMS  int64
}

// Tick attempts one lease+process+ack cycle. Returns true if a job was attempted.
func (w *Worker) Tick() bool {
	job, err := w.Queue.Lease(w.ID, w.LeaseMS)
	if err != nil || job == nil {
		return false
	}

	// Simulate processing time (the harness's clock advances; in real life this
	// would be wall-clock work).
	w.Clock.Advance(w.Failures.ProcessingTimeMS)

	if w.Failures.ShouldCrash() {
		// Worker dies before recording or acking. The lease will eventually
		// expire and the queue should re-lease the job to another worker.
		return true
	}

	// Record successful execution (ground truth — this happens regardless of
	// whether the ack reaches the queue).
	w.Ledger.Record(job.ID)

	if w.Failures.ShouldDropAck() {
		// Ack lost in flight; queue still thinks we're working on it.
		return true
	}

	_ = w.Queue.Ack(w.ID, job.ID)
	return true
}

// RunUntilDrained drives all workers in a loop, advancing the clock between
// rounds so expired leases become available again. Bails out when there's
// no progress to make or after a hard iteration cap.
func RunUntilDrained(q Queue, clock *TestClock, workers []*Worker) {
	const maxIterations = 200000
	for i := 0; i < maxIterations; i++ {
		anyProgress := false
		for _, w := range workers {
			if w.Tick() {
				anyProgress = true
			}
		}
		// Advance time past the typical lease so expired leases recycle.
		clock.Advance(2000)

		s := q.Stats()
		if s.Pending == 0 && s.Leased == 0 {
			return
		}
		if !anyProgress && s.Pending == 0 {
			return
		}
	}
}
