package solution

import (
	"math/rand"
	"testing"
)

// regressionClock implements Clock with manual advancement, mirroring
// main.go's TestClock so these tests exercise the same time semantics as
// the harness. (Named distinctly from priorityTestClock/raceClock, which
// already exist in this package from the advanced-challenges work.)
type regressionClock struct{ now int64 }

func (c *regressionClock) Now() int64            { return c.now }
func (c *regressionClock) Advance(deltaMS int64) { c.now += deltaMS }

// TestJobQueue_Level1Scenarios pins today's external behavior for the
// no-failure scenarios that main.go's level1() exercises: basic
// enqueue/lease/ack, a completed job not being re-leasable, batch
// enqueue/lease/ack, fail-then-release-to-another-worker, and stale-ack
// rejection after lease expiry. This is the regression safety net for the
// Kafka-inspired sharding redesign.
func TestJobQueue_Level1Scenarios(t *testing.T) {
	clock := &regressionClock{}
	q := New(clock)

	id, err := q.Enqueue([]byte("hello"))
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	lj, err := q.Lease("worker-1", 5000)
	if err != nil || lj == nil {
		t.Fatalf("Lease failed: job=%v err=%v", lj, err)
	}
	if lj.ID != id {
		t.Fatalf("leased job ID mismatch: got %s want %s", lj.ID, id)
	}
	if string(lj.Payload) != "hello" {
		t.Fatalf("leased job payload mismatch: got %q", lj.Payload)
	}

	if err := q.Ack("worker-1", lj.ID); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}

	if j, _ := q.Lease("worker-1", 5000); j != nil {
		t.Fatalf("acked job should not be re-leasable, got %+v", j)
	}

	// Batch of 10 jobs: all must be leasable and ackable.
	for i := 0; i < 10; i++ {
		if _, err := q.Enqueue([]byte("batch")); err != nil {
			t.Fatalf("batch enqueue %d failed: %v", i, err)
		}
	}
	leased := 0
	for {
		j, err := q.Lease("worker-1", 5000)
		if err != nil {
			t.Fatalf("batch lease failed: %v", err)
		}
		if j == nil {
			break
		}
		if err := q.Ack("worker-1", j.ID); err != nil {
			t.Fatalf("batch ack failed: %v", err)
		}
		leased++
	}
	if leased != 10 {
		t.Fatalf("expected to lease+ack all 10 batch jobs, got %d", leased)
	}

	// Fail re-lease: a failed job must become leasable to a different worker.
	if _, err := q.Enqueue([]byte("to-fail")); err != nil {
		t.Fatalf("enqueue to-fail failed: %v", err)
	}
	j1, err := q.Lease("worker-1", 5000)
	if err != nil || j1 == nil {
		t.Fatalf("could not lease to-fail job: job=%v err=%v", j1, err)
	}
	if err := q.Fail("worker-1", j1.ID); err != nil {
		t.Fatalf("Fail failed: %v", err)
	}
	j2, err := q.Lease("worker-2", 5000)
	if err != nil || j2 == nil || j2.ID != j1.ID {
		t.Fatalf("failed job was not re-leased correctly: j1=%+v j2=%+v err=%v", j1, j2, err)
	}
	if err := q.Ack("worker-2", j2.ID); err != nil {
		t.Fatalf("ack after fail-release failed: %v", err)
	}

	// Stale ack rejection: once a lease expires and is reassigned, the
	// original holder's ack must be rejected.
	if _, err := q.Enqueue([]byte("stale")); err != nil {
		t.Fatalf("enqueue stale failed: %v", err)
	}
	s1, err := q.Lease("worker-1", 1000)
	if err != nil || s1 == nil {
		t.Fatalf("could not lease stale job: job=%v err=%v", s1, err)
	}
	clock.Advance(2000)
	s2, err := q.Lease("worker-2", 5000)
	if err != nil || s2 == nil || s2.ID != s1.ID {
		t.Fatalf("lease did not transfer after expiry: s1=%+v s2=%+v err=%v", s1, s2, err)
	}
	if err := q.Ack("worker-1", s1.ID); err == nil {
		t.Fatalf("stale ack from expired lease holder was accepted (BAD)")
	}
	if err := q.Ack("worker-2", s2.ID); err != nil {
		t.Fatalf("ack from current lease holder failed: %v", err)
	}
}

// TestJobQueue_AckWrongWorkerRejected and TestJobQueue_AckUnknownJobRejected
// pin the existing error contract for Ack/Fail.
func TestJobQueue_AckWrongWorkerRejected(t *testing.T) {
	clock := &regressionClock{}
	q := New(clock)
	_, _ = q.Enqueue([]byte("x"))
	lj, _ := q.Lease("worker-1", 5000)
	if lj == nil {
		t.Fatal("expected a leasable job")
	}
	if err := q.Ack("worker-2", lj.ID); err == nil {
		t.Fatal("expected error when acking with the wrong worker ID")
	}
	if err := q.Fail("worker-2", lj.ID); err == nil {
		t.Fatal("expected error when failing with the wrong worker ID")
	}
}

func TestJobQueue_AckUnknownJobRejected(t *testing.T) {
	clock := &regressionClock{}
	q := New(clock)
	if err := q.Ack("worker-1", "job-does-not-exist"); err == nil {
		t.Fatal("expected error acking an unknown job ID")
	}
	if err := q.Fail("worker-1", "job-does-not-exist"); err == nil {
		t.Fatal("expected error failing an unknown job ID")
	}
}

// TestJobQueue_ReadyQueueFIFOOrder asserts that never-leased jobs are
// leased out in the order they were enqueued, globally, regardless of
// which shard each one landed in (the ready-FIFO + global-seq
// tie-break's contract).
func TestJobQueue_ReadyQueueFIFOOrder(t *testing.T) {
	clock := &regressionClock{}
	q := New(clock)

	var ids []string
	for i := 0; i < 5; i++ {
		id, err := q.Enqueue([]byte("x"))
		if err != nil {
			t.Fatalf("enqueue %d failed: %v", i, err)
		}
		ids = append(ids, id)
	}

	for i, want := range ids {
		lj, err := q.Lease("worker-1", 5000)
		if err != nil || lj == nil {
			t.Fatalf("lease %d failed: job=%v err=%v", i, lj, err)
		}
		if lj.ID != want {
			t.Fatalf("lease %d: got %s, want %s (FIFO order violated)", i, lj.ID, want)
		}
	}
}

// TestJobQueue_ReclaimsExpiredLease asserts that once a lease expires, the
// same job is handed to the next Lease call even though no ready-queue
// jobs triggered it — i.e. the per-shard expiry heap path works on its own.
func TestJobQueue_ReclaimsExpiredLease(t *testing.T) {
	clock := &regressionClock{}
	q := New(clock)

	id, err := q.Enqueue([]byte("x"))
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	lj1, err := q.Lease("worker-1", 1000)
	if err != nil || lj1 == nil || lj1.ID != id {
		t.Fatalf("initial lease failed: job=%v err=%v", lj1, err)
	}

	// Not yet expired: nothing else to lease.
	if j, _ := q.Lease("worker-2", 1000); j != nil {
		t.Fatalf("lease should be nil before expiry, got %+v", j)
	}

	clock.Advance(2000)
	lj2, err := q.Lease("worker-2", 1000)
	if err != nil || lj2 == nil || lj2.ID != id {
		t.Fatalf("expired lease was not reclaimed: job=%v err=%v", lj2, err)
	}
}

// TestJobQueue_EnqueueDistributesAcrossShards guards against a
// round-robin bug that sends everything to shard 0: enqueuing
// numShards*3 jobs must land at least one job in every shard.
func TestJobQueue_EnqueueDistributesAcrossShards(t *testing.T) {
	clock := &regressionClock{}
	q := New(clock)

	seen := make(map[int]bool, numShards)
	for i := 0; i < numShards*3; i++ {
		id, err := q.Enqueue([]byte("x"))
		if err != nil {
			t.Fatalf("enqueue %d failed: %v", i, err)
		}
		idx, _, err := parseJobID(id)
		if err != nil {
			t.Fatalf("parseJobID(%q) failed: %v", id, err)
		}
		seen[idx] = true
	}
	if len(seen) != numShards {
		t.Errorf("jobs landed in %d distinct shards, want all %d", len(seen), numShards)
	}
}

// TestJobQueue_StatsAggregatesAcrossShards enqueues/leases/acks/fails a
// mix of jobs that (by round-robin) land across different shards, and
// confirms Stats() sums every shard's counters correctly, including
// Abandoned.
func TestJobQueue_StatsAggregatesAcrossShards(t *testing.T) {
	clock := &regressionClock{}
	q := New(clock)

	const n = numShards * 2
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		id, err := q.Enqueue([]byte("x"))
		if err != nil {
			t.Fatalf("enqueue %d failed: %v", i, err)
		}
		ids[i] = id
	}

	// Complete half, leave a quarter leased, and leave the remainder
	// pending before introducing the capped job below.
	completed := 0
	leased := 0
	for i := range ids {
		switch {
		case i < n/2:
			lj, err := q.Lease("worker-1", 5000)
			if err != nil || lj == nil {
				t.Fatalf("lease for completion failed at %d: job=%v err=%v", i, lj, err)
			}
			if err := q.Ack("worker-1", lj.ID); err != nil {
				t.Fatalf("ack failed at %d: %v", i, err)
			}
			completed++
		case i < n*3/4:
			lj, err := q.Lease("worker-2", 5000)
			if err != nil || lj == nil {
				t.Fatalf("lease to leave leased failed at %d: job=%v err=%v", i, lj, err)
			}
			leased++
		default:
			// left pending
		}
	}

	// PriorityHigh guarantees this is the very next job Lease hands out
	// (base High always beats base Normal before any aging elapses),
	// regardless of how many Normal-priority "ids" jobs are still
	// pending.
	cappedID, err := q.Enqueue([]byte("capped"), WithPriority(PriorityHigh), WithMaxAttempts(1))
	if err != nil {
		t.Fatalf("enqueue capped failed: %v", err)
	}
	capLease, err := q.Lease("worker-3", 1000)
	if err != nil || capLease == nil || capLease.ID != cappedID {
		t.Fatalf("lease of capped job failed: job=%v err=%v", capLease, err)
	}
	clock.Advance(2000) // expire -> abandoned (maxAttempts=1)
	// Trigger the lazy abandon transition. If any "ids" jobs are still
	// pending, this call immediately leases and completes one of them
	// instead of returning nil — account for that too.
	trigger, err := q.Lease("worker-4", 1000)
	if err != nil {
		t.Fatalf("lease to trigger abandon failed: %v", err)
	}
	if trigger != nil {
		if err := q.Ack("worker-4", trigger.ID); err != nil {
			t.Fatalf("ack of trigger lease failed: %v", err)
		}
		completed++
	}

	s := q.Stats()
	wantPending := n - completed - leased
	if s.Completed != completed {
		t.Errorf("Stats().Completed = %d, want %d", s.Completed, completed)
	}
	if s.Leased != leased {
		t.Errorf("Stats().Leased = %d, want %d", s.Leased, leased)
	}
	if s.Pending != wantPending {
		t.Errorf("Stats().Pending = %d, want %d", s.Pending, wantPending)
	}
	if s.Abandoned != 1 {
		t.Errorf("Stats().Abandoned = %d, want 1", s.Abandoned)
	}
	total := s.Completed + s.Leased + s.Pending + s.Abandoned
	if total != n+1 {
		t.Errorf("Stats total = %d, want %d (every enqueued job accounted for)", total, n+1)
	}
}

// ---- trimmed crash/ack-drop sweep, mirroring main.go's level2/level3 ----

// ledger is a trimmed stand-in for main.go's ExecutionLedger: an
// out-of-band record of how many times each job's "work" actually ran,
// independent of what the queue believes.
type ledger struct{ counts map[string]int }

func newLedger() *ledger           { return &ledger{counts: map[string]int{}} }
func (l *ledger) record(id string) { l.counts[id]++ }
func (l *ledger) lost(ids []string) int {
	n := 0
	for _, id := range ids {
		if l.counts[id] == 0 {
			n++
		}
	}
	return n
}
func (l *ledger) excess(ids []string) int {
	n := 0
	for _, id := range ids {
		if l.counts[id] > 1 {
			n += l.counts[id] - 1
		}
	}
	return n
}

// sweepWorker mirrors main.go's Worker.Tick: lease, maybe crash before
// recording, else record, maybe drop the ack.
type sweepWorker struct {
	id        string
	q         *JobQueue
	clock     *regressionClock
	ledger    *ledger
	crashRate float64
	dropRate  float64
	leaseMS   int64
	processMS int64
	rng       *rand.Rand
}

func (w *sweepWorker) tick() bool {
	j, err := w.q.Lease(w.id, w.leaseMS)
	if err != nil || j == nil {
		return false
	}
	w.clock.Advance(w.processMS)
	if w.rng.Float64() < w.crashRate {
		return true
	}
	w.ledger.record(j.ID)
	if w.rng.Float64() < w.dropRate {
		return true
	}
	_ = w.q.Ack(w.id, j.ID)
	return true
}

func sweepRunUntilDrained(q *JobQueue, clock *regressionClock, workers []*sweepWorker) {
	const maxIterations = 200000
	for i := 0; i < maxIterations; i++ {
		anyProgress := false
		for _, w := range workers {
			if w.tick() {
				anyProgress = true
			}
		}
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

// TestJobQueue_ZeroLossUnderCrashAndAckDrop is a trimmed, table-driven
// version of main.go's Level 2/3 sweep, run as a real go test so a
// regression is caught by `go test` without eyeballing `go run .` output.
// It asserts zero lost jobs and bounded excess (duplicate) executions at
// every rate from 0.1% to 50%, against the sharded design.
func TestJobQueue_ZeroLossUnderCrashAndAckDrop(t *testing.T) {
	rates := []float64{0.001, 0.01, 0.05, 0.10, 0.20, 0.30, 0.50}
	const jobCount = 80
	const numWorkers = 5

	for _, rate := range rates {
		clock := &regressionClock{}
		q := New(clock)
		led := newLedger()

		jobIDs := make([]string, 0, jobCount)
		for i := 0; i < jobCount; i++ {
			id, err := q.Enqueue([]byte("payload"))
			if err != nil {
				t.Fatalf("rate %.3f: enqueue failed: %v", rate, err)
			}
			jobIDs = append(jobIDs, id)
		}

		workers := make([]*sweepWorker, numWorkers)
		for i := range workers {
			workers[i] = &sweepWorker{
				id:        "worker-" + string(rune('A'+i)),
				q:         q,
				clock:     clock,
				ledger:    led,
				crashRate: rate,
				dropRate:  rate,
				leaseMS:   1000,
				processMS: 100,
				rng:       rand.New(rand.NewSource(int64(rate*10000) + 1)),
			}
		}

		sweepRunUntilDrained(q, clock, workers)

		lost := led.lost(jobIDs)
		excess := led.excess(jobIDs)
		if lost != 0 {
			t.Errorf("rate %.3f%%: %d jobs lost, want 0", rate*100, lost)
		}
		if float64(excess) > float64(jobCount)*rate*2+1 {
			t.Errorf("rate %.3f%%: excess %d exceeds bound %.1f", rate*100, excess, float64(jobCount)*rate*2+1)
		}
	}
}
