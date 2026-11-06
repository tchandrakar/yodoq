package solution

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// raceClock is a Clock safe for concurrent use by many goroutines, since the
// concurrency test below calls Now() (indirectly, via the queue) from many
// goroutines at once. It uses a monotonically increasing counter rather than
// wall-clock time so the test stays fast and deterministic.
type raceClock struct {
	ms int64
}

func (c *raceClock) Now() int64 {
	return atomic.AddInt64(&c.ms, 1)
}

// TestConcurrentAccess exercises Enqueue/Lease/Ack/Fail/Stats from many
// goroutines concurrently against one shared *JobQueue. Run with `go test
// -race` to prove the single-mutex design (FR-7 / Concurrency challenge) has
// no data races, and assert that every enqueued job is accounted for in
// Stats() at the end (no job silently lost or double-counted).
func TestConcurrentAccess(t *testing.T) {
	q := New(&raceClock{})

	const numJobs = 500
	const numWorkers = 20

	jobIDs := make([]string, numJobs)
	var wg sync.WaitGroup

	// Enqueue concurrently.
	wg.Add(numJobs)
	for i := 0; i < numJobs; i++ {
		i := i
		go func() {
			defer wg.Done()
			id, err := q.Enqueue([]byte(fmt.Sprintf("payload-%d", i)))
			if err != nil {
				t.Errorf("Enqueue failed: %v", err)
				return
			}
			jobIDs[i] = id
		}()
	}
	wg.Wait()

	for _, id := range jobIDs {
		if id == "" {
			t.Fatalf("not all jobs were enqueued")
		}
	}

	// Many workers race to Lease/Ack/Fail/Stats concurrently until the queue
	// drains. Each worker keeps leasing until there's nothing left to do.
	// A shared, mutex-free attempt counter (via sync/atomic, keyed by job ID)
	// ensures every job is Fail'd at most once before being Ack'd, so the
	// queue is guaranteed to drain rather than bouncing a job between
	// Pending/Leased forever.
	var attemptsMu sync.Mutex
	attempts := make(map[string]int, numJobs)
	nextAttempt := func(id string) int {
		attemptsMu.Lock()
		defer attemptsMu.Unlock()
		attempts[id]++
		return attempts[id]
	}

	var wg2 sync.WaitGroup
	wg2.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		workerID := fmt.Sprintf("worker-%d", w)
		go func() {
			defer wg2.Done()
			idleStreak := 0
			for idleStreak < 50 {
				// Calling Stats concurrently is part of what we're proving
				// race-free, independent of whether a job is available.
				_ = q.Stats()

				leased, err := q.Lease(workerID, 1000)
				if err != nil {
					t.Errorf("Lease failed: %v", err)
					return
				}
				if leased == nil {
					idleStreak++
					continue
				}
				idleStreak = 0

				// Fail the job's first attempt (exercising the Fail/re-lease
				// path under the race detector), Ack every subsequent one —
				// guaranteeing the job eventually completes.
				if nextAttempt(leased.ID) == 1 {
					if err := q.Fail(workerID, leased.ID); err != nil {
						t.Errorf("Fail failed: %v", err)
					}
				} else {
					if err := q.Ack(workerID, leased.ID); err != nil {
						t.Errorf("Ack failed: %v", err)
					}
				}
			}
		}()
	}
	wg2.Wait()

	// Drain any jobs still Pending/Leased after the race above (the
	// alternating Fail path means some jobs may need more than one pass).
	for i := 0; i < numJobs*4; i++ {
		leased, err := q.Lease("drain-worker", 1000)
		if err != nil {
			t.Fatalf("Lease failed during drain: %v", err)
		}
		if leased == nil {
			break
		}
		if err := q.Ack("drain-worker", leased.ID); err != nil {
			t.Fatalf("Ack failed during drain: %v", err)
		}
	}

	s := q.Stats()
	if s.Pending != 0 || s.Leased != 0 {
		t.Fatalf("queue did not fully drain: %+v", s)
	}
	if s.Completed != numJobs {
		t.Fatalf("expected %d completed jobs, got %d (stats: %+v)", numJobs, s.Completed, s)
	}
}
