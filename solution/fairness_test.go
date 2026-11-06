package solution

import (
	"fmt"
	"testing"
)

// TestFairnessUnderSustainedHighLoadAndCappedJobs is an integration-style
// test combining Priorities, worker fairness, and max-attempts in one
// scenario closer to the real harness shape (main.go's runUntilDrained),
// rather than isolated unit tests. It seeds a shared queue with a sustained
// High-priority flood, a handful of Low-priority jobs, and a couple of
// WithMaxAttempts-capped jobs that are deliberately never acked, then drives
// several simulated workers round by round.
//
// It asserts:
//   - (a) every Low-priority job is eventually leased within the documented
//     anti-starvation bound (FR-3),
//   - (b) no single workerID is starved relative to the others over many
//     rounds (FR-5),
//   - (c) the capped jobs end up Abandoned and stop being leased once
//     exhausted (FR-6, FR-9/FR-10, as a demonstration that retries can't
//     monopolize throughput beyond the documented, bounded mechanisms).
func TestFairnessUnderSustainedHighLoadAndCappedJobs(t *testing.T) {
	clock := &priorityTestClock{}
	const agingIntervalMS = 2000
	q := New(clock, WithAgingIntervalMS(agingIntervalMS))

	const numWorkers = 4
	workerIDs := make([]string, numWorkers)
	leaseCounts := make(map[string]int, numWorkers)
	for i := range workerIDs {
		workerIDs[i] = fmt.Sprintf("worker-%d", i)
	}

	const numLowJobs = 5
	lowIDs := make(map[string]bool, numLowJobs)
	lowPending := make(map[string]bool, numLowJobs)
	for i := 0; i < numLowJobs; i++ {
		id, err := q.Enqueue([]byte("low"), WithPriority(PriorityLow))
		if err != nil {
			t.Fatalf("Enqueue low job failed: %v", err)
		}
		lowIDs[id] = true
		lowPending[id] = true
	}

	const maxAttempts = 2
	const numCappedJobs = 2
	cappedIDs := make(map[string]bool, numCappedJobs)
	for i := 0; i < numCappedJobs; i++ {
		id, err := q.Enqueue([]byte("capped"), WithPriority(PriorityHigh), WithMaxAttempts(maxAttempts))
		if err != nil {
			t.Fatalf("Enqueue capped job failed: %v", err)
		}
		cappedIDs[id] = true
	}

	// Documented bound (architecture.md): a Low job's effective priority
	// reaches PriorityHigh after at most 2*agingIntervalMS of wait, plus
	// however long it takes to drain whatever was already strictly ahead of
	// it. Give this scenario generous headroom (20 rounds) while still
	// being a fixed, finite bound — not "eventually, probabilistically."
	const maxRounds = 20
	const highFloodPerRound = numWorkers // sustained: at least one per worker per round

	leasedLowAtRound := make(map[string]int, numLowJobs)

	for round := 1; round <= maxRounds; round++ {
		// Sustained High-priority load arriving faster than it drains.
		for i := 0; i < highFloodPerRound; i++ {
			if _, err := q.Enqueue([]byte("high-flood"), WithPriority(PriorityHigh)); err != nil {
				t.Fatalf("Enqueue high-flood job failed: %v", err)
			}
		}

		for _, wID := range workerIDs {
			j, err := q.Lease(wID, 1000)
			if err != nil {
				t.Fatalf("Lease failed: %v", err)
			}
			if j == nil {
				continue
			}
			leaseCounts[wID]++

			switch {
			case lowIDs[j.ID]:
				if lowPending[j.ID] {
					leasedLowAtRound[j.ID] = round
					delete(lowPending, j.ID)
				}
				if err := q.Ack(wID, j.ID); err != nil {
					t.Fatalf("Ack low job failed: %v", err)
				}
			case cappedIDs[j.ID]:
				// Deliberately never Ack/Fail — simulate a worker that
				// keeps crashing on this job, consuming attempts until it's
				// abandoned.
			default:
				if err := q.Ack(wID, j.ID); err != nil {
					t.Fatalf("Ack high-flood job failed: %v", err)
				}
			}
		}

		clock.Advance(agingIntervalMS)

		if len(lowPending) == 0 {
			break
		}
	}

	// (a) every Low-priority job was eventually leased, within the bound.
	if len(lowPending) != 0 {
		t.Errorf("%d Low-priority job(s) never leased within %d rounds: %v", len(lowPending), maxRounds, lowPending)
	}
	for id, round := range leasedLowAtRound {
		// 2*agingIntervalMS of wait corresponds to 2 rounds (each round
		// advances the clock by one agingIntervalMS); allow some extra
		// rounds for queueing behind whatever was already ahead at that
		// moment, but it must still be a small, bounded number — not
		// "eventually" over all 20 rounds.
		const roundBoundWithQueueingSlack = 6
		if round > roundBoundWithQueueingSlack {
			t.Errorf("low job %s leased at round %d, want within %d rounds", id, round, roundBoundWithQueueingSlack)
		}
	}

	// (b) no single worker is starved relative to the others.
	minLeases, maxLeases := -1, -1
	for _, wID := range workerIDs {
		c := leaseCounts[wID]
		if minLeases == -1 || c < minLeases {
			minLeases = c
		}
		if c > maxLeases {
			maxLeases = c
		}
	}
	if minLeases == 0 {
		t.Fatalf("at least one worker was never leased a job: %v", leaseCounts)
	}
	// Selection depends only on queue state and now, never on workerID
	// (FR-5), and every worker calls Lease once per round in the same
	// fixed order, so lease counts should be nearly identical.
	if maxLeases > minLeases*2 {
		t.Errorf("lease counts are not fair across workers: %v (min=%d, max=%d)", leaseCounts, minLeases, maxLeases)
	}

	// (c) the capped jobs end up Abandoned and stop consuming attempts.
	s := q.Stats()
	if s.Abandoned != numCappedJobs {
		t.Errorf("Stats().Abandoned = %d, want %d", s.Abandoned, numCappedJobs)
	}
	for id := range cappedIDs {
		if j, ok := q.jobs[id]; ok {
			if j.state != stateAbandoned {
				t.Errorf("capped job %s state = %v, want stateAbandoned", id, j.state)
			}
			if j.attempts != maxAttempts {
				t.Errorf("capped job %s attempts = %d, want exactly %d (no further attempts after abandonment)", id, j.attempts, maxAttempts)
			}
		}
	}
}
