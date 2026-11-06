package solution

import "testing"

func TestMaxAttemptsExhaustionBecomesAbandoned(t *testing.T) {
	clock := &priorityTestClock{}
	q := New(clock)

	const k = 3
	id, err := q.Enqueue([]byte("capped"), WithMaxAttempts(k))
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Lease and let the lease expire (simulating a crash, no Ack/Fail) k
	// times. Each of the first k-1 expirations must put the job back to
	// Pending/re-leasable; the k-th must abandon it.
	for attempt := 1; attempt <= k; attempt++ {
		j, err := q.Lease("worker-1", 1000)
		if err != nil {
			t.Fatalf("Lease (attempt %d) failed: %v", attempt, err)
		}
		if j == nil || j.ID != id {
			t.Fatalf("Lease (attempt %d) = %v, want job %s", attempt, j, id)
		}
		clock.Advance(2000) // let the lease expire without Ack/Fail
	}

	// The abandon transition happens lazily, the next time Lease's scan
	// notices the expired, attempt-exhausted job (mirroring how expiry is
	// always detected lazily in this design, never via a background timer).
	if j, err := q.Lease("worker-2", 1000); err != nil {
		t.Fatalf("Lease (trigger abandon) failed: %v", err)
	} else if j != nil {
		t.Fatalf("expected no leasable job (should be abandoned), got %v", j)
	}

	// After k expired leases, the job must be Abandoned and never leased
	// again.
	s := q.Stats()
	if s.Abandoned != 1 {
		t.Errorf("Stats().Abandoned = %d, want 1", s.Abandoned)
	}
	if s.Pending != 0 || s.Leased != 0 {
		t.Errorf("expected no Pending/Leased jobs after abandonment, got %+v", s)
	}

	j, err := q.Lease("worker-2", 1000)
	if err != nil {
		t.Fatalf("Lease after abandonment failed: %v", err)
	}
	if j != nil {
		t.Errorf("abandoned job must never be leased again, got %v", j)
	}
}

func TestMaxAttemptsExhaustionViaFailBecomesAbandoned(t *testing.T) {
	clock := &priorityTestClock{}
	q := New(clock)

	const k = 2
	id, err := q.Enqueue([]byte("capped"), WithMaxAttempts(k))
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	for attempt := 1; attempt <= k; attempt++ {
		j, err := q.Lease("worker-1", 1000)
		if err != nil || j == nil || j.ID != id {
			t.Fatalf("Lease (attempt %d) failed: j=%v err=%v", attempt, j, err)
		}
		if err := q.Fail("worker-1", j.ID); err != nil {
			t.Fatalf("Fail (attempt %d) failed: %v", attempt, err)
		}
	}

	s := q.Stats()
	if s.Abandoned != 1 {
		t.Errorf("Stats().Abandoned = %d, want 1", s.Abandoned)
	}
}

func TestUnlimitedAttemptsNeverAbandons(t *testing.T) {
	clock := &priorityTestClock{}
	q := New(clock)

	id, err := q.Enqueue([]byte("unlimited")) // no WithMaxAttempts: unlimited
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Expire the lease many times — far more than any reasonable attempt
	// cap — and confirm the job stays Pending/re-leasable indefinitely
	// (today's behavior, proven as a regression).
	for i := 0; i < 50; i++ {
		j, err := q.Lease("worker-1", 1000)
		if err != nil {
			t.Fatalf("Lease (iteration %d) failed: %v", i, err)
		}
		if j == nil || j.ID != id {
			t.Fatalf("Lease (iteration %d) = %v, want job %s", i, j, id)
		}
		clock.Advance(2000)
	}

	s := q.Stats()
	if s.Abandoned != 0 {
		t.Errorf("Stats().Abandoned = %d, want 0 (unlimited attempts)", s.Abandoned)
	}
}

func TestAckAndFailOnAbandonedJobReturnExistingLeaseError(t *testing.T) {
	clock := &priorityTestClock{}
	q := New(clock)

	id, err := q.Enqueue([]byte("capped"), WithMaxAttempts(1))
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	j, err := q.Lease("worker-1", 1000)
	if err != nil || j == nil || j.ID != id {
		t.Fatalf("Lease failed: j=%v err=%v", j, err)
	}
	clock.Advance(2000) // expire -> abandoned (maxAttempts=1)

	// Force the abandon transition by attempting another Lease (which scans
	// and abandons expired, exhausted jobs).
	if _, err := q.Lease("worker-2", 1000); err != nil {
		t.Fatalf("Lease failed: %v", err)
	}

	if err := q.Ack("worker-1", id); err == nil {
		t.Errorf("Ack on abandoned job should fail, got nil error")
	}
	if err := q.Fail("worker-1", id); err == nil {
		t.Errorf("Fail on abandoned job should fail, got nil error")
	}
}

func TestReleaseJobDecision(t *testing.T) {
	cases := []struct {
		name        string
		attempts    int
		maxAttempts int
		want        jobState
	}{
		{"unlimited", 100, 0, statePending},
		{"below cap", 2, 5, statePending},
		{"at cap", 5, 5, stateAbandoned},
		{"beyond cap", 6, 5, stateAbandoned},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			j := &job{attempts: c.attempts, maxAttempts: c.maxAttempts}
			got := releaseJob(j)
			if got != c.want {
				t.Errorf("releaseJob(attempts=%d, maxAttempts=%d) = %v, want %v", c.attempts, c.maxAttempts, got, c.want)
			}
		})
	}
}
