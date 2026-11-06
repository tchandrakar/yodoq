package solution

import "testing"

// priorityTestClock is a simple manually-advanced fake clock for
// deterministic priority/aging tests (mirrors main.go's TestClock, which
// solution can't import since main depends on solution, not vice versa).
type priorityTestClock struct{ now int64 }

func (c *priorityTestClock) Now() int64            { return c.now }
func (c *priorityTestClock) Advance(deltaMS int64) { c.now += deltaMS }

func TestLeasePrefersHigherPriorityWithNoAgingElapsed(t *testing.T) {
	clock := &priorityTestClock{}
	q := New(clock)

	lowID, _ := q.Enqueue([]byte("low"), WithPriority(PriorityLow))
	normalID, _ := q.Enqueue([]byte("normal"), WithPriority(PriorityNormal))
	highID, _ := q.Enqueue([]byte("high"), WithPriority(PriorityHigh))

	j1, err := q.Lease("worker-1", 5000)
	if err != nil || j1 == nil {
		t.Fatalf("Lease failed: %v", err)
	}
	if j1.ID != highID {
		t.Errorf("first lease = %s, want High job %s", j1.ID, highID)
	}

	j2, err := q.Lease("worker-1", 5000)
	if err != nil || j2 == nil {
		t.Fatalf("Lease failed: %v", err)
	}
	if j2.ID != normalID {
		t.Errorf("second lease = %s, want Normal job %s", j2.ID, normalID)
	}

	j3, err := q.Lease("worker-1", 5000)
	if err != nil || j3 == nil {
		t.Fatalf("Lease failed: %v", err)
	}
	if j3.ID != lowID {
		t.Errorf("third lease = %s, want Low job %s", j3.ID, lowID)
	}
}

func TestLeasePreservesFIFOForEqualPriority(t *testing.T) {
	clock := &priorityTestClock{}
	q := New(clock)

	const n = 10
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		id, err := q.Enqueue([]byte("payload"))
		if err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
		ids[i] = id
	}

	for i := 0; i < n; i++ {
		j, err := q.Lease("worker-1", 5000)
		if err != nil || j == nil {
			t.Fatalf("Lease %d failed: %v", i, err)
		}
		if j.ID != ids[i] {
			t.Errorf("lease order[%d] = %s, want %s (today's FIFO order must be unchanged)", i, j.ID, ids[i])
		}
	}
}

func TestLowPriorityJobIsLeasedWithinAgingBound(t *testing.T) {
	clock := &priorityTestClock{}
	const agingIntervalMS = 2000
	q := New(clock, WithAgingIntervalMS(agingIntervalMS))

	lowID, err := q.Enqueue([]byte("low"), WithPriority(PriorityLow))
	if err != nil {
		t.Fatalf("Enqueue low failed: %v", err)
	}

	// Simulate sustained High-priority load arriving faster than it's
	// drained: enqueue (and immediately re-lease/ack) a flood of High jobs
	// each round, advancing the clock by agingIntervalMS steps, and confirm
	// the Low job is leased within the documented bound of
	// 2*agingIntervalMS of wait time.
	const maxSteps = 2 // documented bound: 2 * agingIntervalMS
	for step := 0; step < maxSteps; step++ {
		for i := 0; i < 5; i++ {
			if _, err := q.Enqueue([]byte("high-flood"), WithPriority(PriorityHigh)); err != nil {
				t.Fatalf("Enqueue high failed: %v", err)
			}
		}
		// Drain all currently-ready High jobs this round so only aging (not
		// an ever-growing High backlog) determines whether Low wins.
		for {
			j, err := q.Lease("worker-1", 1000)
			if err != nil {
				t.Fatalf("Lease failed: %v", err)
			}
			if j == nil || j.ID == lowID {
				if j != nil {
					_ = q.Ack("worker-1", j.ID)
				}
				break
			}
			_ = q.Ack("worker-1", j.ID)
		}

		s := q.Stats()
		if s.Pending == 0 && s.Leased == 0 {
			// Everything (including lowID) got drained already.
			return
		}

		clock.Advance(agingIntervalMS)
	}

	// After maxSteps*agingIntervalMS of wait, lowID must now win against any
	// freshly enqueued High job (effective priority reaches PriorityHigh).
	if _, err := q.Enqueue([]byte("high-flood"), WithPriority(PriorityHigh)); err != nil {
		t.Fatalf("Enqueue high failed: %v", err)
	}
	j, err := q.Lease("worker-1", 1000)
	if err != nil {
		t.Fatalf("Lease failed: %v", err)
	}
	if j == nil {
		t.Fatalf("expected a job to be leased, got nil")
	}
	if j.ID != lowID {
		t.Errorf("after aging bound elapsed, leased %s, want the aged Low job %s", j.ID, lowID)
	}
}

func TestEffectivePriorityAging(t *testing.T) {
	cases := []struct {
		name            string
		base            Priority
		enqueuedAt      int64
		now             int64
		agingIntervalMS int64
		want            int
	}{
		{"no wait", PriorityLow, 0, 0, 2000, int(PriorityLow)},
		{"one interval", PriorityLow, 0, 2000, 2000, int(PriorityLow) - 1},
		{"two intervals reaches high", PriorityLow, 0, 4000, 2000, int(PriorityHigh)},
		{"floored at high beyond bound", PriorityLow, 0, 100000, 2000, int(PriorityHigh)},
		{"already high stays high", PriorityHigh, 0, 100000, 2000, int(PriorityHigh)},
		{"zero aging interval is a no-op", PriorityLow, 0, 100000, 0, int(PriorityLow)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := effectivePriority(c.base, c.enqueuedAt, c.now, c.agingIntervalMS)
			if got != c.want {
				t.Errorf("effectivePriority(%v, %d, %d, %d) = %d, want %d", c.base, c.enqueuedAt, c.now, c.agingIntervalMS, got, c.want)
			}
		})
	}
}
