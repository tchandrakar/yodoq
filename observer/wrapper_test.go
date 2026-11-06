package observer

import (
	"sync"
	"testing"
	"time"

	"yodoq/solution"
)

// fakeClock is a tiny local Clock fake (observer only needs
// solution.Clock's one-method interface, so no dependency on
// internal/harness is needed here).
type fakeClock struct{ now int64 }

func (c *fakeClock) Now() int64            { return c.now }
func (c *fakeClock) Advance(deltaMS int64) { c.now += deltaMS }

func drain(t *testing.T, ch <-chan Event, n int) []Event {
	t.Helper()
	events := make([]Event, 0, n)
	for i := 0; i < n; i++ {
		select {
		case e := <-ch:
			events = append(events, e)
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for event %d/%d", i+1, n)
		}
	}
	return events
}

func TestWrapper_FirstLeaseIsGranted(t *testing.T) {
	clock := &fakeClock{}
	bus := NewBus(10)
	q := Wrap(solution.New(clock), clock, bus)
	_, ch, _ := bus.Subscribe()

	id, err := q.Enqueue([]byte("hello"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	drain(t, ch, 1) // enqueued

	lj, err := q.Lease("w1", 1000)
	if err != nil || lj == nil {
		t.Fatalf("Lease: %v, %v", lj, err)
	}
	if lj.ID != id {
		t.Fatalf("leased job ID %q, want %q", lj.ID, id)
	}

	events := drain(t, ch, 1)
	if events[0].Kind != KindLeaseGranted {
		t.Fatalf("got Kind %q, want %q", events[0].Kind, KindLeaseGranted)
	}
	if events[0].PrevWorkerID != "" {
		t.Fatalf("first lease should not have a PrevWorkerID, got %q", events[0].PrevWorkerID)
	}
}

func TestWrapper_ReLeaseAfterExplicitFailIsGranted(t *testing.T) {
	clock := &fakeClock{}
	bus := NewBus(10)
	q := Wrap(solution.New(clock), clock, bus)
	_, ch, _ := bus.Subscribe()

	_, _ = q.Enqueue([]byte("hello"))
	drain(t, ch, 1) // enqueued

	j1, _ := q.Lease("w1", 1000)
	drain(t, ch, 1) // lease_granted

	if err := q.Fail("w1", j1.ID); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	drain(t, ch, 1) // fail

	j2, err := q.Lease("w2", 1000)
	if err != nil || j2 == nil {
		t.Fatalf("re-Lease: %v, %v", j2, err)
	}
	events := drain(t, ch, 1)
	if events[0].Kind != KindLeaseGranted {
		t.Fatalf("got Kind %q after explicit Fail, want %q (not a reclaim)", events[0].Kind, KindLeaseGranted)
	}
}

func TestWrapper_ReLeaseAfterExpiryIsReclaimed(t *testing.T) {
	clock := &fakeClock{}
	bus := NewBus(10)
	q := Wrap(solution.New(clock), clock, bus)
	_, ch, _ := bus.Subscribe()

	_, _ = q.Enqueue([]byte("hello"))
	drain(t, ch, 1) // enqueued

	j1, _ := q.Lease("w1", 1000)
	drain(t, ch, 1) // lease_granted

	// Advance the clock past the lease's expiry without acking/failing —
	// simulates a crashed worker.
	clock.Advance(2000)

	j2, err := q.Lease("w2", 1000)
	if err != nil || j2 == nil {
		t.Fatalf("re-Lease after expiry: %v, %v", j2, err)
	}
	if j2.ID != j1.ID {
		t.Fatalf("expected the same job to be reclaimed, got %q want %q", j2.ID, j1.ID)
	}

	events := drain(t, ch, 1)
	if events[0].Kind != KindLeaseReclaimed {
		t.Fatalf("got Kind %q, want %q", events[0].Kind, KindLeaseReclaimed)
	}
	if events[0].PrevWorkerID != "w1" {
		t.Fatalf("PrevWorkerID = %q, want %q", events[0].PrevWorkerID, "w1")
	}
	if events[0].WorkerID != "w2" {
		t.Fatalf("WorkerID = %q, want %q", events[0].WorkerID, "w2")
	}
}

func TestWrapper_AckRejectedPublishesErrorEvent(t *testing.T) {
	clock := &fakeClock{}
	bus := NewBus(10)
	q := Wrap(solution.New(clock), clock, bus)
	_, ch, _ := bus.Subscribe()

	_, _ = q.Enqueue([]byte("hello"))
	drain(t, ch, 1) // enqueued

	j1, _ := q.Lease("w1", 1000)
	drain(t, ch, 1) // lease_granted

	// A worker that never held the lease tries to ack it.
	err := q.Ack("someone-else", j1.ID)
	if err == nil {
		t.Fatal("expected Ack from a non-lease-holder to be rejected")
	}

	events := drain(t, ch, 1)
	if events[0].Kind != KindAckRejected {
		t.Fatalf("got Kind %q, want %q", events[0].Kind, KindAckRejected)
	}
	if events[0].Err == "" {
		t.Fatal("expected Err to be populated on ack_rejected")
	}
}

func TestWrapper_StatsAlwaysPublishesSnapshot(t *testing.T) {
	clock := &fakeClock{}
	bus := NewBus(10)
	q := Wrap(solution.New(clock), clock, bus)
	_, ch, _ := bus.Subscribe()

	s := q.Stats()
	events := drain(t, ch, 1)
	if events[0].Kind != KindStatsSnapshot {
		t.Fatalf("got Kind %q, want %q", events[0].Kind, KindStatsSnapshot)
	}
	if events[0].Stats == nil {
		t.Fatal("expected Stats to be populated on stats_snapshot")
	}
	if *events[0].Stats != s {
		t.Fatalf("published Stats %+v != returned Stats %+v", *events[0].Stats, s)
	}
}

// TestWrapper_ConcurrentWorkersNoRace drives several goroutines concurrently
// through Enqueue/Lease/Ack/Fail/Stats to exercise the wrapper's own shadow
// map locking independent of solution.JobQueue's internal locking. Run with
// -race.
func TestWrapper_ConcurrentWorkersNoRace(t *testing.T) {
	clock := &fakeClock{}
	bus := NewBus(500)
	q := Wrap(solution.New(clock), clock, bus)

	const jobCount = 50
	const workerCount = 8
	for i := 0; i < jobCount; i++ {
		if _, err := q.Enqueue([]byte("payload")); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	var wg sync.WaitGroup
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		workerID := "worker-" + string(rune('A'+w))
		go func() {
			defer wg.Done()
			for i := 0; i < jobCount; i++ {
				lj, err := q.Lease(workerID, 1000)
				if err != nil {
					t.Errorf("Lease: %v", err)
					return
				}
				if lj == nil {
					continue
				}
				_ = q.Ack(workerID, lj.ID)
				_ = q.Stats()
			}
		}()
	}
	wg.Wait()
}
