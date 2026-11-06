package observer

import (
	"sync"
	"time"

	"yodoq/solution"
)

// jobShadow is the wrapper's own, minimal shadow of a job's lease state —
// just enough to infer "this Lease call reclaimed an expired lease" purely
// from the public API's call sequence, without ever reaching into
// solution.JobQueue's internals.
type jobShadow struct {
	lastWorker string
	state      string // "leased" | "acked" | "failed"
}

// Queue wraps a *solution.JobQueue, forwarding every call unchanged and
// publishing one Event per call outcome (plus a derived "lease reclaimed"
// event) to a Bus. It never modifies solution/queue.go and never blocks the
// caller on a slow subscriber (Bus.Publish is itself non-blocking).
type Queue struct {
	inner *solution.JobQueue
	clock solution.Clock
	bus   *Bus

	mu     sync.Mutex
	shadow map[string]jobShadow
}

// Wrap constructs an observer.Queue around inner, publishing events to bus.
// clock should be the same Clock inner was constructed with, so SimTimeMS
// stamping on published events lines up with the queue's own notion of
// time.
func Wrap(inner *solution.JobQueue, clock solution.Clock, bus *Bus) *Queue {
	return &Queue{
		inner:  inner,
		clock:  clock,
		bus:    bus,
		shadow: make(map[string]jobShadow),
	}
}

// Enqueue forwards to the wrapped queue and publishes KindEnqueued on
// success.
func (q *Queue) Enqueue(payload []byte, opts ...solution.EnqueueOption) (string, error) {
	id, err := q.inner.Enqueue(payload, opts...)
	if err != nil {
		return id, err
	}
	q.bus.Publish(Event{
		Kind:        KindEnqueued,
		JobID:       id,
		PayloadSize: len(payload),
		SimTimeMS:   q.clock.Now(),
		WallTime:    time.Now(),
	})
	return id, nil
}

// Lease forwards to the wrapped queue. On success, it infers whether this
// is a fresh grant or a reclaim of an expired lease by comparing against
// its own shadow bookkeeping (see package doc / architecture.md for why
// this is sound): the only way Lease can return a job ID the wrapper still
// has marked "leased" is that nobody at the public API boundary released
// it explicitly, so the queue must have reclaimed an expired lease
// internally.
func (q *Queue) Lease(workerID string, leaseMS int64) (*solution.LeasedJob, error) {
	lj, err := q.inner.Lease(workerID, leaseMS)
	if err != nil || lj == nil {
		return lj, err // nothing to publish; no job was available (or an error)
	}

	q.mu.Lock()
	prev, seen := q.shadow[lj.ID]
	q.shadow[lj.ID] = jobShadow{lastWorker: workerID, state: "leased"}
	q.mu.Unlock()

	ev := Event{
		Kind:           KindLeaseGranted,
		JobID:          lj.ID,
		WorkerID:       workerID,
		LeaseExpiresAt: lj.LeaseExpiresAt,
		PayloadSize:    len(lj.Payload),
		SimTimeMS:      q.clock.Now(),
		WallTime:       time.Now(),
	}
	if seen && prev.state == "leased" {
		ev.Kind = KindLeaseReclaimed
		ev.PrevWorkerID = prev.lastWorker
	}
	q.bus.Publish(ev)
	return lj, nil
}

// Ack forwards to the wrapped queue. On success, it marks the job "acked"
// in the shadow map *before* publishing, so a subsequent Lease of that job
// ID (e.g. after a future Fail/expiry on a re-enqueued scenario) is
// correctly classified as a fresh grant, not a reclaim. On failure, the
// shadow is left untouched (the state didn't change) and
// KindAckRejected is published with the error.
func (q *Queue) Ack(workerID, jobID string) error {
	err := q.inner.Ack(workerID, jobID)
	if err != nil {
		q.bus.Publish(Event{
			Kind:      KindAckRejected,
			JobID:     jobID,
			WorkerID:  workerID,
			Err:       err.Error(),
			SimTimeMS: q.clock.Now(),
			WallTime:  time.Now(),
		})
		return err
	}

	q.mu.Lock()
	q.shadow[jobID] = jobShadow{lastWorker: workerID, state: "acked"}
	q.mu.Unlock()

	q.bus.Publish(Event{
		Kind:      KindAck,
		JobID:     jobID,
		WorkerID:  workerID,
		SimTimeMS: q.clock.Now(),
		WallTime:  time.Now(),
	})
	return nil
}

// Fail forwards to the wrapped queue. On success, it marks the job
// "failed" in the shadow map before publishing (same reasoning as Ack). On
// failure, KindFailRejected is published with the error and the shadow is
// left untouched.
func (q *Queue) Fail(workerID, jobID string) error {
	err := q.inner.Fail(workerID, jobID)
	if err != nil {
		q.bus.Publish(Event{
			Kind:      KindFailRejected,
			JobID:     jobID,
			WorkerID:  workerID,
			Err:       err.Error(),
			SimTimeMS: q.clock.Now(),
			WallTime:  time.Now(),
		})
		return err
	}

	q.mu.Lock()
	q.shadow[jobID] = jobShadow{lastWorker: workerID, state: "failed"}
	q.mu.Unlock()

	q.bus.Publish(Event{
		Kind:      KindFail,
		JobID:     jobID,
		WorkerID:  workerID,
		SimTimeMS: q.clock.Now(),
		WallTime:  time.Now(),
	})
	return nil
}

// Stats forwards to the wrapped queue and always publishes
// KindStatsSnapshot with the result — the natural tap point, since every
// caller (the harness's runUntilDrained, the playground's polling loop)
// already calls Stats() every round.
func (q *Queue) Stats() solution.Stats {
	s := q.inner.Stats()
	q.bus.Publish(Event{
		Kind:      KindStatsSnapshot,
		Stats:     &s,
		SimTimeMS: q.clock.Now(),
		WallTime:  time.Now(),
	})
	return s
}
