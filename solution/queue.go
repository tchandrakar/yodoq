package solution

import (
	"errors"
	"fmt"
	"sync"
)

// LeasedJob is what a worker receives when it successfully leases a job.
type LeasedJob struct {
	ID             string
	Payload        []byte
	LeaseExpiresAt int64 // milliseconds, in the same frame as Clock.Now()
}

// Clock is the queue's view of time. The test harness supplies a fake clock
// so it can fast-forward past lease expiries without sleeping.
type Clock interface {
	Now() int64 // milliseconds
}

// Stats reports the current size of each job state.
type Stats struct {
	Pending   int // never leased, or failed and waiting
	Leased    int // currently held by a worker (may have expired)
	Completed int // acked successfully
}

type jobState int

const (
	statePending jobState = iota
	stateLeased
	stateCompleted
)

type job struct {
	id             string
	payload        []byte
	state          jobState
	leaseWorker    string
	leaseExpiresAt int64
}

// JobQueue is what you implement.
//
// LEVELS TO IMPLEMENT:
//  1. Basic Enqueue / Lease / Ack / Fail with no failures
//  2. Worker crash recovery — when a lease expires, the job must be re-leasable
//  3. Bounded duplication under crash + dropped-ack rates of 0.1% to 50%
type JobQueue struct {
	clock Clock

	mu     sync.Mutex
	jobs   map[string]*job
	order  []string
	nextID int64
}

// New constructs an empty queue using the given clock.
func New(clock Clock) *JobQueue {
	return &JobQueue{
		clock: clock,
		jobs:  make(map[string]*job),
	}
}

// Enqueue stores a new job for processing and returns its unique ID.
func (q *JobQueue) Enqueue(payload []byte) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.nextID++
	id := fmt.Sprintf("job-%d", q.nextID)
	q.jobs[id] = &job{
		id:      id,
		payload: payload,
		state:   statePending,
	}
	q.order = append(q.order, id)
	return id, nil
}

// Lease atomically reserves the next available job for `workerID` for
// `leaseMS` milliseconds. Returns (nil, nil) when no job is available.
func (q *JobQueue) Lease(workerID string, leaseMS int64) (*LeasedJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.clock.Now()
	for _, id := range q.order {
		j := q.jobs[id]
		if j.state == stateCompleted {
			continue
		}
		if j.state == stateLeased && j.leaseExpiresAt > now {
			continue
		}

		j.state = stateLeased
		j.leaseWorker = workerID
		j.leaseExpiresAt = now + leaseMS
		return &LeasedJob{
			ID:             j.id,
			Payload:        j.payload,
			LeaseExpiresAt: j.leaseExpiresAt,
		}, nil
	}
	return nil, nil
}

// Ack marks a job as completed. Should return an error if `workerID` is not
// the current lease holder (e.g. the lease expired and was reassigned).
func (q *JobQueue) Ack(workerID, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	j, ok := q.jobs[jobID]
	if !ok {
		return errors.New("job not found")
	}
	if j.state != stateLeased || j.leaseWorker != workerID {
		return errors.New("worker does not hold the current lease for this job")
	}
	j.state = stateCompleted
	j.leaseWorker = ""
	return nil
}

// Fail releases the lease without completing. The job must become
// immediately re-leasable.
func (q *JobQueue) Fail(workerID, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	j, ok := q.jobs[jobID]
	if !ok {
		return errors.New("job not found")
	}
	if j.state != stateLeased || j.leaseWorker != workerID {
		return errors.New("worker does not hold the current lease for this job")
	}
	j.state = statePending
	j.leaseWorker = ""
	j.leaseExpiresAt = 0
	return nil
}

// Stats returns counts of jobs by state. Used for harness progress checks.
func (q *JobQueue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()

	var s Stats
	for _, j := range q.jobs {
		switch j.state {
		case statePending:
			s.Pending++
		case stateLeased:
			s.Leased++
		case stateCompleted:
			s.Completed++
		}
	}
	return s
}
