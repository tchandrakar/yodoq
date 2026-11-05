package solution

import "errors"

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

// JobQueue is what you implement.
//
// LEVELS TO IMPLEMENT:
//  1. Basic Enqueue / Lease / Ack / Fail with no failures
//  2. Worker crash recovery — when a lease expires, the job must be re-leasable
//  3. Bounded duplication under crash + dropped-ack rates of 0.1% to 50%
type JobQueue struct {
	clock Clock
	// TODO: add your fields here
}

// New constructs an empty queue using the given clock.
func New(clock Clock) *JobQueue {
	return &JobQueue{clock: clock}
}

// Enqueue stores a new job for processing and returns its unique ID.
//
// TODO:
//   - Generate an ID (any unique identifier)
//   - Store the job in your "pending" state
//   - Return the ID
func (q *JobQueue) Enqueue(payload []byte) (string, error) {
	return "", errors.New("not implemented")
}

// Lease atomically reserves the next available job for `workerID` for
// `leaseMS` milliseconds. Returns (nil, nil) when no job is available.
//
// TODO:
//   - Find an unleased pending job (or a job whose lease has expired)
//   - Mark it as leased to workerID until clock.Now() + leaseMS
//   - Return the leased job
func (q *JobQueue) Lease(workerID string, leaseMS int64) (*LeasedJob, error) {
	return nil, errors.New("not implemented")
}

// Ack marks a job as completed. Should return an error if `workerID` is not
// the current lease holder (e.g. the lease expired and was reassigned).
//
// TODO:
//   - Validate workerID owns the current lease
//   - Mark the job completed
//   - Reject stale acks
func (q *JobQueue) Ack(workerID, jobID string) error {
	return errors.New("not implemented")
}

// Fail releases the lease without completing. The job must become
// immediately re-leasable.
//
// TODO:
//   - Validate workerID owns the lease
//   - Return the job to the pending state
func (q *JobQueue) Fail(workerID, jobID string) error {
	return errors.New("not implemented")
}

// Stats returns counts of jobs by state. Used for harness progress checks.
//
// TODO: count pending / leased / completed jobs
func (q *JobQueue) Stats() Stats {
	return Stats{}
}
