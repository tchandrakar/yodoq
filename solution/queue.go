package solution

import "sync"

// LeasedJob is what a worker receives when it successfully leases a job.
type LeasedJob struct {
	ID             string
	Payload        []byte
	LeaseExpiresAt int64 // milliseconds, in the same frame as Clock.Now()
}

// Clock is the queue's view of time. The test harness supplies a fake clock
// so it can fast-forward past lease expiries without sleeping. When
// JobQueue is driven by concurrent callers (see solution/concurrency_test.go),
// Now() may be called concurrently from different goroutines and must be
// safe for that — the same implicit requirement the single-mutex design
// had before sharding, just exercised more directly now that Enqueue no
// longer takes a queue-wide lock around its Now() call.
type Clock interface {
	Now() int64 // milliseconds
}

// Stats reports the current size of each job state.
type Stats struct {
	Pending   int // never leased, or failed and waiting
	Leased    int // currently held by a worker (may have expired)
	Completed int // acked successfully
	Abandoned int // exhausted its WithMaxAttempts cap; never leased again
}

type jobState int

const (
	statePending jobState = iota
	stateLeased
	stateCompleted
	stateAbandoned
)

// job's priority/enqueuedAt/attempts/maxAttempts fields (added for the
// Priorities and Exactly-once-ish advanced challenges) cost ~32 bytes
// (4 extra machine words on a 64-bit platform) per in-flight job,
// unconditionally — present whether or not WithPriority/WithMaxAttempts are
// ever used, since Go has no cheap "optional field." At 1,000,000
// concurrently in-flight jobs (far beyond this project's harness, which
// runs 200 jobs per level), that's ~32MB of extra RAM — negligible next to
// typical job payload sizes, and zero *additional* allocations (the fields
// live inline in the existing job struct already pointed to from a
// shard's jobs map).
type job struct {
	id             string // external ID, "job-<shardIdx>-<offset>"
	offset         int64  // per-shard monotonic offset; the job's key within its shard
	payload        []byte
	state          jobState
	leaseWorker    string
	leaseExpiresAt int64
	heapIndex      int // maintained by expiryHeap; -1 when not in the heap

	priority    Priority // set at Enqueue time; PriorityNormal by default
	enqueuedAt  int64    // ms, from clock.Now() at Enqueue time; used for aging
	attempts    int      // count of leases handed out so far
	maxAttempts int      // 0 = unlimited (today's behavior)
	seq         int64    // global enqueue sequence number; cross-shard FIFO tie-break
}

// JobQueue is what you implement.
//
// LEVELS TO IMPLEMENT:
//  1. Basic Enqueue / Lease / Ack / Fail with no failures
//  2. Worker crash recovery — when a lease expires, the job must be re-leasable
//  3. Bounded duplication under crash + dropped-ack rates of 0.1% to 50%
//
// Internally, JobQueue partitions the job space across numShards
// independent shards (see shard.go), each with its own mutex, one ready
// FIFO per priority level in use, and its own expiry min-heap —
// avoiding a global lock and a linear scan in Lease. A job's external ID
// encodes its shard and per-shard offset ("job-<shardIdx>-<offset>"), so
// Ack/Fail can route straight to the owning shard with no separate
// lookup structure.
//
// Lease still needs the globally best-by-effective-priority job (ties
// broken by global enqueue order), not just "the first shard that has
// anything" — otherwise sharding would silently break the
// priority/aging/fairness guarantees. So Lease reclaims each shard's
// expired leases, peeks (without popping) each shard's best candidate,
// picks the global best, and takes exactly that job from its shard,
// retrying if a concurrent Lease call won the race for the same job (see
// shard.go's peekBest/takeIfPending). This costs O(numShards) shard-local
// peeks per Lease call — independent of total job count n — in exchange
// for byte-identical selection semantics to the pre-sharding design.
type JobQueue struct {
	clock Clock
	opts  queueOptions

	shards        [numShards]*shard
	enqueueCursor uint64 // round-robins Enqueue across shards; see nextEnqueueShard
	seqCounter    int64  // global monotonic enqueue sequence, for FIFO tie-breaks
	cursorMu      sync.Mutex
}

// New constructs an empty queue using the given clock. Options are optional;
// New(clock) (no opts) keeps today's behavior exactly (aging interval
// defaults to defaultAgingIntervalMS).
func New(clock Clock, opts ...QueueOption) *JobQueue {
	q := &JobQueue{
		clock: clock,
		opts:  resolveQueueOptions(opts...),
	}
	for i := range q.shards {
		q.shards[i] = newShard(i)
	}
	return q
}

// nextEnqueueShard round-robins shard selection for Enqueue under a tiny
// dedicated mutex (cheaper and simpler than reasoning about atomic
// wraparound semantics for a counter this infrequently contended — Enqueue
// is not on Lease's hot path). There's no job key to hash on, and
// round-robin keeps shards evenly loaded.
func (q *JobQueue) nextEnqueueShard() int {
	q.cursorMu.Lock()
	defer q.cursorMu.Unlock()
	idx := int(q.enqueueCursor % uint64(numShards))
	q.enqueueCursor++
	return idx
}

func (q *JobQueue) nextSeq() int64 {
	q.cursorMu.Lock()
	defer q.cursorMu.Unlock()
	q.seqCounter++
	return q.seqCounter
}

// Enqueue stores a new job for processing and returns its unique ID.
// opts is optional; q.Enqueue(payload) (zero opts) keeps today's behavior
// exactly (PriorityNormal, unlimited attempts). Shards are chosen
// round-robin: there's no job key to hash on, and round-robin keeps
// shards evenly loaded.
func (q *JobQueue) Enqueue(payload []byte, opts ...EnqueueOption) (string, error) {
	resolved := resolveOptions(opts...)
	now := q.clock.Now()
	seq := q.nextSeq()
	idx := q.nextEnqueueShard()
	id := q.shards[idx].enqueue(payload, resolved, now, seq)
	return id, nil
}

// Lease atomically reserves the next available job for `workerID` for
// `leaseMS` milliseconds. Returns (nil, nil) when no job is available.
//
// It peeks every shard's best candidate (by effective priority, then
// global enqueue order), picks the single globally best one, and takes
// it from its owning shard — retrying the whole peek if another
// concurrent Lease call won the race for that exact job in the meantime.
// Selection depends only on queue state and now, never on workerID, so no
// caller is systematically favored.
func (q *JobQueue) Lease(workerID string, leaseMS int64) (*LeasedJob, error) {
	now := q.clock.Now()
	agingMS := q.opts.agingIntervalMS

	for {
		bestShard := -1
		var bestOffset int64
		var best candidate
		found := false
		for i, sh := range q.shards {
			offset, c, ok := sh.peekBest(now, agingMS)
			if !ok {
				continue
			}
			if !found || betterCandidate(&best, c) {
				found = true
				best = c
				bestShard = i
				bestOffset = offset
			}
		}
		if !found {
			return nil, nil
		}
		if lj, ok := q.shards[bestShard].takeIfPending(bestOffset, workerID, now, leaseMS); ok {
			return lj, nil
		}
		// Another concurrent Lease call took this exact job between our
		// peek and our take attempt; re-peek and try again.
	}
}

// Ack marks a job as completed. Should return an error if `workerID` is not
// the current lease holder (e.g. the lease expired and was reassigned).
func (q *JobQueue) Ack(workerID, jobID string) error {
	idx, offset, err := parseJobID(jobID)
	if err != nil {
		return err
	}
	return q.shards[idx].ack(workerID, offset)
}

// Fail releases the lease without completing. The job must become
// immediately re-leasable.
func (q *JobQueue) Fail(workerID, jobID string) error {
	idx, offset, err := parseJobID(jobID)
	if err != nil {
		return err
	}
	return q.shards[idx].fail(workerID, offset)
}

// Stats returns counts of jobs by state. Used for harness progress checks.
// Each shard's lock is taken and released in turn, so this never holds
// more than one shard's lock at a time.
func (q *JobQueue) Stats() Stats {
	var s Stats
	for i := range q.shards {
		p, l, c, a := q.shards[i].stats()
		s.Pending += p
		s.Leased += l
		s.Completed += c
		s.Abandoned += a
	}
	return s
}

// lookupJob returns the job behind an external ID, if any. It's an
// internal test helper (white-box tests that need to inspect a job's
// state/attempts/priority directly, now that there's no single flat
// map[string]*job to read from) — never used by Enqueue/Lease/Ack/Fail/Stats
// themselves.
func (q *JobQueue) lookupJob(id string) (*job, bool) {
	idx, offset, err := parseJobID(id)
	if err != nil {
		return nil, false
	}
	return q.shards[idx].lookup(offset)
}
