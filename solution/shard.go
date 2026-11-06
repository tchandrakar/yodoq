package solution

import (
	"container/heap"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// numShards is the fixed number of partitions JobQueue splits its job
// space across. It's an internal constant (not exported, not threaded
// through New): determinism across runs matters more than adapting to
// core count for a queue this size, and New(clock)'s signature can't
// change anyway.
const numShards = 16

// readyQueue is a FIFO of per-shard job offsets, popped in O(1) amortized
// via a head index rather than reslicing on every pop (mirroring the
// ready-FIFO approach from the original non-sharded redesign).
type readyQueue struct {
	items []int64
	head  int
}

func (r *readyQueue) empty() bool { return r.head >= len(r.items) }

func (r *readyQueue) front() int64 { return r.items[r.head] }

func (r *readyQueue) pushBack(offset int64) {
	r.items = append(r.items, offset)
}

func (r *readyQueue) popFront() int64 {
	offset := r.items[r.head]
	r.head++
	r.compact()
	return offset
}

// compact drops the already-consumed prefix once the queue is fully
// drained, so items doesn't grow unboundedly across a long run that
// repeatedly enqueues and leases jobs at the same priority level.
func (r *readyQueue) compact() {
	if r.head > 0 && r.head == len(r.items) {
		r.items = r.items[:0]
		r.head = 0
	}
}

// shard owns one partition of the job space: its own mutex, job table
// (keyed by per-shard offset, not by the external string ID), one ready
// FIFO per distinct Priority currently in use, and an expiry min-heap of
// currently-leased jobs. No operation ever needs to hold more than one
// shard's mutex at a time, since a job's shard is fully determined by its
// ID (Ack/Fail) or by round-robin assignment at Enqueue time.
//
// Pending jobs are bucketed by base Priority (map[Priority]*readyQueue)
// rather than kept in one FIFO, because effectivePriority ages jobs at
// different rates depending on their base priority — a single FIFO can no
// longer guarantee the next job to hand out sits at its front. Within one
// bucket it still does: aging is monotonic in wait time, so the
// longest-waiting (frontmost) job in a bucket always has the lowest
// (most urgent) effective-priority score among that bucket. See
// peekBest, which only ever needs to look at each bucket's front.
type shard struct {
	mu         sync.Mutex
	idx        int
	nextOffset int64
	jobs       map[int64]*job
	ready      map[Priority]*readyQueue
	leaseHeap  expiryHeap

	pendingN, leasedN, completedN, abandonedN int
}

func newShard(idx int) *shard {
	return &shard{
		idx:   idx,
		jobs:  make(map[int64]*job),
		ready: make(map[Priority]*readyQueue),
	}
}

func (s *shard) bucket(p Priority) *readyQueue {
	b, ok := s.ready[p]
	if !ok {
		b = &readyQueue{}
		s.ready[p] = b
	}
	return b
}

// enqueue stores a new job in this shard and returns its external ID,
// formatted as "job-<shardIdx>-<offset>" (see parseJobID for the
// inverse). seq is the job's global enqueue sequence number, used for
// cross-shard FIFO tie-breaking (see priority.go's candidate).
func (s *shard) enqueue(payload []byte, resolved enqueueOptions, now, seq int64) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset := s.nextOffset
	s.nextOffset++
	id := fmt.Sprintf("job-%d-%d", s.idx, offset)
	j := &job{
		id:          id,
		offset:      offset,
		payload:     payload,
		state:       statePending,
		heapIndex:   -1,
		priority:    resolved.priority,
		enqueuedAt:  now,
		maxAttempts: resolved.maxAttempts,
		seq:         seq,
	}
	s.jobs[offset] = j
	s.bucket(j.priority).pushBack(offset)
	s.pendingN++
	return id
}

// reclaimExpired pops every currently-expired lease off the heap and
// routes each job to either its priority bucket (still retryable) or to
// the terminal Abandoned state (maxAttempts exhausted), exactly as the
// non-sharded Lease scan did before sharding — just scoped to this
// shard's own heap instead of a global one.
func (s *shard) reclaimExpired(now int64) {
	for s.leaseHeap.Len() > 0 && s.leaseHeap[0].leaseExpiresAt <= now {
		j := heap.Pop(&s.leaseHeap).(*job)
		s.leasedN--
		if releaseJob(j) == stateAbandoned {
			j.state = stateAbandoned
			j.leaseWorker = ""
			s.abandonedN++
			continue
		}
		j.state = statePending
		j.leaseWorker = ""
		j.leaseExpiresAt = 0
		s.bucket(j.priority).pushBack(j.offset)
		s.pendingN++
	}
}

// peekBest reclaims this shard's expired leases, then reports its single
// best ready candidate (by effective priority, see priority.go) without
// removing it — the caller (JobQueue.Lease) compares peeks across every
// shard before committing to one via takeIfPending, so the globally best
// job wins regardless of which shard it landed in.
func (s *shard) peekBest(now, agingIntervalMS int64) (offset int64, c candidate, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.reclaimExpired(now)

	found := false
	var bestOffset int64
	var best candidate
	for _, b := range s.ready {
		if b.empty() {
			continue
		}
		off := b.front()
		j := s.jobs[off]
		cc := candidate{
			seq:               j.seq,
			effectivePriority: effectivePriority(j.priority, j.enqueuedAt, now, agingIntervalMS),
		}
		if !found || betterCandidate(&best, cc) {
			found = true
			best = cc
			bestOffset = off
		}
	}
	return bestOffset, best, found
}

// takeIfPending attempts to actually hand out the job at offset,
// validating it's still pending and still at the front of its priority
// bucket (i.e. nothing else took it between the caller's peekBest and
// this call). Returns ok=false on any mismatch, telling the caller to
// retry its global peek — this is the only place two concurrent Lease
// calls can race for the same job, and exactly one of them wins.
func (s *shard) takeIfPending(offset int64, workerID string, now, leaseMS int64) (*LeasedJob, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[offset]
	if !ok || j.state != statePending {
		return nil, false
	}
	b := s.ready[j.priority]
	if b == nil || b.empty() || b.front() != offset {
		return nil, false
	}
	b.popFront()
	s.pendingN--

	recordAttempt(j)
	j.state = stateLeased
	j.leaseWorker = workerID
	j.leaseExpiresAt = now + leaseMS
	heap.Push(&s.leaseHeap, j)
	s.leasedN++
	return &LeasedJob{
		ID:             j.id,
		Payload:        j.payload,
		LeaseExpiresAt: j.leaseExpiresAt,
	}, true
}

// ack marks offset as completed, validating that workerID holds its
// current lease.
func (s *shard) ack(workerID string, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[offset]
	if !ok {
		return errors.New("job not found")
	}
	if j.state != stateLeased || j.leaseWorker != workerID {
		return errors.New("worker does not hold the current lease for this job")
	}
	s.removeFromLeaseHeap(j)
	j.state = stateCompleted
	j.leaseWorker = ""
	s.leasedN--
	s.completedN++
	return nil
}

// fail releases offset's lease without completing it. Per releaseJob's
// dedup/max-attempts decision, the job becomes immediately re-leasable
// (Pending, back in its priority bucket) or, if it has exhausted
// maxAttempts, terminally Abandoned.
func (s *shard) fail(workerID string, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[offset]
	if !ok {
		return errors.New("job not found")
	}
	if j.state != stateLeased || j.leaseWorker != workerID {
		return errors.New("worker does not hold the current lease for this job")
	}
	s.removeFromLeaseHeap(j)
	s.leasedN--
	j.leaseWorker = ""
	j.leaseExpiresAt = 0
	if releaseJob(j) == stateAbandoned {
		j.state = stateAbandoned
		s.abandonedN++
		return nil
	}
	j.state = statePending
	s.bucket(j.priority).pushBack(j.offset)
	s.pendingN++
	return nil
}

// removeFromLeaseHeap drops j's live entry from the expiry heap, if it
// has one, via its tracked heapIndex. Every leased job has at most one
// entry (takeIfPending is the only place that pushes), so this keeps the
// heap's size bounded by leasedN rather than accumulating stale entries.
func (s *shard) removeFromLeaseHeap(j *job) {
	if j.heapIndex >= 0 {
		heap.Remove(&s.leaseHeap, j.heapIndex)
	}
}

// lookup returns the job at offset, if any, for internal test inspection
// (see JobQueue.lookupJob).
func (s *shard) lookup(offset int64) (*job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[offset]
	return j, ok
}

// stats returns this shard's O(1) running counters.
func (s *shard) stats() (pending, leased, completed, abandoned int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingN, s.leasedN, s.completedN, s.abandonedN
}

// parseJobID splits an external job ID ("job-<shardIdx>-<offset>") back
// into its shard index and per-shard offset. A malformed or out-of-range
// ID (shouldn't happen given main.go only ever passes back IDs this
// package minted) returns the same "job not found" error Ack/Fail already
// return for unknown IDs.
func parseJobID(jobID string) (shardIdx int, offset int64, err error) {
	parts := strings.SplitN(jobID, "-", 3)
	if len(parts) != 3 || parts[0] != "job" {
		return 0, 0, errors.New("job not found")
	}
	idx, err := strconv.Atoi(parts[1])
	if err != nil || idx < 0 || idx >= numShards {
		return 0, 0, errors.New("job not found")
	}
	off, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || off < 0 {
		return 0, 0, errors.New("job not found")
	}
	return idx, off, nil
}
