package solution

// Priority is a job's priority level. Lower values are more urgent.
type Priority int

const (
	PriorityHigh   Priority = 0
	PriorityNormal Priority = 1
	PriorityLow    Priority = 2
)

// effectivePriority returns a job's priority score as of now, after aging:
// every agingIntervalMS of wait time (now - enqueuedAt) reduces the score by
// 1 (i.e. makes it more urgent), floored at PriorityHigh. Lower is more
// urgent, matching the Priority constants above. agingIntervalMS is a
// per-queue, configurable value (see WithAgingIntervalMS in options.go), not
// a package-level constant, so this is a pure function of its inputs only —
// it never reads time.Now() or any queue-wide state.
func effectivePriority(basePriority Priority, enqueuedAt, now, agingIntervalMS int64) int {
	waited := now - enqueuedAt
	if waited <= 0 || agingIntervalMS <= 0 {
		return int(basePriority)
	}
	steps := waited / agingIntervalMS
	score := int64(basePriority) - steps
	if score < int64(PriorityHigh) {
		score = int64(PriorityHigh)
	}
	return int(score)
}

// candidate is one eligible job's standing in Lease's selection, now
// compared across shards (see shard.go's peekBest and queue.go's Lease)
// rather than within a single global scan. seq is the job's global
// enqueue sequence number (assigned by JobQueue.Enqueue via an atomic
// counter), which replaces the old single-slice order-index as the FIFO
// tie-break now that jobs are partitioned across per-shard offsets that
// aren't comparable to each other for ordering purposes.
type candidate struct {
	seq               int64
	effectivePriority int
}

// betterCandidate reports whether candidate c should replace best (best may
// be nil, meaning "no candidate yet"). Lower effectivePriority wins; ties
// are broken by earlier global sequence number (seq), which is what makes
// this degenerate to today's exact "first eligible job in order" behavior
// whenever every job shares the same effective priority (always true for
// main.go, which never sets a priority and never waits long enough to
// matter within one Lease call).
func betterCandidate(best *candidate, c candidate) bool {
	if best == nil {
		return true
	}
	if c.effectivePriority != best.effectivePriority {
		return c.effectivePriority < best.effectivePriority
	}
	return c.seq < best.seq
}
