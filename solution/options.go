package solution

// EnqueueOption customizes a single call to Enqueue. The zero value of
// every field in enqueueOptions must mean "today's behavior" (PriorityNormal,
// unlimited attempts), so that existing call sites like
// q.Enqueue([]byte(...)) (zero opts) keep compiling and behaving exactly as
// before.
type EnqueueOption func(*enqueueOptions)

// enqueueOptions holds the resolved result of applying a job's
// EnqueueOptions.
type enqueueOptions struct {
	priority    Priority // defaults to PriorityNormal
	maxAttempts int      // defaults to 0 (unlimited)
}

// WithPriority sets the priority level a job is enqueued at. Jobs enqueued
// without this option default to PriorityNormal.
func WithPriority(p Priority) EnqueueOption {
	return func(o *enqueueOptions) {
		o.priority = p
	}
}

// WithMaxAttempts caps how many times a job may be leased in total. k <= 0
// (including simply omitting this option) means unlimited attempts,
// identical to today's behavior. k >= 1 means the job may be leased at most
// k times total; once the k-th lease is given up without an intervening Ack,
// the job becomes Abandoned instead of Pending (see dedup.go).
func WithMaxAttempts(k int) EnqueueOption {
	return func(o *enqueueOptions) {
		o.maxAttempts = k
	}
}

// resolveOptions applies opts over the documented defaults and returns the
// resolved enqueueOptions.
func resolveOptions(opts ...EnqueueOption) enqueueOptions {
	resolved := enqueueOptions{
		priority:    PriorityNormal,
		maxAttempts: 0,
	}
	for _, opt := range opts {
		opt(&resolved)
	}
	return resolved
}

// QueueOption customizes a *JobQueue at construction time via New. The zero
// value of every queueOptions field must mean "today's behavior," so that
// existing call sites like solution.New(clock) (zero opts) keep compiling
// and behaving exactly as before.
type QueueOption func(*queueOptions)

// queueOptions holds the resolved result of applying New's QueueOptions.
type queueOptions struct {
	agingIntervalMS int64
}

// defaultAgingIntervalMS is the aging rate used when New is called with no
// WithAgingIntervalMS option — every defaultAgingIntervalMS of wait time
// reduces a job's effective priority value by 1 (see priority.go).
const defaultAgingIntervalMS int64 = 2000

// WithAgingIntervalMS overrides the queue's anti-starvation aging rate: every
// ms of wait time a pending job accrues reduces its effective priority value
// by 1, floored at PriorityHigh. ms <= 0 falls back to
// defaultAgingIntervalMS, since an interval of zero or negative would be
// meaningless (instant/undefined aging).
func WithAgingIntervalMS(ms int64) QueueOption {
	return func(o *queueOptions) {
		if ms <= 0 {
			ms = defaultAgingIntervalMS
		}
		o.agingIntervalMS = ms
	}
}

// resolveQueueOptions applies opts over the documented defaults and returns
// the resolved queueOptions.
func resolveQueueOptions(opts ...QueueOption) queueOptions {
	resolved := queueOptions{
		agingIntervalMS: defaultAgingIntervalMS,
	}
	for _, opt := range opts {
		opt(&resolved)
	}
	return resolved
}
