package solution

// recordAttempt increments the job's attempt counter. Called by Lease
// exactly once, at the moment a lease is handed out (whether this is the
// job's first lease or a re-lease after a crash/Fail).
func recordAttempt(j *job) {
	j.attempts++
}

// releaseJob decides the post-release state for a job whose lease is being
// given up — either because it expired (crash recovery) or because the
// worker explicitly called Fail. Returns statePending if the job may still
// be retried, or stateAbandoned if it has exhausted maxAttempts.
// maxAttempts <= 0 means unlimited: always statePending, identical to
// today's behavior.
//
// Honesty note (FR-8): this bounds *leases*, not *executions*. The queue
// can only observe that a job was handed to a worker, never whether that
// worker actually ran it — a worker that is leased and crashes before
// recording execution still consumes an attempt, and a worker that
// successfully runs the job but whose Ack is dropped also still consumes an
// attempt on the next (unnecessary) re-lease. So "a job can be leased at
// most K times" is the honest, enforceable guarantee; it upper-bounds, but
// may slightly over-count relative to, actual executions.
func releaseJob(j *job) jobState {
	if j.maxAttempts > 0 && j.attempts >= j.maxAttempts {
		return stateAbandoned
	}
	return statePending
}
