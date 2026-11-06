package solution

// expiryHeap is a container/heap-based min-heap over *job ordered by
// leaseExpiresAt. It lets a shard find the soonest-expiring leased job in
// O(log k) instead of scanning every job, where k is the number of jobs
// currently in the heap (bounded by that shard's leasedN, never by total
// historical job count).
//
// Jobs are pushed onto the heap when they're leased and popped either when
// their lease is reclaimed as expired (shard.peekBest), or removed exactly
// via their tracked heapIndex when Ack/Fail releases the lease before it
// expires (shard.ack/shard.fail) — so a job never has more than one live
// heap entry at a time.
type expiryHeap []*job

func (h expiryHeap) Len() int { return len(h) }

func (h expiryHeap) Less(i, j int) bool {
	return h[i].leaseExpiresAt < h[j].leaseExpiresAt
}

func (h expiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *expiryHeap) Push(x any) {
	j := x.(*job)
	j.heapIndex = len(*h)
	*h = append(*h, j)
}

func (h *expiryHeap) Pop() any {
	old := *h
	n := len(old)
	j := old[n-1]
	old[n-1] = nil
	j.heapIndex = -1
	*h = old[:n-1]
	return j
}
