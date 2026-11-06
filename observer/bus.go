package observer

import "sync"

// defaultSubscriberChannelCapacity is the bounded per-subscriber channel
// size. See Bus's backpressure policy below.
const defaultSubscriberChannelCapacity = 256

// Bus is a bounded, non-blocking, drop-newest fan-out pub/sub hub for
// Events. Publish never blocks the caller: it appends to a bounded ring
// buffer (so late joiners get recent backlog) and then does a best-effort,
// non-blocking send to every subscriber.
//
// Backpressure policy: each subscriber has a bounded channel (capacity
// defaultSubscriberChannelCapacity). When that channel is full, the
// incoming event is dropped (not the oldest queued one) and the
// subscriber's Dropped counter increments. This guarantees Publish is
// O(subscribers) and never blocks regardless of how slow a consumer is,
// and preserves strict ordering of what does arrive — a slow consumer
// self-heals once it catches up, and the next stats_snapshot
// resynchronizes it regardless of exactly which events it missed.
type Bus struct {
	mu          sync.Mutex
	nextSeq     uint64
	backlogSize int
	ring        []Event // bounded, oldest first
	nextSubID   int64
	subs        map[int64]*subscriber
}

type subscriber struct {
	ch      chan Event
	dropped uint64
}

// NewBus constructs a Bus with the given backlog ring-buffer size. A
// non-positive backlogSize disables backlog (new subscribers only see
// events published after they subscribe).
func NewBus(backlogSize int) *Bus {
	if backlogSize < 0 {
		backlogSize = 0
	}
	return &Bus{
		backlogSize: backlogSize,
		subs:        make(map[int64]*subscriber),
	}
}

// Publish assigns the event the next monotonic Seq, appends it to the
// backlog ring buffer, and fans it out to every current subscriber without
// blocking. Safe for concurrent use; never blocks regardless of how slow
// subscribers are.
func (b *Bus) Publish(e Event) {
	b.mu.Lock()
	b.nextSeq++
	e.Seq = b.nextSeq

	if b.backlogSize > 0 {
		b.ring = append(b.ring, e)
		if len(b.ring) > b.backlogSize {
			// Drop the oldest; keep the ring bounded.
			b.ring = b.ring[len(b.ring)-b.backlogSize:]
		}
	}

	for _, sub := range b.subs {
		select {
		case sub.ch <- e:
		default:
			sub.dropped++
		}
	}
	b.mu.Unlock()
}

// Subscribe registers a new subscriber and returns its id, a receive-only
// channel of future events, and a snapshot of the current backlog (oldest
// first) so a late joiner can catch up before consuming live events.
func (b *Bus) Subscribe() (id int64, ch <-chan Event, backlog []Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextSubID++
	id = b.nextSubID
	sub := &subscriber{ch: make(chan Event, defaultSubscriberChannelCapacity)}
	b.subs[id] = sub

	backlog = make([]Event, len(b.ring))
	copy(backlog, b.ring)

	return id, sub.ch, backlog
}

// Unsubscribe removes a subscriber. Safe to call more than once for the
// same id, or for an id that was never registered.
func (b *Bus) Unsubscribe(id int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if sub, ok := b.subs[id]; ok {
		close(sub.ch)
		delete(b.subs, id)
	}
}

// Dropped returns how many events have been dropped for the given
// subscriber id because its channel was full when Publish tried to send
// to it. Returns 0 for an unknown id (e.g. already unsubscribed).
func (b *Bus) Dropped(id int64) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if sub, ok := b.subs[id]; ok {
		return sub.dropped
	}
	return 0
}
