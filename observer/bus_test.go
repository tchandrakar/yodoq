package observer

import (
	"testing"
	"time"
)

func TestBus_PublishFanOutToAllSubscribers(t *testing.T) {
	b := NewBus(10)
	_, ch1, _ := b.Subscribe()
	_, ch2, _ := b.Subscribe()

	b.Publish(Event{Kind: KindEnqueued, JobID: "job-1"})

	for i, ch := range []<-chan Event{ch1, ch2} {
		select {
		case e := <-ch:
			if e.JobID != "job-1" || e.Kind != KindEnqueued {
				t.Fatalf("subscriber %d got unexpected event: %+v", i, e)
			}
			if e.Seq == 0 {
				t.Fatalf("subscriber %d got event with zero Seq", i)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d did not receive the published event", i)
		}
	}
}

func TestBus_DropsNewestWhenSubscriberFull(t *testing.T) {
	b := NewBus(0)
	id, ch, _ := b.Subscribe()

	// Fill the subscriber's channel beyond capacity without draining it.
	total := defaultSubscriberChannelCapacity + 5
	for i := 0; i < total; i++ {
		b.Publish(Event{Kind: KindEnqueued, JobID: "x"})
	}

	if dropped := b.Dropped(id); dropped != 5 {
		t.Fatalf("Dropped() = %d, want 5", dropped)
	}

	// What *does* arrive must be the oldest events, in order (drop-newest
	// policy preserves ordering of what arrives).
	var seqs []uint64
	for len(ch) > 0 {
		e := <-ch
		seqs = append(seqs, e.Seq)
	}
	if len(seqs) != defaultSubscriberChannelCapacity {
		t.Fatalf("got %d buffered events, want %d", len(seqs), defaultSubscriberChannelCapacity)
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("events out of order: %v", seqs)
		}
	}
	if seqs[0] != 1 {
		t.Fatalf("first buffered event has Seq %d, want 1 (oldest should be kept)", seqs[0])
	}
}

func TestBus_BacklogDeliveredOnSubscribe(t *testing.T) {
	b := NewBus(5)
	for i := 0; i < 3; i++ {
		b.Publish(Event{Kind: KindEnqueued, JobID: "job"})
	}

	_, _, backlog := b.Subscribe()
	if len(backlog) != 3 {
		t.Fatalf("got %d backlog events, want 3", len(backlog))
	}
	for i := 1; i < len(backlog); i++ {
		if backlog[i].Seq <= backlog[i-1].Seq {
			t.Fatalf("backlog out of order: %+v", backlog)
		}
	}

	// Backlog ring is bounded: publish past the bound and confirm it
	// truncates to the most recent backlogSize entries.
	for i := 0; i < 10; i++ {
		b.Publish(Event{Kind: KindEnqueued, JobID: "job"})
	}
	_, _, backlog2 := b.Subscribe()
	if len(backlog2) != 5 {
		t.Fatalf("got %d backlog events after overflow, want 5", len(backlog2))
	}
}

func TestBus_UnsubscribeStopsDelivery(t *testing.T) {
	b := NewBus(0)
	id, ch, _ := b.Subscribe()
	b.Unsubscribe(id)

	b.Publish(Event{Kind: KindEnqueued, JobID: "after-unsubscribe"})

	// The channel should be closed, not blocked waiting for a delivery
	// that will never come.
	select {
	case e, ok := <-ch:
		if ok {
			t.Fatalf("got event %+v on a channel that should be closed", e)
		}
	case <-time.After(time.Second):
		t.Fatal("channel was not closed after Unsubscribe")
	}

	// Unsubscribing again, or an unknown id, must not panic.
	b.Unsubscribe(id)
	b.Unsubscribe(999)
}
