package solution

import "testing"

func TestResolveOptionsDefaults(t *testing.T) {
	got := resolveOptions()
	if got.priority != PriorityNormal {
		t.Errorf("default priority = %v, want PriorityNormal", got.priority)
	}
	if got.maxAttempts != 0 {
		t.Errorf("default maxAttempts = %d, want 0 (unlimited)", got.maxAttempts)
	}
}

func TestResolveOptionsOverrides(t *testing.T) {
	got := resolveOptions(WithPriority(PriorityHigh), WithMaxAttempts(3))
	if got.priority != PriorityHigh {
		t.Errorf("priority = %v, want PriorityHigh", got.priority)
	}
	if got.maxAttempts != 3 {
		t.Errorf("maxAttempts = %d, want 3", got.maxAttempts)
	}
}

func TestResolveQueueOptionsDefaults(t *testing.T) {
	got := resolveQueueOptions()
	if got.agingIntervalMS != defaultAgingIntervalMS {
		t.Errorf("default agingIntervalMS = %d, want %d", got.agingIntervalMS, defaultAgingIntervalMS)
	}
}

func TestResolveQueueOptionsOverride(t *testing.T) {
	got := resolveQueueOptions(WithAgingIntervalMS(500))
	if got.agingIntervalMS != 500 {
		t.Errorf("agingIntervalMS = %d, want 500", got.agingIntervalMS)
	}
}

func TestWithAgingIntervalMSNonPositiveFallsBackToDefault(t *testing.T) {
	got := resolveQueueOptions(WithAgingIntervalMS(0))
	if got.agingIntervalMS != defaultAgingIntervalMS {
		t.Errorf("agingIntervalMS with ms=0 = %d, want default %d", got.agingIntervalMS, defaultAgingIntervalMS)
	}

	got = resolveQueueOptions(WithAgingIntervalMS(-10))
	if got.agingIntervalMS != defaultAgingIntervalMS {
		t.Errorf("agingIntervalMS with ms=-10 = %d, want default %d", got.agingIntervalMS, defaultAgingIntervalMS)
	}
}

func TestNewWithNoOptsMatchesDefaultBehavior(t *testing.T) {
	clock := &raceClock{}
	q := New(clock)
	if q.opts.agingIntervalMS != defaultAgingIntervalMS {
		t.Errorf("New(clock) agingIntervalMS = %d, want default %d", q.opts.agingIntervalMS, defaultAgingIntervalMS)
	}

	id, err := q.Enqueue([]byte("payload"))
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	j := q.jobs[id]
	if j.priority != PriorityNormal {
		t.Errorf("job priority = %v, want PriorityNormal", j.priority)
	}
	if j.maxAttempts != 0 {
		t.Errorf("job maxAttempts = %d, want 0 (unlimited)", j.maxAttempts)
	}
}
