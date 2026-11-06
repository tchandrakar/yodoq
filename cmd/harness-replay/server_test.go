package main

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"yodoq/internal/wsserver"
	"yodoq/observer"
)

// TestReplay_ThreeLevelsEmitExpectedEventCounts runs all three levels
// against a bus with a WS test client attached, draining it concurrently
// (a real client would too — the Bus's channel is bounded, and this test
// otherwise risks dropping events faster than the assertions below can
// account for), and asserts at least one event of each major taxonomy kind
// appears and that scenario_finished fires once per level-1/level-2 run
// plus once per level-3 rate.
func TestReplay_ThreeLevelsEmitExpectedEventCounts(t *testing.T) {
	bus := observer.NewBus(500)

	httpSrv := httptest.NewServer(wsserver.Handler(bus))
	defer httpSrv.Close()

	wsURL := "ws" + strings.TrimPrefix(httpSrv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	seenKinds := map[observer.Kind]int{}
	var mu sync.Mutex
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		// gorilla/websocket does not support resuming reads after any
		// error (including a deadline timeout) — the connection is
		// considered failed from that point on — so this is a plain
		// blocking read loop; the test stops it by closing conn.
		for {
			var env observer.Envelope
			if err := conn.ReadJSON(&env); err != nil {
				return
			}
			mu.Lock()
			seenKinds[env.Event.Kind]++
			mu.Unlock()
		}
	}()

	runLevels(bus, []int{1, 2, 3})

	// Give the reader a little time to drain anything still in flight,
	// then stop it by closing the connection (causes the blocked read to
	// return with an error).
	time.Sleep(300 * time.Millisecond)
	conn.Close()
	<-readerDone

	mu.Lock()
	defer mu.Unlock()
	for _, want := range []observer.Kind{
		observer.KindEnqueued,
		observer.KindLeaseGranted,
		observer.KindAck,
		observer.KindScenarioStarted,
		observer.KindScenarioFinished,
	} {
		if seenKinds[want] == 0 {
			t.Errorf("expected at least one %q event, saw none (seen: %v)", want, seenKinds)
		}
	}

	const wantScenarioFinished = 2 + 7 // level1 + level2, plus level3's 7 rates
	if got := seenKinds[observer.KindScenarioFinished]; got != wantScenarioFinished {
		t.Errorf("scenario_finished fired %d times, want %d", got, wantScenarioFinished)
	}
}
