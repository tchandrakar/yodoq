package wsserver

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"yodoq/observer"
)

func TestWsserver_BacklogThenLiveStream(t *testing.T) {
	bus := observer.NewBus(10)
	bus.Publish(observer.Event{Kind: observer.KindEnqueued, JobID: "backlog-job"})

	srv := httptest.NewServer(Handler(bus))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// First frame should be the backlog event published before connecting.
	var env observer.Envelope
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := conn.ReadJSON(&env); err != nil {
		t.Fatalf("read backlog frame: %v", err)
	}
	if env.V != observer.CurrentProtocolVersion {
		t.Fatalf("V = %d, want %d", env.V, observer.CurrentProtocolVersion)
	}
	if env.Event.JobID != "backlog-job" {
		t.Fatalf("got JobID %q, want %q", env.Event.JobID, "backlog-job")
	}

	// Now publish a live event and confirm it streams through too.
	bus.Publish(observer.Event{Kind: observer.KindAck, JobID: "live-job"})
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := conn.ReadJSON(&env); err != nil {
		t.Fatalf("read live frame: %v", err)
	}
	if env.Event.JobID != "live-job" {
		t.Fatalf("got JobID %q, want %q", env.Event.JobID, "live-job")
	}
}
