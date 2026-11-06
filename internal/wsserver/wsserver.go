// Package wsserver exposes an observer.Bus as a WebSocket endpoint: on
// connect, a client receives the bus's current backlog (oldest first),
// then a live stream of Envelope{V:1, Event: e} JSON frames as events are
// published, until it disconnects. Both cmd/playground and
// cmd/harness-replay use this single Handler so their wire framing can
// never drift apart.
package wsserver

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"

	"yodoq/observer"
)

var upgrader = websocket.Upgrader{
	// Local demo tool: CORS is wide open since web/'s Vite dev server runs
	// on a different origin/port than the Go server.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Handler returns an http.Handler that upgrades incoming requests to
// WebSocket connections and streams bus's events to each one.
func Handler(bus *observer.Bus) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("wsserver: upgrade failed: %v", err)
			return
		}
		defer conn.Close()

		id, ch, backlog := bus.Subscribe()
		defer bus.Unsubscribe(id)
		log.Printf("wsserver: client connected (subscriber %d), %d backlog events", id, len(backlog))
		defer log.Printf("wsserver: client disconnected (subscriber %d)", id)

		// Reader goroutine: WebSocket requires reading control frames (e.g.
		// pings/close) even on an otherwise write-only connection; also lets
		// us detect the client going away.
		closed := make(chan struct{})
		go func() {
			defer close(closed)
			for {
				if _, _, err := conn.NextReader(); err != nil {
					return
				}
			}
		}()

		for _, e := range backlog {
			if err := conn.WriteJSON(observer.Envelope{V: observer.CurrentProtocolVersion, Event: e}); err != nil {
				return
			}
		}

		for {
			select {
			case <-closed:
				return
			case e, ok := <-ch:
				if !ok {
					return
				}
				if err := conn.WriteJSON(observer.Envelope{V: observer.CurrentProtocolVersion, Event: e}); err != nil {
					return
				}
			}
		}
	})
}
