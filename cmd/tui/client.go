package main

import (
	"time"

	"github.com/gorilla/websocket"

	"yodoq/observer"
)

// connectedMsg carries the live websocket connection once dialing
// succeeds.
type connectedMsg struct{ conn *websocket.Conn }

// disconnectedMsg is sent when the read loop ends (error or clean close).
type disconnectedMsg struct{ err error }

// envelopeMsg carries one decoded event frame from the server.
type envelopeMsg struct{ envelope observer.Envelope }

// dial attempts one connection to addr. Intended to be wrapped in a
// tea.Cmd; returns connectedMsg on success or disconnectedMsg on failure
// (the model's Update reacts to either by scheduling a retry).
func dial(addr string) (*websocket.Conn, error) {
	conn, _, err := websocket.DefaultDialer.Dial(addr, nil)
	return conn, err
}

// readLoop reads frames from conn until it errors or closes, sending each
// one to the given channel as an envelopeMsg, and a final disconnectedMsg
// when the loop ends. Meant to run in its own goroutine, fed into
// bubbletea via a channel-reading tea.Cmd (see model.go's waitForMsg).
func readLoop(conn *websocket.Conn, out chan<- any) {
	for {
		var env observer.Envelope
		if err := conn.ReadJSON(&env); err != nil {
			out <- disconnectedMsg{err: err}
			return
		}
		out <- envelopeMsg{envelope: env}
	}
}

// reconnectDelay is the fixed backoff between dial attempts. A demo tool's
// TUI doesn't need exponential backoff with jitter; a short fixed delay
// keeps "server not up yet" and "server restarted" both responsive.
const reconnectDelay = 1 * time.Second
