// Package observer provides a non-invasive observability layer over
// yodoq/solution.JobQueue: an event model, a bounded fan-out pub/sub bus,
// and a wrapper that forwards every call to a *solution.JobQueue while
// publishing one Event per call outcome. Nothing here touches
// solution/queue.go, and nothing here depends on HTTP/WebSocket/TUI
// machinery — it stays importable by anything.
package observer

import (
	"time"

	"yodoq/solution"
)

// Kind identifies what kind of thing happened.
type Kind string

const (
	KindEnqueued         Kind = "enqueued"
	KindLeaseGranted     Kind = "lease_granted"
	KindLeaseReclaimed   Kind = "lease_reclaimed" // expiry, not Fail
	KindAck              Kind = "ack"
	KindAckRejected      Kind = "ack_rejected"
	KindFail             Kind = "fail"
	KindFailRejected     Kind = "fail_rejected"
	KindStatsSnapshot    Kind = "stats_snapshot"
	KindScenarioStarted  Kind = "scenario_started"
	KindScenarioFinished Kind = "scenario_finished"
)

// Event is one observable happening. JSON field names are stable; new
// fields may be added over time (clients must ignore unknown fields —
// encoding/json already does this on decode).
type Event struct {
	Seq            uint64            `json:"seq"` // bus-assigned, monotonic
	Kind           Kind              `json:"kind"`
	SimTimeMS      int64             `json:"simTimeMs"` // from the queue's Clock
	WallTime       time.Time         `json:"wallTime"`  // for human-readable UI timestamps
	JobID          string            `json:"jobId,omitempty"`
	WorkerID       string            `json:"workerId,omitempty"`
	PrevWorkerID   string            `json:"prevWorkerId,omitempty"` // set on lease_reclaimed
	PayloadSize    int               `json:"payloadSize,omitempty"`
	LeaseExpiresAt int64             `json:"leaseExpiresAt,omitempty"`
	Err            string            `json:"err,omitempty"`   // set on *_rejected
	Stats          *solution.Stats   `json:"stats,omitempty"` // set on stats_snapshot
	ScenarioName   string            `json:"scenarioName,omitempty"`
	Meta           map[string]string `json:"meta,omitempty"` // open extension point
}

// Envelope is the wire message. "v" lets a client detect a future breaking
// change without every event needing it; absent a bump, clients just
// ignore fields they don't recognize.
type Envelope struct {
	V     int   `json:"v"` // currently 1
	Event Event `json:"event"`
}

// CurrentProtocolVersion is the Envelope.V value this package emits.
const CurrentProtocolVersion = 1
