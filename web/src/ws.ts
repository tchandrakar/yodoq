// A small WebSocket client for the YODOQ observability protocol shared by
// cmd/playground and cmd/harness-replay: on connect, the server replays its
// backlog (oldest first), then streams live Envelope{v, event} JSON frames
// until the client disconnects.

export type Kind =
  | 'enqueued'
  | 'lease_granted'
  | 'lease_reclaimed'
  | 'ack'
  | 'ack_rejected'
  | 'fail'
  | 'fail_rejected'
  | 'stats_snapshot'
  | 'scenario_started'
  | 'scenario_finished'

export interface Stats {
  Pending: number
  Leased: number
  Completed: number
  Abandoned: number
}

export interface Event {
  seq: number
  kind: Kind
  simTimeMs: number
  wallTime: string
  jobId?: string
  workerId?: string
  prevWorkerId?: string
  payloadSize?: number
  leaseExpiresAt?: number
  err?: string
  stats?: Stats
  scenarioName?: string
  meta?: Record<string, string>
}

export interface Envelope {
  v: number
  event: Event
}

export type ConnectionStatus = 'connecting' | 'open' | 'closed'

// connect opens a WebSocket to addr (e.g. "ws://localhost:8080/ws") and
// invokes onEvent for every Envelope received (backlog and live alike —
// the caller can't tell the difference, and generally shouldn't need to:
// backlog just arrives first). onStatus reports connection lifecycle
// changes, which the UI uses to show a "disconnected" banner.
export function connect(
  addr: string,
  onEvent: (e: Envelope) => void,
  onStatus: (s: ConnectionStatus) => void,
): () => void {
  let closedByCaller = false
  let ws: WebSocket | null = null

  const open = () => {
    if (closedByCaller) return
    onStatus('connecting')
    ws = new WebSocket(addr)
    ws.onopen = () => onStatus('open')
    ws.onmessage = (ev) => {
      try {
        const envelope = JSON.parse(ev.data) as Envelope
        onEvent(envelope)
      } catch (err) {
        console.error('failed to parse envelope', err)
      }
    }
    ws.onclose = () => {
      onStatus('closed')
      if (!closedByCaller) {
        // Reconnect with a fixed short backoff; this is a demo tool, not a
        // production client, so a simple fixed delay is enough.
        setTimeout(open, 1000)
      }
    }
    ws.onerror = () => {
      // onclose will fire right after; nothing extra to do here.
    }
  }

  open()

  return () => {
    closedByCaller = true
    ws?.close()
  }
}
