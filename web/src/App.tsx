import { useEffect, useReducer, useRef, useState } from 'react'
import './App.css'
import EventFeed from './EventFeed'
import LeasesTable, { deriveLeases } from './LeasesTable'
import StatsStrip from './StatsStrip'
import { connect, type ConnectionStatus, type Envelope, type Stats } from './ws'

const MAX_EVENTS = 500
const MAX_STATS_HISTORY = 50

interface State {
  events: Envelope[] // newest first, bounded to MAX_EVENTS
  statsHistory: Stats[] // oldest first, bounded to MAX_STATS_HISTORY
  lastStats: Stats | null
  simNow: number
}

type Action = { type: 'reset' } | { type: 'envelope'; envelope: Envelope }

function reducer(state: State, action: Action): State {
  switch (action.type) {
    case 'reset':
      return { events: [], statsHistory: [], lastStats: null, simNow: 0 }
    case 'envelope': {
      const events = [action.envelope, ...state.events]
      if (events.length > MAX_EVENTS) events.length = MAX_EVENTS

      let statsHistory = state.statsHistory
      let lastStats = state.lastStats
      if (action.envelope.event.kind === 'stats_snapshot' && action.envelope.event.stats) {
        lastStats = action.envelope.event.stats
        statsHistory = [...state.statsHistory, lastStats]
        if (statsHistory.length > MAX_STATS_HISTORY) {
          statsHistory = statsHistory.slice(statsHistory.length - MAX_STATS_HISTORY)
        }
      }

      return { events, statsHistory, lastStats, simNow: action.envelope.event.simTimeMs }
    }
  }
}

function App() {
  const [hostPort, setHostPort] = useState('localhost:8080')
  const [status, setStatus] = useState<ConnectionStatus>('closed')
  const [filter, setFilter] = useState('')
  const [state, dispatch] = useReducer(reducer, {
    events: [],
    statsHistory: [],
    lastStats: null,
    simNow: 0,
  })
  const disconnectRef = useRef<(() => void) | null>(null)

  useEffect(() => {
    disconnectRef.current?.()
    dispatch({ type: 'reset' })
    disconnectRef.current = connect(
      `ws://${hostPort}/ws`,
      (envelope) => dispatch({ type: 'envelope', envelope }),
      setStatus,
    )
    return () => disconnectRef.current?.()
  }, [hostPort])

  const leases = deriveLeases(state.events.map((env) => env.event))

  return (
    <div className="app">
      <header className="toolbar">
        <h1>YODOQ Observability</h1>
        <label>
          Server{' '}
          <input
            value={hostPort}
            onChange={(e) => setHostPort(e.target.value)}
            placeholder="localhost:8080"
          />
        </label>
        <span className={`status status-${status}`}>{status}</span>
      </header>

      <StatsStrip current={state.lastStats} history={state.statsHistory} />

      <section>
        <h2>In-flight leases</h2>
        <LeasesTable leases={leases} simNow={state.simNow} />
      </section>

      <section>
        <h2>Event feed</h2>
        <input
          className="filter"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="filter by kind, jobId, workerId, or scenario…"
        />
        <EventFeed events={state.events} filter={filter} />
      </section>
    </div>
  )
}

export default App
