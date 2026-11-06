import { useEffect, useRef, useState } from 'react'
import './App.css'
import { connect, type ConnectionStatus, type Envelope } from './ws'

const MAX_EVENTS = 500

function App() {
  const [hostPort, setHostPort] = useState('localhost:8080')
  const [status, setStatus] = useState<ConnectionStatus>('closed')
  const [events, setEvents] = useState<Envelope[]>([])
  const disconnectRef = useRef<(() => void) | null>(null)

  useEffect(() => {
    disconnectRef.current?.()
    setEvents([])
    disconnectRef.current = connect(
      `ws://${hostPort}/ws`,
      (env) => {
        setEvents((prev) => {
          const next = [env, ...prev]
          return next.length > MAX_EVENTS ? next.slice(0, MAX_EVENTS) : next
        })
      },
      setStatus,
    )
    return () => disconnectRef.current?.()
  }, [hostPort])

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

      <main>
        <ul className="feed">
          {events.map((env) => (
            <li key={env.event.seq} className={`event kind-${env.event.kind}`}>
              <span className="seq">#{env.event.seq}</span>
              <span className="kind">{env.event.kind}</span>
              {env.event.jobId && <span className="field">job={env.event.jobId}</span>}
              {env.event.workerId && <span className="field">worker={env.event.workerId}</span>}
              {env.event.prevWorkerId && (
                <span className="field">prevWorker={env.event.prevWorkerId}</span>
              )}
              {env.event.err && <span className="field err">err={env.event.err}</span>}
              {env.event.stats && (
                <span className="field">
                  pending={env.event.stats.Pending} leased={env.event.stats.Leased} completed=
                  {env.event.stats.Completed} abandoned={env.event.stats.Abandoned}
                </span>
              )}
              {env.event.scenarioName && (
                <span className="field">scenario={env.event.scenarioName}</span>
              )}
            </li>
          ))}
        </ul>
        {events.length === 0 && (
          <p className="empty">No events yet — waiting for {hostPort}…</p>
        )}
      </main>
    </div>
  )
}

export default App
