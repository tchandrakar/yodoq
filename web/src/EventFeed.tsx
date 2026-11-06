import type { Envelope } from './ws'

interface Props {
  events: Envelope[] // newest first
  filter: string
}

// matchesFilter does a simple case-insensitive substring match against
// kind/jobId/workerId — no server-side query language needed at this
// scale; filtering happens entirely client-side over what's already
// buffered.
function matchesFilter(env: Envelope, filter: string): boolean {
  if (!filter) return true
  const needle = filter.toLowerCase()
  const e = env.event
  return (
    e.kind.toLowerCase().includes(needle) ||
    (e.jobId ?? '').toLowerCase().includes(needle) ||
    (e.workerId ?? '').toLowerCase().includes(needle) ||
    (e.scenarioName ?? '').toLowerCase().includes(needle)
  )
}

export default function EventFeed({ events, filter }: Props) {
  const filtered = events.filter((env) => matchesFilter(env, filter))

  if (filtered.length === 0) {
    return <p className="empty">No events match.</p>
  }

  return (
    <ul className="feed">
      {filtered.map((env) => (
        <li key={env.event.seq} className={`event kind-${env.event.kind}`}>
          <span className="seq">#{env.event.seq}</span>
          <span className="kind">{env.event.kind}</span>
          {env.event.jobId && <span className="field">job={env.event.jobId}</span>}
          {env.event.workerId && <span className="field">worker={env.event.workerId}</span>}
          {env.event.prevWorkerId && (
            <span className="field">prevWorker={env.event.prevWorkerId}</span>
          )}
          {env.event.err && <span className="field err">err={env.event.err}</span>}
          {env.event.scenarioName && (
            <span className="field">scenario={env.event.scenarioName}</span>
          )}
        </li>
      ))}
    </ul>
  )
}
