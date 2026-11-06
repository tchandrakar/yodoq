import type { Event } from './ws'

export interface Lease {
  jobId: string
  workerId: string
  leaseExpiresAt: number
}

// deriveLeases replays the event list (newest first, as App.tsx stores
// them) to find jobs currently in flight: a job enters on
// lease_granted/lease_reclaimed and leaves on ack/fail. Since events are
// newest-first, the first (most recent) lease/ack/fail seen per job ID
// wins.
export function deriveLeases(events: Event[]): Lease[] {
  const seen = new Set<string>()
  const leases: Lease[] = []
  for (const e of events) {
    if (!e.jobId || seen.has(e.jobId)) continue
    switch (e.kind) {
      case 'lease_granted':
      case 'lease_reclaimed':
        seen.add(e.jobId)
        leases.push({
          jobId: e.jobId,
          workerId: e.workerId ?? '',
          leaseExpiresAt: e.leaseExpiresAt ?? 0,
        })
        break
      case 'ack':
      case 'fail':
        seen.add(e.jobId) // job left in-flight; exclude it
        break
    }
  }
  return leases
}

function expiryClass(leaseExpiresAt: number, simNow: number): string {
  const remaining = leaseExpiresAt - simNow
  if (remaining <= 0) return 'expiry-red'
  if (remaining < 300) return 'expiry-amber'
  return 'expiry-green'
}

interface Props {
  leases: Lease[]
  simNow: number
}

export default function LeasesTable({ leases, simNow }: Props) {
  if (leases.length === 0) {
    return <p className="empty">No jobs currently leased.</p>
  }
  return (
    <table className="leases">
      <thead>
        <tr>
          <th>Job</th>
          <th>Worker</th>
          <th>Expires in (sim ms)</th>
        </tr>
      </thead>
      <tbody>
        {leases.map((l) => (
          <tr key={l.jobId} className={expiryClass(l.leaseExpiresAt, simNow)}>
            <td>{l.jobId}</td>
            <td>{l.workerId}</td>
            <td>{l.leaseExpiresAt - simNow}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
