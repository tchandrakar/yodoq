import type { Stats } from './ws'

const LABELS: { key: keyof Stats; label: string; color: string }[] = [
  { key: 'Pending', label: 'Pending', color: '#8884d8' },
  { key: 'Leased', label: 'Leased', color: '#e0a800' },
  { key: 'Completed', label: 'Completed', color: '#2e7d32' },
  { key: 'Abandoned', label: 'Abandoned', color: '#c0392b' },
]

// Sparkline renders a tiny dependency-free SVG polyline over the last N
// values of one stat — no charting library needed for four sparklines.
function Sparkline({ values, color }: { values: number[]; color: string }) {
  if (values.length < 2) {
    return <svg className="sparkline" viewBox="0 0 100 24" />
  }
  const max = Math.max(1, ...values)
  const step = 100 / (values.length - 1)
  const points = values
    .map((v, i) => `${i * step},${24 - (v / max) * 22 - 1}`)
    .join(' ')
  return (
    <svg className="sparkline" viewBox="0 0 100 24" preserveAspectRatio="none">
      <polyline points={points} fill="none" stroke={color} strokeWidth="2" />
    </svg>
  )
}

interface Props {
  current: Stats | null
  history: Stats[] // oldest first, bounded by the caller
}

export default function StatsStrip({ current, history }: Props) {
  return (
    <div className="stats-strip">
      {LABELS.map(({ key, label, color }) => (
        <div className="stat-tile" key={key}>
          <div className="stat-label">{label}</div>
          <div className="stat-value">{current ? current[key] : '–'}</div>
          <Sparkline values={history.map((s) => s[key])} color={color} />
        </div>
      ))}
    </div>
  )
}
