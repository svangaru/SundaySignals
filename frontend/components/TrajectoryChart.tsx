'use client'
import {
  ComposedChart,
  Line,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceDot,
} from 'recharts'
import type { PlayerSeason } from '@/lib/types'

interface Props {
  seasons: PlayerSeason[]
}

interface ChartRow {
  season: number
  target_share: number | null
  snap_share:   number | null
  fantasy_ppg:  number | null
  broke_out:    boolean | null
}

export default function TrajectoryChart({ seasons }: Props) {
  if (seasons.length === 0) return null

  const data: ChartRow[] = seasons.map((s) => ({
    season:       s.season,
    target_share: s.target_share   != null ? parseFloat((s.target_share  * 100).toFixed(1)) : null,
    snap_share:   s.snap_share_y0  != null ? parseFloat((s.snap_share_y0 * 100).toFixed(1)) : null,
    fantasy_ppg:  s.fantasy_ppg    != null ? parseFloat(s.fantasy_ppg.toFixed(1))           : null,
    broke_out:    s.broke_out,
  }))

  const breakoutSeasons = data.filter((d) => d.broke_out)

  return (
    <div className="space-y-6">
      {/* Usage chart */}
      <div>
        <p className="text-xs text-gray-400 mb-2">Usage Rate (%)</p>
        <ResponsiveContainer width="100%" height={180}>
          <ComposedChart data={data} margin={{ top: 4, right: 8, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
            <XAxis dataKey="season" tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} unit="%" domain={[0, 'auto']} />
            <Tooltip formatter={(v: number) => [`${v}%`]} />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            <Line
              type="monotone"
              dataKey="target_share"
              name="Target Share"
              stroke="#6366f1"
              strokeWidth={2}
              dot={{ r: 3 }}
              connectNulls
            />
            <Line
              type="monotone"
              dataKey="snap_share"
              name="Snap Share"
              stroke="#f59e0b"
              strokeWidth={2}
              dot={{ r: 3 }}
              connectNulls
            />
            {breakoutSeasons.map((d) =>
              d.target_share != null ? (
                <ReferenceDot
                  key={d.season}
                  x={d.season}
                  y={d.target_share}
                  r={6}
                  fill="#22c55e"
                  stroke="white"
                  strokeWidth={2}
                  label={{ value: '🔥', fontSize: 14, dy: -12 }}
                />
              ) : null
            )}
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      {/* Fantasy PPG chart */}
      <div>
        <p className="text-xs text-gray-400 mb-2">Fantasy PPG (PPR)</p>
        <ResponsiveContainer width="100%" height={160}>
          <ComposedChart data={data} margin={{ top: 4, right: 8, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
            <XAxis dataKey="season" tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Bar
              dataKey="fantasy_ppg"
              name="Fantasy PPG"
              fill="#6366f1"
              fillOpacity={0.7}
              radius={[3, 3, 0, 0]}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
