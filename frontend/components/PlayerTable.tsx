import Link from 'next/link'
import type { PlayerRow } from '@/lib/types'
import { TIER_COLORS, TIER_LABELS, fmtProb } from '@/lib/types'

interface Props {
  players: PlayerRow[]
}

export default function PlayerTable({ players }: Props) {
  return (
    <div className="overflow-x-auto rounded-xl border border-gray-200 bg-white">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="border-b border-gray-100 bg-gray-50 text-left text-xs font-semibold uppercase tracking-wide text-gray-500">
            <th className="px-4 py-3">Player</th>
            <th className="px-4 py-3">Pos</th>
            <th className="px-4 py-3">Team</th>
            <th className="px-4 py-3">Breakout %</th>
            <th className="px-4 py-3">Signal</th>
            <th className="px-4 py-3 text-right">Updated</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-50">
          {players.map((p) => {
            const tier   = TIER_COLORS[p.risk_tier] ?? TIER_COLORS.low
            const label  = TIER_LABELS[p.risk_tier]  ?? p.risk_tier
            const pct    = fmtProb(p.breakout_prob)
            const date   = new Date(p.updated_at).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })

            return (
              <tr
                key={`${p.player_id}-${p.season}`}
                className="hover:bg-gray-50 transition-colors cursor-pointer"
              >
                <td className="px-4 py-3 font-medium text-gray-900">
                  <Link href={`/player/${p.player_id}`} className="hover:underline">
                    {p.player_name}
                  </Link>
                </td>
                <td className="px-4 py-3 text-gray-500">{p.position}</td>
                <td className="px-4 py-3 text-gray-500">{p.team}</td>
                <td className="px-4 py-3">
                  <div className="flex items-center gap-2">
                    <div className="w-24 h-2 rounded-full bg-gray-100 overflow-hidden">
                      <div
                        className={`h-full rounded-full ${
                          p.risk_tier === 'high'
                            ? 'bg-green-500'
                            : p.risk_tier === 'medium'
                            ? 'bg-amber-400'
                            : 'bg-red-400'
                        }`}
                        style={{ width: `${p.breakout_prob * 100}%` }}
                      />
                    </div>
                    <span className="font-mono font-semibold text-gray-800 w-8 text-right">
                      {pct}
                    </span>
                  </div>
                </td>
                <td className="px-4 py-3">
                  <span className={`inline-flex px-2 py-0.5 rounded text-xs font-semibold ${tier.bg} ${tier.text}`}>
                    {label}
                  </span>
                </td>
                <td className="px-4 py-3 text-right text-gray-400 text-xs">{date}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
