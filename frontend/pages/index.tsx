import type { GetServerSideProps, NextPage } from 'next'
import { useState } from 'react'
import { supabase } from '@/lib/supabase'
import type { PlayerRow, Prediction, PlayerSeason } from '@/lib/types'
import PlayerTable from '@/components/PlayerTable'

interface Props {
  players: PlayerRow[]
  season: number
  availableSeasons: number[]
}

const POSITIONS = ['RB', 'WR', 'TE']
const TIERS = ['high', 'medium', 'low']
const MODEL_TYPES = ['seasonal', 'weekly']

export default function IndexPage({ players, season, availableSeasons }: Props) {
  const [posFilter, setPosFilter]     = useState<string>('all')
  const [tierFilter, setTierFilter]   = useState<string>('all')
  const [modelFilter, setModelFilter] = useState<string>('seasonal')
  const [search, setSearch]           = useState('')

  const filtered = players.filter((p) => {
    if (posFilter !== 'all' && p.position !== posFilter) return false
    if (tierFilter !== 'all' && p.risk_tier !== tierFilter) return false
    if (search && !p.player_name.toLowerCase().includes(search.toLowerCase())) return false
    return true
  })

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Player Explorer</h1>
        <p className="text-sm text-gray-500 mt-1">
          {season} season — breakout and regression probabilities for RB, WR, TE
        </p>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-3 mb-5">
        <input
          type="text"
          placeholder="Search player..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-gray-400 w-48"
        />

        <select
          value={posFilter}
          onChange={(e) => setPosFilter(e.target.value)}
          className="border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-gray-400"
        >
          <option value="all">All Positions</option>
          {POSITIONS.map((p) => <option key={p} value={p}>{p}</option>)}
        </select>

        <select
          value={tierFilter}
          onChange={(e) => setTierFilter(e.target.value)}
          className="border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-gray-400"
        >
          <option value="all">All Tiers</option>
          {TIERS.map((t) => (
            <option key={t} value={t}>
              {t === 'high' ? 'Buy (High)' : t === 'medium' ? 'Hold (Medium)' : 'Sell (Low)'}
            </option>
          ))}
        </select>

        <div className="flex gap-1 border border-gray-300 rounded-md overflow-hidden text-sm">
          {MODEL_TYPES.map((m) => (
            <button
              key={m}
              onClick={() => setModelFilter(m)}
              className={`px-3 py-1.5 capitalize transition-colors ${
                modelFilter === m ? 'bg-gray-900 text-white' : 'bg-white text-gray-600 hover:bg-gray-100'
              }`}
            >
              {m}
            </button>
          ))}
        </div>

        <span className="ml-auto text-sm text-gray-400 self-center">
          {filtered.length} players
        </span>
      </div>

      {filtered.length === 0 ? (
        <div className="text-center py-20 text-gray-400">
          {players.length === 0
            ? 'No predictions found. Run ingest + train pipelines to populate data.'
            : 'No players match your filters.'}
        </div>
      ) : (
        <PlayerTable players={filtered} />
      )}
    </div>
  )
}

export const getServerSideProps: GetServerSideProps<Props> = async (ctx) => {
  const season = parseInt((ctx.query.season as string) ?? '2024', 10)
  const modelType = (ctx.query.model ?? 'seasonal') as string

  const [predsRes, seasonsRes] = await Promise.all([
    supabase
      .from('predictions')
      .select('player_id, season, prediction_type, breakout_prob, updated_at')
      .eq('season', season)
      .eq('prediction_type', modelType)
      .order('breakout_prob', { ascending: false }),
    supabase
      .from('player_seasons')
      .select('player_id, season, player_name, position, team')
      .eq('season', season),
  ])

  type PredRow    = Pick<Prediction,   'player_id' | 'season' | 'prediction_type' | 'breakout_prob' | 'updated_at'>
  type SeasonMeta = Pick<PlayerSeason, 'player_id' | 'season' | 'player_name' | 'position' | 'team'>

  const preds: PredRow[]         = predsRes.data ?? []
  const seasonRows: SeasonMeta[] = seasonsRes.data ?? []

  const metaById: Record<string, SeasonMeta> = {}
  for (const row of seasonRows) {
    metaById[row.player_id] = row
  }

  // risk_tier is always NULL in the DB (broken check constraint) — derive from prob.
  const deriveTier = (prob: number): string =>
    prob >= 0.65 ? 'high' : prob >= 0.40 ? 'medium' : 'low'

  const players: PlayerRow[] = preds
    .filter((p) => p.breakout_prob != null)
    .map((p) => {
      const meta = metaById[p.player_id]
      const prob = p.breakout_prob!
      return {
        player_id:    p.player_id,
        season:       p.season,
        player_name:  meta?.player_name  ?? p.player_id,
        position:     meta?.position     ?? '—',
        team:         meta?.team         ?? '—',
        breakout_prob: prob,
        risk_tier:    deriveTier(prob),
        updated_at:   p.updated_at,
      }
    })

  // Available seasons for a future season-switcher
  const availableSeasons = [2020, 2021, 2022, 2023, 2024]

  return { props: { players, season, availableSeasons } }
}
