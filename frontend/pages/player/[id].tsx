import type { GetServerSideProps } from 'next'
import { useState, useCallback } from 'react'
import Link from 'next/link'
import { supabase } from '@/lib/supabase'
import type {
  PlayerSeason,
  PlayerWeek,
  Prediction,
  ScoreResponse,
} from '@/lib/types'
import {
  SEASONAL_FEATURES,
  TIER_COLORS,
  TIER_LABELS,
  fmtProb,
} from '@/lib/types'
import TrajectoryChart from '@/components/TrajectoryChart'
import ShapChart from '@/components/ShapChart'
import CompsCard from '@/components/CompsCard'
import WeightSliders from '@/components/WeightSliders'

interface Props {
  playerId: string
  seasons: PlayerSeason[]
  prediction: Prediction | null
  weeks: PlayerWeek[]
}

function buildBaseFeatures(row: PlayerSeason): Record<string, number> {
  const age = row.age ?? 25
  const features: Record<string, number> = {
    age,
    age_squared:         age * age,
    draft_number:        row.draft_number       ?? 200,
    target_share:        row.target_share        ?? 0,
    target_share_y1:     row.target_share_y1     ?? 0,
    target_share_y2:     row.target_share_y2     ?? 0,
    target_share_slope:  row.target_share_slope  ?? 0,
    snap_share_y0:       row.snap_share_y0       ?? 0,
    snap_share_y1:       row.snap_share_y1       ?? 0,
    snap_share_y2:       row.snap_share_y2       ?? 0,
    snap_share_slope:    row.snap_share_slope     ?? 0,
    air_yards_share:     row.air_yards_share      ?? 0,
    wopr:                row.wopr                ?? 0,
    dom:                 row.dom                 ?? 0,
    w8dom:               row.w8dom               ?? 0,
    receiving_epa:       row.receiving_epa        ?? 0,
    racr:                row.racr                ?? 0,
    yprr:                row.yprr                ?? 0,
    team_pass_vol_trend: row.team_pass_vol_trend  ?? 0,
    oc_change:           row.oc_change            ? 1 : 0,
    qb_change:           row.qb_change            ? 1 : 0,
  }
  return features
}

export default function PlayerDetailPage({ playerId, seasons, prediction, weeks }: Props) {
  const latest  = seasons.find((s) => s.season === Math.max(...seasons.map((x) => x.season)))
  const player  = latest ?? seasons[0]

  const [liveScore, setLiveScore]   = useState<ScoreResponse | null>(null)
  const [scoring, setScoring]       = useState(false)
  const [scoreError, setScoreError] = useState<string | null>(null)

  const baseFeatures = player ? buildBaseFeatures(player) : {}

  const handleRescore = useCallback(async (weights: Record<string, number>) => {
    if (!player) return
    setScoring(true)
    setScoreError(null)
    try {
      const res = await fetch('/api/score', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          features:        baseFeatures,
          model_type:      'seasonal',
          feature_weights: weights,
        }),
      })
      if (!res.ok) {
        const e = await res.json()
        setScoreError(e.error ?? 'Scoring failed')
        return
      }
      const data: ScoreResponse = await res.json()
      setLiveScore(data)
    } catch {
      setScoreError('Could not reach inference service')
    } finally {
      setScoring(false)
    }
  }, [player, baseFeatures])

  const displayProb = liveScore?.breakout_prob ?? prediction?.breakout_prob
  const displayTier = liveScore?.risk_tier     ?? prediction?.risk_tier ?? 'low'
  const displayShap = liveScore?.shap_values   ?? prediction?.shap_values
  const tierStyle   = TIER_COLORS[displayTier] ?? TIER_COLORS.low

  if (!player) {
    return <p className="text-gray-500">Player not found.</p>
  }

  return (
    <div>
      {/* Back */}
      <Link href="/" className="text-sm text-gray-400 hover:text-gray-700 mb-4 inline-block">
        ← Back to Explorer
      </Link>

      {/* Header */}
      <div className="flex items-baseline gap-4 mb-6">
        <h1 className="text-3xl font-bold text-gray-900">
          {player.player_name ?? playerId}
        </h1>
        <span className="text-gray-500 text-lg">
          {player.position} · {player.team} · Age {player.age?.toFixed(0) ?? '—'}
        </span>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">

        {/* Left column — charts */}
        <div className="lg:col-span-2 space-y-6">
          <div className="bg-white rounded-xl border border-gray-200 p-5">
            <h2 className="text-sm font-semibold text-gray-600 uppercase tracking-wide mb-4">
              Usage Trajectory
            </h2>
            <TrajectoryChart seasons={seasons} />
          </div>

          {displayShap && Object.keys(displayShap).length > 0 && (
            <div className="bg-white rounded-xl border border-gray-200 p-5">
              <h2 className="text-sm font-semibold text-gray-600 uppercase tracking-wide mb-4">
                Feature Impact (SHAP)
              </h2>
              <ShapChart shap={displayShap} />
            </div>
          )}
        </div>

        {/* Right column — score + comps + sliders */}
        <div className="space-y-6">

          {/* Score card */}
          <div className={`rounded-xl border p-5 ${tierStyle.bg} ${tierStyle.border}`}>
            <p className={`text-xs font-semibold uppercase tracking-widest mb-1 ${tierStyle.text}`}>
              Breakout Probability
            </p>
            <div className="flex items-end gap-2">
              <span className={`text-5xl font-black ${tierStyle.text}`}>
                {displayProb != null ? fmtProb(displayProb) : '—'}
              </span>
              <span className={`text-xl mb-1 ${tierStyle.text}`}>%</span>
            </div>
            <span className={`inline-block mt-2 px-2 py-0.5 rounded text-xs font-semibold ${tierStyle.bg} ${tierStyle.text} border ${tierStyle.border}`}>
              {TIER_LABELS[displayTier] ?? displayTier}
            </span>
            {liveScore && (
              <p className="text-xs mt-2 text-gray-500 italic">Live re-score active</p>
            )}
            {scoring && (
              <p className="text-xs mt-2 text-gray-400 animate-pulse">Recalculating…</p>
            )}
            {scoreError && (
              <p className="text-xs mt-2 text-red-600">{scoreError}</p>
            )}
          </div>

          {/* Comps */}
          {prediction?.comps && prediction.comps.length > 0 && (
            <div className="bg-white rounded-xl border border-gray-200 p-5">
              <h2 className="text-sm font-semibold text-gray-600 uppercase tracking-wide mb-3">
                Historical Comps
              </h2>
              <CompsCard comps={prediction.comps} />
            </div>
          )}

          {/* Weight sliders */}
          <div className="bg-white rounded-xl border border-gray-200 p-5">
            <h2 className="text-sm font-semibold text-gray-600 uppercase tracking-wide mb-1">
              Feature Weights
            </h2>
            <p className="text-xs text-gray-400 mb-4">
              Adjust multipliers and re-score live via Modal inference.
            </p>
            <WeightSliders
              features={SEASONAL_FEATURES as unknown as string[]}
              onRescore={handleRescore}
              disabled={scoring}
            />
          </div>
        </div>
      </div>
    </div>
  )
}

export const getServerSideProps: GetServerSideProps<Props> = async (ctx) => {
  const playerId = ctx.params?.id as string
  const season   = parseInt((ctx.query.season as string) ?? '2024', 10)

  const [seasonsRes, predRes, weeksRes] = await Promise.all([
    supabase
      .from('player_seasons')
      .select('*')
      .eq('player_id', playerId)
      .order('season', { ascending: true }),
    supabase
      .from('predictions')
      .select('*')
      .eq('player_id', playerId)
      .eq('season', season)
      .eq('model_type', 'seasonal')
      .single(),
    supabase
      .from('player_weeks')
      .select('*')
      .eq('player_id', playerId)
      .eq('season', season)
      .order('week', { ascending: true }),
  ])

  return {
    props: {
      playerId,
      seasons:    seasonsRes.data ?? [],
      prediction: predRes.data    ?? null,
      weeks:      weeksRes.data   ?? [],
    },
  }
}
