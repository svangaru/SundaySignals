// ---------------------------------------------------------------------------
// Supabase table shapes
// ---------------------------------------------------------------------------

export interface PlayerSeason {
  player_id: string
  season: number
  player_name: string | null
  position: string | null
  team: string | null
  age: number | null
  draft_number: number | null
  games: number | null
  targets: number | null
  receptions: number | null
  receiving_yards: number | null
  receiving_tds: number | null
  carries: number | null
  rushing_yards: number | null
  target_share: number | null
  air_yards_share: number | null
  wopr: number | null
  dom: number | null
  w8dom: number | null
  receiving_epa: number | null
  racr: number | null
  fantasy_points_ppr: number | null
  target_share_y1: number | null
  target_share_y2: number | null
  target_share_slope: number | null
  snap_share_y0: number | null
  snap_share_y1: number | null
  snap_share_y2: number | null
  snap_share_slope: number | null
  yprr: number | null
  team_pass_vol_trend: number | null
  oc_change: boolean | null
  qb_change: boolean | null
  fantasy_ppg: number | null
  broke_out: boolean | null
}

export interface PlayerWeek {
  player_id: string
  season: number
  week: number
  snap_pct: number | null
  targets: number | null
  target_share: number | null
  air_yards: number | null
  air_yards_share: number | null
  red_zone_targets: number | null
  route_participation: number | null
  ppr_points: number | null
  receiving_epa: number | null
  snap_pct_r3: number | null
  target_share_r3: number | null
  air_yards_r3: number | null
  snap_pct_delta_r3: number | null
  target_share_delta_r3: number | null
}

export interface Comp {
  player_id: string
  season: number
  player_name: string
  broke_out: boolean
  similarity: number
}

export interface Prediction {
  player_id: string
  season: number
  prediction_type: string  // DB column is "prediction_type", not "model_type"
  week: number | null
  buy_sell_score: number | null
  breakout_prob: number | null
  risk_tier: string | null  // always NULL in DB — derive from breakout_prob in UI
  direction: string | null  // always NULL in DB
  shap_values: Record<string, number> | null
  comps: Comp[] | null
  updated_at: string
}

export interface ModelPerformance {
  id: number
  model_type: string
  train_seasons: string    // e.g. "2020-2021" (was train_max_season: number)
  test_season: number
  auc_roc: number | null
  precision_at_20: number | null
  calibration_err: number | null  // note: not "calibration_error"
  comp_accuracy: number | null
  run_at: string           // note: not "created_at"
}

// ---------------------------------------------------------------------------
// Derived / UI types
// ---------------------------------------------------------------------------

// Merged prediction + player metadata for the explorer table
export interface PlayerRow {
  player_id: string
  season: number
  player_name: string
  position: string
  team: string
  breakout_prob: number
  risk_tier: string
  updated_at: string
}

// ---------------------------------------------------------------------------
// ML constants (mirrors pipelines/constants.py)
// ---------------------------------------------------------------------------

export const SEASONAL_FEATURES = [
  'age',
  'age_squared',
  'draft_number',
  'target_share',
  'target_share_y1',
  'target_share_y2',
  'target_share_slope',
  'snap_share_y0',
  'snap_share_y1',
  'snap_share_y2',
  'snap_share_slope',
  'air_yards_share',
  'wopr',
  'dom',
  'w8dom',
  'receiving_epa',
  'racr',
  'yprr',
  'team_pass_vol_trend',
  'oc_change',
  'qb_change',
] as const

export type SeasonalFeature = (typeof SEASONAL_FEATURES)[number]

// Feature display labels for the weight sliders UI
export const FEATURE_LABELS: Record<string, string> = {
  age: 'Age',
  age_squared: 'Age²',
  draft_number: 'Draft Capital',
  target_share: 'Target Share (Y0)',
  target_share_y1: 'Target Share (Y-1)',
  target_share_y2: 'Target Share (Y-2)',
  target_share_slope: 'Target Share Trend',
  snap_share_y0: 'Snap Share (Y0)',
  snap_share_y1: 'Snap Share (Y-1)',
  snap_share_y2: 'Snap Share (Y-2)',
  snap_share_slope: 'Snap Share Trend',
  air_yards_share: 'Air Yards Share',
  wopr: 'WOPR',
  dom: 'DOM',
  w8dom: 'W8DOM',
  receiving_epa: 'Receiving EPA',
  racr: 'RACR',
  yprr: 'YPRR',
  team_pass_vol_trend: 'Team Pass Vol Trend',
  oc_change: 'OC Change',
  qb_change: 'QB Change',
}

// ---------------------------------------------------------------------------
// API types
// ---------------------------------------------------------------------------

export interface ScoreRequest {
  features: Record<string, number>
  model_type: 'seasonal' | 'weekly'
  feature_weights?: Record<string, number>
}

export interface ScoreResponse {
  breakout_prob: number
  risk_tier: string
  shap_values: Record<string, number>
  model_type: string
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

export const TIER_COLORS: Record<string, { bg: string; text: string; border: string }> = {
  high:   { bg: 'bg-green-100',  text: 'text-green-800',  border: 'border-green-200' },
  medium: { bg: 'bg-amber-100',  text: 'text-amber-800',  border: 'border-amber-200' },
  low:    { bg: 'bg-red-100',    text: 'text-red-800',    border: 'border-red-200'   },
}

export const TIER_LABELS: Record<string, string> = {
  high:   'Buy',
  medium: 'Hold',
  low:    'Sell',
}

export function fmtPct(n: number | null | undefined, decimals = 1): string {
  if (n == null) return '—'
  return `${(n * 100).toFixed(decimals)}%`
}

export function fmtProb(n: number | null | undefined): string {
  if (n == null) return '—'
  return `${Math.round(n * 100)}`
}
