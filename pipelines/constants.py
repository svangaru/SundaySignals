"""
Shared constants for all pipeline modules.
"""

POSITIONS = ["RB", "WR", "TE"]

# Breakout label definition
BREAKOUT_PPG_THRESHOLD = 0.30  # ≥30% YoY improvement in fantasy PPG
BREAKOUT_MIN_GAMES = 8         # must play at least this many games

# Column lists must match DB schema exactly
SEASONAL_COLS = [
    "player_id", "season", "player_name", "position", "team",
    "age", "draft_number",
    "games", "targets", "receptions", "receiving_yards", "receiving_tds",
    "carries", "rushing_yards",
    "target_share", "air_yards_share", "wopr", "dom", "w8dom",
    "receiving_epa", "racr", "fantasy_points_ppr",
    "target_share_y1", "target_share_y2", "target_share_slope",
    "snap_share_y0", "snap_share_y1", "snap_share_y2", "snap_share_slope",
    "yprr", "team_pass_vol_trend",
    "oc_change", "qb_change",
    "fantasy_ppg", "broke_out",
]

WEEKLY_COLS = [
    "player_id", "season", "week",
    "snap_pct", "targets", "target_share",
    "air_yards", "air_yards_share",
    "red_zone_targets", "route_participation",
    "ppr_points", "receiving_epa",
    "snap_pct_r3", "target_share_r3", "air_yards_r3",
    "snap_pct_delta_r3", "target_share_delta_r3",
]

# ---------------------------------------------------------------------------
# ML feature lists — subset of *_COLS used as model inputs
# age_squared and ppr_points_vs_prior are derived at train time (not in DB)
# ---------------------------------------------------------------------------

SEASONAL_FEATURES: list[str] = [
    "age",
    "age_squared",
    "draft_number",
    "target_share",
    "target_share_y1",
    "target_share_y2",
    "target_share_slope",
    "snap_share_y0",
    "snap_share_y1",
    "snap_share_y2",
    "snap_share_slope",
    "air_yards_share",
    "wopr",
    "dom",
    "w8dom",
    "receiving_epa",
    "racr",
    "yprr",
    "team_pass_vol_trend",
    "oc_change",
    "qb_change",
]

WEEKLY_FEATURES: list[str] = [
    "snap_pct_delta_r3",
    "target_share_delta_r3",
    "air_yards_share",
    "air_yards_r3",
    "red_zone_targets",
    "route_participation",
    "ppr_points",
    "ppr_points_vs_prior",
]

SEASONAL_TARGET = "broke_out"
WEEKLY_TARGET   = "on_track_breakout"

# Risk tier probability thresholds
RISK_TIER_HIGH = 0.65   # p ≥ 0.65 → high
RISK_TIER_MED  = 0.40   # 0.40 ≤ p < 0.65 → medium; below → low

# Supabase Storage
MODEL_BUCKET        = "models"
SEASONAL_MODEL_PATH = "seasonal/latest.joblib"
WEEKLY_MODEL_PATH   = "weekly/latest.joblib"

# Walk-forward backtest windows: (train_max_season, test_season)
BACKTEST_SPLITS: list[tuple[int, int]] = [
    (2021, 2022),
    (2022, 2023),
    (2023, 2024),
]
