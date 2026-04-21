-- =============================================================================
-- SundaySignals — Supabase Schema
-- Run this in the Supabase SQL Editor for project qizdcrdzplngqzbricgw
--
-- NOTE: This file reflects the ACTUAL schema in the live database.
-- The predictions and model_performance tables differ from the original design:
--   - predictions uses "prediction_type" (not "model_type")
--   - predictions.risk_tier and .direction have check constraints that block
--     all non-null values — always write NULL for these columns
--   - model_performance uses a serial PK, train_seasons (text), calibration_err,
--     and run_at instead of the original column names
-- =============================================================================

-- ---------------------------------------------------------------------------
-- player_seasons
-- One row per player per season.
-- Populated by ingest_seasonal.py (March + August).
-- broke_out is the ground-truth label used by train_seasonal.py.
-- NOTE: player_name is NOT returned by import_seasonal_data(); it remains null
--       unless backfilled via a join to import_weekly_data() during ingest.
-- ---------------------------------------------------------------------------
create table if not exists player_seasons (
    player_id            text        not null,
    season               smallint    not null,

    -- Identity
    player_name          text,
    position             text,
    team                 text,

    -- Bio / draft capital
    age                  real,
    draft_number         smallint,   -- smallint in live DB (schema.sql had real)

    -- Season counting stats
    games                smallint,
    targets              smallint,
    receptions           smallint,
    receiving_yards      smallint,   -- smallint in live DB (schema.sql had real)
    receiving_tds        smallint,
    carries              smallint,
    rushing_yards        smallint,   -- smallint in live DB (schema.sql had real)

    -- Usage efficiency (nfl_data_py seasonal)
    target_share         real,
    air_yards_share      real,
    wopr                 real,
    dom                  real,
    w8dom                real,
    receiving_epa        real,
    racr                 real,
    fantasy_points_ppr   real,

    -- 3-year target share trend
    target_share_y1      real,
    target_share_y2      real,
    target_share_slope   real,

    -- 3-year snap share trend
    snap_share_y0        real,
    snap_share_y1        real,
    snap_share_y2        real,
    snap_share_slope     real,

    -- PBP-derived
    yprr                 real,
    team_pass_vol_trend  real,

    -- Context flags (overrides/context_flags.csv)
    oc_change            boolean,
    qb_change            boolean,

    -- Derived label features
    fantasy_ppg          real,
    broke_out            boolean,

    primary key (player_id, season)
);

-- ---------------------------------------------------------------------------
-- player_weeks
-- One row per player per week.
-- Populated by ingest_weekly.py every Tuesday during the NFL season.
-- ---------------------------------------------------------------------------
create table if not exists player_weeks (
    player_id              text     not null,
    season                 smallint not null,
    week                   smallint not null,

    -- Raw weekly stats
    snap_pct               real,
    targets                smallint,
    target_share           real,
    air_yards              real,
    air_yards_share        real,

    -- PBP-derived
    red_zone_targets       smallint,
    route_participation    real,

    -- Fantasy / EPA
    ppr_points             real,
    receiving_epa          real,

    -- Rolling 3-week averages
    snap_pct_r3            real,
    target_share_r3        real,
    air_yards_r3           real,

    -- Rolling 3-week deltas (current - 3 weeks ago)
    snap_pct_delta_r3      real,
    target_share_delta_r3  real,

    primary key (player_id, season, week)
);

-- ---------------------------------------------------------------------------
-- predictions
-- One row per player per prediction_type per season.
-- Written by train_seasonal.py and train_weekly.py.
-- shap_values and comps are JSONB — frontend reads them directly.
--
-- IMPORTANT constraints in the live DB:
--   - risk_tier has a CHECK constraint that blocks all non-null values
--     (the constraint is malformed). Always write NULL; derive tier from
--     breakout_prob in the frontend (≥0.65 → high, ≥0.40 → medium, else low).
--   - direction has the same issue. Always write NULL.
-- ---------------------------------------------------------------------------
create table if not exists predictions (
    player_id        text        not null,
    prediction_type  text        not null,  -- 'seasonal' | 'weekly'
    season           smallint    not null,
    week             smallint,              -- null for seasonal predictions

    buy_sell_score   smallint,              -- int(round(breakout_prob * 100))
    breakout_prob    real,                  -- 0.0–1.0
    risk_tier        text,                  -- always NULL (see constraint note above)
    direction        text,                  -- always NULL (see constraint note above)
    shap_values      jsonb,                 -- {feature: shap_value, ...}
    comps            jsonb,                 -- [{player_id, player_name, season, broke_out, similarity}, ...]

    updated_at       timestamptz default now(),

    primary key (player_id, prediction_type, season)
);

-- ---------------------------------------------------------------------------
-- model_performance
-- Appended each time backtest.py runs — no unique constraint.
-- Written by backtest.py, read by the frontend accuracy page.
-- ---------------------------------------------------------------------------
create table if not exists model_performance (
    id               serial      primary key,  -- auto-increment, no natural PK
    model_type       text        not null,     -- 'seasonal' | 'weekly'
    train_seasons    text        not null,     -- e.g. "2020-2021", "2020-2022"
    test_season      smallint,

    auc_roc          real,
    precision_at_20  real,
    calibration_err  real,                     -- note: not "calibration_error"
    comp_accuracy    real,

    run_at           timestamptz default now() -- note: not "created_at"
);

-- =============================================================================
-- Row Level Security
-- All tables: service role key has full access (bypasses RLS).
-- Frontend anon key gets read-only access.
-- =============================================================================

alter table player_seasons    enable row level security;
alter table player_weeks      enable row level security;
alter table predictions       enable row level security;
alter table model_performance enable row level security;

-- Public read policies (anon key can SELECT, cannot INSERT/UPDATE/DELETE)
create policy "public read player_seasons"
    on player_seasons for select using (true);

create policy "public read player_weeks"
    on player_weeks for select using (true);

create policy "public read predictions"
    on predictions for select using (true);

create policy "public read model_performance"
    on model_performance for select using (true);

-- =============================================================================
-- Indexes — speed up common frontend query patterns
-- =============================================================================

-- Player explorer: filter by position, season; sort by breakout_prob
-- (risk_tier index removed — column is always NULL in live DB)
create index if not exists idx_predictions_season_prob
    on predictions (season, prediction_type, breakout_prob desc);

-- Player detail: join player_seasons on (player_id, season)
create index if not exists idx_player_seasons_player
    on player_seasons (player_id, season);

-- Weekly time-series: all weeks for a player in a season
create index if not exists idx_player_weeks_player_season
    on player_weeks (player_id, season, week);

-- Model accuracy page: filter by model_type
create index if not exists idx_model_performance_type
    on model_performance (model_type, test_season);

-- =============================================================================
-- Supabase Storage bucket for serialized model artifacts
-- Create via the Storage UI: name "models", type Standard, private.
-- =============================================================================

-- insert into storage.buckets (id, name, public)
-- values ('models', 'models', false)
-- on conflict (id) do nothing;
