-- =============================================================================
-- SundaySignals — Supabase Schema
-- Run this in the Supabase SQL Editor for project qizdcrdzplngqzbricgw
-- =============================================================================

-- ---------------------------------------------------------------------------
-- player_seasons
-- One row per player per season.
-- Populated by ingest_seasonal.py (March + August).
-- broke_out is the ground-truth label used by train_seasonal.py.
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
    draft_number         real,

    -- Season counting stats
    games                smallint,
    targets              smallint,
    receptions           smallint,
    receiving_yards      real,
    receiving_tds        smallint,
    carries              smallint,
    rushing_yards        real,

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

    -- Context flags (overrides/context_flags.csv + PFR)
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
-- One row per player per model_type.
-- Written by train_seasonal.py and train_weekly.py.
-- shap_values and comps are JSONB — frontend reads them directly.
-- ---------------------------------------------------------------------------
create table if not exists predictions (
    player_id      text        not null,
    season         smallint    not null,
    model_type     text        not null,  -- 'seasonal' | 'weekly'

    breakout_prob  real,                  -- 0.0–1.0
    risk_tier      text,                  -- 'high' | 'medium' | 'low'
    shap_values    jsonb,                 -- {feature: shap_value, ...}
    comps          jsonb,                 -- [{player_id, player_name, season, broke_out, similarity}, ...]

    updated_at     timestamptz default now(),

    primary key (player_id, season, model_type)
);

-- ---------------------------------------------------------------------------
-- model_performance
-- One row per model_type per backtest split.
-- Written by backtest.py, read by the frontend accuracy page.
-- ---------------------------------------------------------------------------
create table if not exists model_performance (
    model_type         text     not null,   -- 'seasonal' | 'weekly'
    train_max_season   smallint not null,
    test_season        smallint not null,

    auc_roc            real,
    precision_at_20    real,
    calibration_error  real,
    comp_accuracy      real,

    created_at         timestamptz default now(),

    primary key (model_type, train_max_season, test_season)
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

-- Player explorer: filter by position, season, risk_tier; sort by breakout_prob
create index if not exists idx_predictions_season_tier
    on predictions (season, risk_tier, breakout_prob desc);

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
-- Run this separately in the Supabase SQL Editor or via the Storage UI.
-- =============================================================================

-- insert into storage.buckets (id, name, public)
-- values ('models', 'models', false)
-- on conflict (id) do nothing;

-- Allow service role to read/write the models bucket (RLS on storage is
-- handled automatically when using the service role key in pipelines).
