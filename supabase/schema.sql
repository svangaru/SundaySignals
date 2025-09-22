-- =========================================================
-- SundaySignals schema.sql
-- =========================================================

-- Extensions (UUIDs, crypto helpers)
create extension if not exists pgcrypto;

-- -----------------------------
-- Core reference data
-- -----------------------------
create table if not exists public.players (
  player_id text primary key,                -- Sleeper ID (recommended) or your mapping
  position  text not null,                   -- e.g., QB/RB/WR/TE/DEF/K
  team      text,                            -- e.g., NYJ
  name      text not null
);

create table if not exists public.schedule (
  season int  not null,
  week   int  not null check (week between 1 and 23),
  team   text not null,
  opp    text not null,
  home   boolean not null,                   -- true if home, false if away
  primary key (season, week, team)
);

create index if not exists idx_schedule_season_week on public.schedule (season, week);

create table if not exists public.defense_vs_pos (
  season   int  not null,
  week     int  not null check (week between 1 and 23),
  team     text not null,
  position text not null,
  dvp      numeric not null,                 -- your definition (e.g., fantasy pts allowed)
  primary key (season, week, team, position)
);

create index if not exists idx_dvp_season_week_pos on public.defense_vs_pos (season, week, position);

create table if not exists public.odds (
  season     int  not null,
  week       int  not null check (week between 1 and 23),
  game_id    text not null,
  team       text not null,
  opp        text not null,
  spread     numeric,                        -- from the POV of 'team'
  moneyline  int,                            -- American odds for 'team'
  total      numeric,
  updated_at timestamptz not null default now(),
  primary key (season, week, game_id, team)
);

create index if not exists idx_odds_season_week on public.odds (season, week);
create index if not exists idx_odds_game on public.odds (game_id);

create table if not exists public.news (
  player_id text not null references public.players(player_id) on delete cascade,
  ts        timestamptz not null,
  headline  text not null,
  primary key (player_id, ts)
);

create index if not exists idx_news_ts on public.news (ts desc);

-- -----------------------------
-- Serving / cache
-- -----------------------------
create table if not exists public.pred_cache (
  pk          text not null,                 -- e.g., 'season#2025#week#3'
  sk          text not null,                 -- e.g., 'player#<id>'
  p50         numeric not null,              -- median prediction
  lo          numeric not null,              -- conformal lower
  hi          numeric not null,              -- conformal upper
  valid_until timestamptz not null,
  primary key (pk, sk)
);

create index if not exists idx_pred_cache_valid_until on public.pred_cache (valid_until);

-- -----------------------------
-- User / league integration
-- -----------------------------
create table if not exists public.user_leagues (
  user_id       uuid not null,
  league_id     text not null,
  league_name   text,
  league_avatar text,
  primary key (user_id, league_id)
);

create table if not exists public.rosters (
  league_id text not null,
  user_id   uuid not null,
  player_id text not null references public.players(player_id) on delete cascade,
  slot      text not null,                   -- e.g., QB/RB/WR/TE/FLEX/BN
  season    int  not null,
  week      int  not null check (week between 1 and 23),
  primary key (league_id, user_id, player_id, season, week)
);

create table if not exists public.transactions (
  league_id text not null,
  ts        timestamptz not null,
  type      text not null,                   -- e.g., add/drop/trade
  payload   jsonb not null,
  primary key (league_id, ts)
);

create index if not exists idx_transactions_ts on public.transactions (ts desc);

-- -----------------------------
-- MLOps / runs & registry
-- -----------------------------
create table if not exists public.model_runs (
  run_id     uuid primary key default gen_random_uuid(),
  season     int,
  week       int,
  stage      text not null,                  -- e.g., ingest/build/train/infer/validate/backtest
  metrics    jsonb,                          -- free-form metrics per stage
  status     text not null default 'started',-- started/success/failed
  started_at timestamptz not null default now(),
  ended_at   timestamptz
);

create index if not exists idx_model_runs_stage_time on public.model_runs (stage, started_at desc);

create table if not exists public.model_registry (
  model_id     text primary key,             -- stable identifier for the trained model
  label        text,                         -- human-friendly label
  metrics      jsonb,                        -- summary metrics (MAE, pinball, coverage, etc.)
  is_prod      boolean not null default false,
  prod_week    int,
  prod_season  int
);

-- -----------------------------
-- Waiver suggestions (optional)
-- -----------------------------
create table if not exists public.waiver_suggestions (
  league_id text not null,
  season    int  not null,
  week      int  not null check (week between 1 and 23),
  player_id text not null references public.players(player_id) on delete cascade,
  evor      numeric not null,                -- expected value over replacement
  reason    text,
  created_at timestamptz not null default now(),
  primary key (league_id, season, week, player_id)
);

create index if not exists idx_waivers_created_at on public.waiver_suggestions (created_at desc);

-- -----------------------------
-- (Optional) Implied probabilities view from moneyline
-- -----------------------------
create or replace view public.odds_implied as
select
  season, week, game_id, team, opp, spread, moneyline, total, updated_at,
  case
    when moneyline is null then null
    when moneyline > 0 then 100.0 / (moneyline + 100.0)
    else (-moneyline) / ((-moneyline) + 100.0)
  end as implied_win_prob
from public.odds;

-- -----------------------------
-- PostgREST cache refresh (run after DDL changes)
-- -----------------------------
-- In the SQL editor, you can run this to force the REST API to see new tables:
-- select pg_notify('pgrst', 'reload schema');
