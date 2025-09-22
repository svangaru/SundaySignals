
create table if not exists players (
  player_id text primary key,
  position text not null,
  team text,
  name text not null
);

create table if not exists schedule (
  season int not null,
  week int not null,
  team text not null,
  opp text not null,
  home boolean not null,
  primary key (season, week, team)
);

create table if not exists defense_vs_pos (
  season int not null,
  week int not null,
  team text not null,
  position text not null,
  dvp numeric not null,
  primary key (season, week, team, position)
);

create table if not exists odds (
  season int not null,
  week int not null,
  game_id text not null,
  team text not null,
  opp text not null,
  spread numeric,
  moneyline int,
  total numeric,
  updated_at timestamptz not null default now(),
  primary key (season, week, game_id, team)
);

create table if not exists news (
  player_id text not null references players(player_id) on delete cascade,
  ts timestamptz not null,
  headline text not null,
  primary key (player_id, ts)
);

create table if not exists pred_cache (
  pk text not null,
  sk text not null,
  p50 numeric not null,
  lo numeric not null,
  hi numeric not null,
  valid_until timestamptz not null,
  primary key (pk, sk)
);

create table if not exists user_leagues (
  user_id uuid not null,
  league_id text not null,
  league_name text,
  league_avatar text,
  primary key (user_id, league_id)
);

create table if not exists rosters (
  league_id text not null,
  user_id uuid not null,
  player_id text not null references players(player_id) on delete cascade,
  slot text not null,
  season int not null,
  week int not null,
  primary key (league_id, user_id, player_id, season, week)
);

create table if not exists transactions (
  league_id text not null,
  ts timestamptz not null,
  type text not null,
  payload jsonb not null,
  primary key (league_id, ts)
);

create table if not exists model_runs (
  run_id uuid primary key default gen_random_uuid(),
  season int,
  week int,
  stage text not null,
  metrics jsonb,
  status text not null default 'started',
  started_at timestamptz not null default now(),
  ended_at timestamptz
);

create table if not exists model_registry (
  model_id text primary key,
  label text,
  metrics jsonb,
  is_prod boolean not null default false,
  prod_week int,
  prod_season int
);

create table if not exists waiver_suggestions (
  league_id text not null,
  season int not null,
  week int not null,
  player_id text not null references players(player_id) on delete cascade,
  evor numeric not null,
  reason text,
  created_at timestamptz not null default now(),
  primary key (league_id, season, week, player_id)
);

create index if not exists idx_pred_cache_valid_until on pred_cache(valid_until);
create index if not exists idx_news_ts on news(ts desc);
