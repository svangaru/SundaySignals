# pipelines/

Python package for all data ingestion, feature engineering, model training, and inference.

---

## File map

```
pipelines/
├── features.py          ← start here — public API, re-exports the two build functions
│
├── seasonal.py          ← build_seasonal_features() + helpers (pre-season model)
├── weekly.py            ← build_weekly_features() + helpers (in-season model)
│
├── loaders.py           ← nfl_data_py wrappers, column name normalisation
├── constants.py         ← POSITIONS, breakout thresholds, DB column lists
├── utils.py             ← df_to_records(), three_point_slope()
│
├── ingest_seasonal.py   ← calls build_seasonal_features → upserts player_seasons
├── ingest_weekly.py     ← calls build_weekly_features → upserts player_weeks
│
├── train_seasonal.py    ← trains XGBoost pre-season model, writes predictions
├── train_weekly.py      ← trains XGBoost in-season model, updates predictions
├── backtest.py          ← walk-forward validation, writes model_performance
└── modal_inference.py   ← Modal serverless function for live re-scoring
```

### Dependency order (nothing imports from a file below it)

```
constants
    ↑
loaders   utils
    ↑       ↑
seasonal  weekly
    ↑       ↑
       features          ← everything outside this package imports from here
    ↑       ↑
ingest_seasonal  ingest_weekly
    ↑                ↑
    train_seasonal  train_weekly  backtest  modal_inference
```

---

## Feature engineering

The two public functions are the only entry points needed outside this package:

```python
from pipelines.features import build_seasonal_features, build_weekly_features

# Pre-season: one row per player per season
df = build_seasonal_features([2022, 2023, 2024])

# In-season: one row per player per week
df = build_weekly_features(2024, weeks=[1, 2, 3])
```

Both return DataFrames whose columns match the Supabase schema exactly (`SEASONAL_COLS` / `WEEKLY_COLS` in `constants.py`).

### What each module computes

**`seasonal.py`**
| Feature | Source | Notes |
|---|---|---|
| `age`, `draft_number` | `loaders.load_roster_meta` | Earliest week roster per season |
| `target_share_y0/y1/y2`, `target_share_slope` | `loaders.load_seasonal` | 3-year lag + linear slope |
| `snap_share_y0/y1/y2`, `snap_share_slope` | `loaders.load_snap_counts` | Season-mean snap %, 3-year lag |
| `yprr` | PBP + snap counts | rec yards / routes run (falls back to targets) |
| `team_pass_vol_trend` | PBP | Slope of team pass attempts over prior 3 seasons |
| `oc_change`, `qb_change` | `overrides/context_flags.csv` | Hand-edited boolean flags |
| `fantasy_ppg` | Derived | `fantasy_points_ppr / games` |
| `broke_out` | Derived | PPG up ≥30% YoY with ≥8 games played |

**`weekly.py`**
| Feature | Source | Notes |
|---|---|---|
| `snap_pct` | `loaders.load_snap_counts` | Offense snap % for the week |
| `red_zone_targets` | PBP | Pass targets with `yardline_100 ≤ 20` |
| `route_participation` | Snap counts | `routes_ran / offense_snaps` |
| `snap_pct_r3`, `target_share_r3`, `air_yards_r3` | Rolling | 3-week rolling mean |
| `snap_pct_delta_r3`, `target_share_delta_r3` | Rolling | Current week minus prior 3-week mean |

---

## Ingest scripts

Both scripts read credentials from environment variables (see root `.env.example`).

```bash
# Seasonal — run from project root
python pipelines/ingest_seasonal.py
python pipelines/ingest_seasonal.py --seasons 2022 2023 2024
python pipelines/ingest_seasonal.py --seasons 2024 --context-flags overrides/context_flags.csv

# Weekly — defaults to current season, all completed weeks
python pipelines/ingest_weekly.py
python pipelines/ingest_weekly.py --season 2024 --week 12   # specific week
python pipelines/ingest_weekly.py --season 2023             # backfill full season
```

### Environment variables

| Variable | Used by | Where it comes from |
|---|---|---|
| `SUPABASE_URL` | Both ingest scripts | `.env` locally, GitHub Actions secret in CI |
| `SUPABASE_SERVICE_KEY` | Both ingest scripts | Same — service role key, never expose to frontend |

`load_dotenv()` looks for `.env` starting in the working directory and walks up. In GitHub Actions, no `.env` file exists — the variables are injected directly as environment secrets.

---

## Breakout label definition

```
broke_out = True  if  (fantasy_ppg_this_season / fantasy_ppg_last_season) >= 1.30
                  AND games_played >= 8
```

Defined in `constants.py` as `BREAKOUT_PPG_THRESHOLD = 0.30` and `BREAKOUT_MIN_GAMES = 8`. Change it there and it propagates everywhere automatically.
