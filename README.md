# NFL Skill Player Breakout & Regression Predictor

A cloud-based fantasy football analytics tool that predicts which skill-position players (RB, WR, TE) are likely to **break out or regress** in the upcoming season and week-to-week during the season. Framed as buy-low / sell-high intelligence for dynasty and redraft leagues.

---

## What it does

**Pre-season model** — runs once each September before Week 1. Uses multi-year usage trajectory, age curve position, draft capital, and contextual changes (new OC, QB, depth chart shifts) to classify each player as a breakout or regression candidate for the full season.

**In-season model** — updates every Tuesday after games finish. Tracks rolling snap share, target share, air yards, and red zone trends week-over-week to update each player's probability in real time.

Both modes write to the same `predictions` table and are surfaced in a unified frontend.

---

## Stack

| Layer | Tool | Cost |
|---|---|---|
| Data | nfl_data_py (nflverse) | Free |
| Storage | Supabase (Postgres + PostgREST) | Free tier |
| Orchestration | GitHub Actions (scheduled workflows) | Free tier |
| ML | XGBoost, SHAP, kNN comps | Free (CPU) |
| Real-time inference | Modal serverless functions | Free tier |
| API | Supabase PostgREST + Vercel API routes | Free tier |
| Frontend | Next.js on Vercel | Free tier |

---

## Repo structure

```
SundaySignals/
├── pipelines/
│   ├── features.py          # public API — re-exports build_seasonal_features, build_weekly_features
│   ├── seasonal.py          # build_seasonal_features + all seasonal helpers
│   ├── weekly.py            # build_weekly_features + all weekly helpers
│   ├── loaders.py           # nfl_data_py wrappers with normalised column names
│   ├── model.py             # shared XGBoost, SHAP, kNN, Supabase Storage helpers
│   ├── constants.py         # POSITIONS, breakout thresholds, DB column lists
│   ├── utils.py             # shared math utilities (three_point_slope, df_to_records)
│   ├── ingest_seasonal.py   # pulls nfl_data_py, upserts player_seasons
│   ├── ingest_weekly.py     # pulls weekly game data, upserts player_weeks
│   ├── train_seasonal.py    # XGBoost seasonal model, SHAP, kNN comps
│   ├── train_weekly.py      # XGBoost weekly model, updates predictions
│   ├── backtest.py          # walk-forward validation, writes model_performance
│   └── modal_inference.py   # Modal app for live inference
│
├── frontend/                # Next.js app (deployed on Vercel)
│   ├── vercel.json          # forces Next.js framework detection
│   ├── lib/
│   │   ├── supabase.ts      # Supabase JS client (anon key)
│   │   └── types.ts         # TypeScript interfaces + ML constants
│   ├── pages/
│   │   ├── index.tsx        # Player explorer — filterable table
│   │   ├── player/[id].tsx  # Player detail — trajectory, SHAP, comps, weight sliders
│   │   ├── accuracy.tsx     # Model accuracy — AUC per split, calibration chart
│   │   └── api/score.ts     # Vercel API route → Modal inference
│   └── components/
│       ├── PlayerTable.tsx
│       ├── ShapChart.tsx
│       ├── TrajectoryChart.tsx
│       ├── CompsCard.tsx
│       ├── WeightSliders.tsx
│       ├── AucTable.tsx
│       └── CalibrationChart.tsx
│
├── supabase/
│   └── schema.sql           # DDL for all 4 tables + RLS policies + indexes
│
├── overrides/
│   └── context_flags.csv    # hand-editable: player_id, season, oc_change, qb_change
│
├── .github/
│   └── workflows/
│       ├── ingest_seasonal.yml   # cron March + August
│       ├── ingest_weekly.yml     # cron Tuesday 6am, Sep–Jan
│       ├── retrain_seasonal.yml  # manual dispatch
│       └── retrain_weekly.yml    # triggered by ingest_weekly completion
│
├── tests/
│   ├── test_features.py     # unit tests for feature engineering (mocked loaders)
│   └── test_schema.py       # validates pipeline output against Supabase schema
│
├── requirements.txt
├── .env.example
└── README.md
```

---

## Setup

### 1. Supabase

1. Create a project at [supabase.com](https://supabase.com)
2. Open the **SQL Editor** and run `supabase/schema.sql` — creates all 4 tables, RLS policies, and indexes
3. Go to **Storage → New bucket** → name it `models`, set to **private**
4. Copy your **Project URL**, **service role key**, and **anon key** from Settings → API

### 2. Python environment

```bash
pip install -r requirements.txt
cp .env.example .env
# fill in SUPABASE_URL, SUPABASE_SERVICE_KEY, SUPABASE_ANON_KEY
```

### 3. Seed the database

Run these in order on first setup:

```bash
# Pull 2020–2024 historical data (~5 min, ~1GB PBP download on first run)
python pipelines/ingest_seasonal.py

# Train the pre-season model, compute SHAP + kNN comps, write predictions
python pipelines/train_seasonal.py

# Run walk-forward backtest, write model_performance rows
python pipelines/backtest.py --model-type seasonal
```

After this the frontend will show real player predictions.

### 4. Modal inference (for live weight sliders)

```bash
# Authenticate with Modal (one time)
modal setup

# Create the Supabase secret in Modal
modal secret create supabase-secrets \
  SUPABASE_URL=<your_url> \
  SUPABASE_SERVICE_KEY=<your_service_key>

# Deploy the inference endpoint — copy the printed URL
modal deploy pipelines/modal_inference.py
```

Add the printed URL as `MODAL_INFERENCE_URL` in Vercel environment variables.

### 5. Frontend (local dev)

```bash
cd frontend
npm install
cp .env.local.example .env.local
# fill in NEXT_PUBLIC_SUPABASE_URL, NEXT_PUBLIC_SUPABASE_ANON_KEY, MODAL_INFERENCE_URL
npm run dev   # http://localhost:3000
```

### 6. Vercel deployment

1. Import the repo at [vercel.com](https://vercel.com)
2. Set **Root Directory** to `frontend` in project Settings → General
3. Add environment variables in Settings → Environment Variables:

| Variable | Value | Notes |
|---|---|---|
| `NEXT_PUBLIC_SUPABASE_URL` | `https://<ref>.supabase.co` | Safe to expose — public project URL |
| `NEXT_PUBLIC_SUPABASE_ANON_KEY` | anon key from Supabase | Safe to expose — read-only via RLS |
| `MODAL_INFERENCE_URL` | printed by `modal deploy` | Server-side only, do not prefix with `NEXT_PUBLIC_` |

### 7. GitHub Actions secrets

Add in repo Settings → Secrets → Actions:

| Secret | Used by |
|---|---|
| `SUPABASE_URL` | All 4 workflows |
| `SUPABASE_SERVICE_KEY` | All 4 workflows |

---

## Data pipeline

### Seasonal ingest (runs twice: March + August)

Pulls 2020–present historical data via `nfl_data_py`. Computes all pre-season features and upserts to `player_seasons`. The August run captures OC/QB/roster changes before model training.

```bash
python pipelines/ingest_seasonal.py
python pipelines/ingest_seasonal.py --seasons 2022 2023 2024 --context-flags overrides/context_flags.csv
```

### Weekly ingest (runs every Tuesday, Sep–Jan)

Pulls the previous week's game data, computes rolling features, upserts to `player_weeks`. Retrain workflow fires automatically on completion.

```bash
python pipelines/ingest_weekly.py
python pipelines/ingest_weekly.py --season 2024 --week 12
```

---

## ML pipeline

### Pre-season model

- **Algorithm**: XGBoost binary classifier
- **Target**: `broke_out = 1` if fantasy PPG increased ≥30% YoY with ≥8 games played
- **Features**: age, age², 3yr target share slope, 3yr snap share slope, YPRR, WOPR, DOM, RACR, receiving EPA, draft capital, OC/QB change flags, team pass volume trend
- **Output**: breakout probability (0–100), SHAP feature importance, risk tier (Buy / Hold / Sell), top-5 historical comps via kNN

### In-season model

- **Algorithm**: XGBoost binary classifier, retrained weekly from scratch
- **Target**: `on_track_breakout = 1` based on current-season pace
- **Features**: rolling 3-week snap share delta, target share delta, air yards share, red zone targets, route participation, current vs prior season PPG

### Comp finder

kNN with cosine similarity on normalized feature vectors. Top-5 most similar player-seasons from 2000–present. Computed at prediction time and stored in Supabase — frontend reads rows directly.

---

## Backtesting

Walk-forward validation across three season splits:

| Split | Train | Predict | Evaluate |
|---|---|---|---|
| 1 | 2020–2021 | 2022 | vs actual 2022 |
| 2 | 2020–2022 | 2023 | vs actual 2023 |
| 3 | 2020–2023 | 2024 | vs actual 2024 (holdout) |

Metrics tracked per split: AUC-ROC, Precision@20, calibration error, comp accuracy.

```bash
python pipelines/backtest.py --model-type seasonal
python pipelines/backtest.py --model-type weekly
python pipelines/backtest.py --model-type both
```

---

## Real-time inference

When a user adjusts feature weights in the UI, `pages/api/score.ts` (Vercel) proxies the request to the Modal endpoint, which loads the serialized model from Supabase Storage and runs `predict_proba`. Latency ~100ms on CPU.

---

## Database schema

| Table | Description |
|---|---|
| `player_seasons` | One row per player per season. Pre-season features + ground truth labels. |
| `player_weeks` | One row per player per week. In-season features + rolling averages. |
| `predictions` | One row per player per prediction type. Scores, SHAP values, comps as JSONB. |
| `model_performance` | Backtest results per split per model. Used for accuracy page charts. |

RLS is enabled on all tables. The frontend uses the anon key (read-only). Pipelines use the service role key.

---

## GitHub Actions

| Workflow | Trigger | What it does |
|---|---|---|
| `ingest_seasonal.yml` | Cron Mar 1 + Aug 1 | Pulls historical data, upserts `player_seasons` |
| `ingest_weekly.yml` | Cron Tue 6am Sep–Jan | Pulls weekly data, upserts `player_weeks` |
| `retrain_seasonal.yml` | Manual dispatch | Trains seasonal model, runs backtest |
| `retrain_weekly.yml` | On `ingest_weekly` success | Trains weekly model, updates predictions |

---

## Tests

```bash
pip install pytest
pytest tests/ -v
```

All tests mock `nfl_data_py` loaders — no network or database calls required.

---

## License

MIT
