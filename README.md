# NFL Skill Player Breakout & Regression Predictor

A cloud-based fantasy football analytics tool that predicts which skill-position players (RB, WR, TE) are likely to **break out or regress** in the upcoming season and week-to-week during the season. Framed as buy-low / sell-high intelligence for dynasty and redraft leagues.

---

## What it does

The tool runs two distinct prediction modes:

**Pre-season model** — runs once each September before Week 1. Uses multi-year usage trajectory, age curve position, draft capital, and contextual changes (new OC, QB, depth chart shifts) to classify each player as a breakout or regression candidate for the full season. Dynasty and redraft use case.

**In-season model** — updates every Tuesday after games finish. Tracks rolling snap share, target share, air yards, and red zone trends week-over-week to update each player's probability in real time. Waiver wire and trade deadline use case.

Both modes write to the same `predictions` table and are surfaced in a unified frontend.

---

## Stack

| Layer | Tool | Cost |
|---|---|---|
| Data | nfl_data_py (nflverse), Pro Football Reference | Free |
| Storage | Supabase (Postgres + PostgREST) | Free tier |
| Orchestration | GitHub Actions (scheduled workflows) | Free tier |
| ML | XGBoost, SHAP, kNN comps | Free (CPU) |
| Real-time inference | Modal serverless functions | Free tier |
| API | Supabase PostgREST + Vercel API routes | Free tier |
| Frontend | Next.js on Vercel | Free tier |

Everything runs on free tiers. Estimated monthly compute: ~50–100 GitHub Actions minutes, ~$0.00 Modal cost for XGBoost CPU inference.

---

## Repo structure

```
nfl-breakout-predictor/
│
├── pipelines/
│   ├── ingest_seasonal.py       # pulls nfl_data_py + PFR, upserts player_seasons
│   ├── ingest_weekly.py         # pulls weekly game data, upserts player_weeks
│   ├── features.py              # all feature engineering — imported by ingest + train
│   ├── train_seasonal.py        # XGBoost seasonal model, SHAP, kNN comps
│   ├── train_weekly.py          # XGBoost weekly model, updates predictions
│   ├── backtest.py              # walk-forward validation, writes model_performance
│   └── modal_inference.py       # Modal app for live inference
│
├── overrides/
│   └── context_flags.csv        # hand-editable fallback: player_id, season, oc_change, qb_change
│
├── .github/
│   └── workflows/
│       ├── ingest_seasonal.yml  # runs March + August
│       ├── ingest_weekly.yml    # runs Tues 6am, Sep–Jan
│       ├── retrain_seasonal.yml # triggered manually each September
│       └── retrain_weekly.yml   # triggered by ingest_weekly completion
│
├── frontend/                    # Next.js app
│   ├── pages/
│   ├── components/
│   └── lib/
│       └── supabase.ts
│
├── tests/
│   ├── test_features.py         # unit tests for feature engineering
│   └── test_schema.py           # validates pipeline output against Supabase schema
│
├── requirements.txt
├── .env.example
└── README.md
```

---

## Data pipeline

### Seasonal ingest (runs twice: March + August)

Pulls 2020–present historical data via `nfl_data_py`. Computes all pre-season features and upserts to `player_seasons`. The August run ensures OC/QB/roster changes from free agency and the draft are captured before model training.

```bash
python pipelines/ingest_seasonal.py
```

### Weekly ingest (runs every Tuesday, Sep–Jan)

Pulls the previous week's game data, computes rolling features, upserts to `player_weeks`. A row count check runs before the model retraining job fires to guard against partial pulls.

```bash
python pipelines/ingest_weekly.py
```

---

## ML pipeline

### Pre-season model

- **Algorithm**: XGBoost binary classifier
- **Target**: `broke_out = 1` if fantasy PPG increased ≥30% YoY with ≥8 games played
- **Features**: age, age², 3yr target share slope, 3yr snap share slope, YPRR, draft position, OC change flag, QB change flag, team pass volume trend, PFF grade
- **Output**: breakout probability (0–100), SHAP feature importance per player, risk tier (green / yellow / red), top-5 historical comps via kNN

### In-season model

- **Algorithm**: XGBoost binary classifier, retrained weekly from scratch
- **Target**: `on_track_breakout = 1` based on current-season pace vs prior season
- **Features**: rolling 3-week snap share delta, rolling 3-week target share delta, air yards share, red zone target share, route participation rate, current vs prior season PPG
- **Output**: updated buy/sell score, direction arrow (up / down / flat)

### Comp finder

kNN with cosine similarity on normalized feature vectors. Top-5 most similar player-seasons from 2000–present. Computed at prediction time and stored in Supabase — frontend reads rows directly.

---

## Backtesting

Walk-forward validation across three season splits to prevent leakage:

| Split | Train | Predict | Evaluate |
|---|---|---|---|
| 1 | 2020–2021 | 2022 | vs actual 2022 |
| 2 | 2020–2022 | 2023 | vs actual 2023 |
| 3 | 2020–2023 | 2024 | vs actual 2024 (holdout) |

Metrics tracked per split: AUC-ROC, Precision@20, calibration error, comp accuracy. Results written to `model_performance` and surfaced as calibration curves in the frontend.

---

## Real-time inference

When a user adjusts feature weights in the UI or queries a custom player profile, a Vercel API route calls a Modal function that loads the serialized model from Supabase Storage and runs `predict_proba` on the custom input. Latency ~100ms on CPU. Cold starts are 2–4 seconds; `min_containers=1` keeps one instance warm.

---

## Database schema

Four tables in Supabase:

| Table | Description |
|---|---|
| `player_seasons` | One row per player per season. Pre-season features + ground truth labels. |
| `player_weeks` | One row per player per week. In-season features + rolling averages. |
| `predictions` | One row per player per prediction type. Scores, SHAP values, comps as JSONB. |
| `model_performance` | Backtest results per split per model. Used for calibration curve in UI. |

Row Level Security is enabled on all tables. The frontend uses the anon key (read-only). Ingest and training jobs use the service role key via GitHub Actions secrets.

---

## Environment variables

Copy `.env.example` to `.env` and fill in:

```
SUPABASE_URL=
SUPABASE_SERVICE_KEY=       # used by pipelines — never expose to frontend
SUPABASE_ANON_KEY=          # used by frontend
MODAL_TOKEN_ID=
MODAL_TOKEN_SECRET=
```

In GitHub Actions, set `SUPABASE_URL` and `SUPABASE_SERVICE_KEY` as repository secrets.

---

## Local setup

```bash
# Python (pipelines)
pip install -r requirements.txt

# Run a quick data exploration before building features
python -c "
import nfl_data_py as nfl
seasonal = nfl.import_seasonal_data([2023])
snap = nfl.import_snap_counts([2023])
print(seasonal.columns.tolist())
print(snap.columns.tolist())
"

# Frontend
cd frontend
npm install
npm run dev
```

---

## Development order

If you're building this from scratch, the recommended sequence is:

1. Run Supabase DDL — create tables and indexes
2. `features.py` — nail down the feature logic before anything else depends on it
3. `ingest_seasonal.py` — populate `player_seasons` with historical data
4. `ingest_weekly.py` — populate `player_weeks`
5. `backtest.py` — validate the feature set produces signal before training
6. `train_seasonal.py` + `train_weekly.py` — model training and prediction writing
7. `modal_inference.py` — wrap the model for live inference
8. GitHub Actions workflows — wire up the schedules
9. Frontend — build against real data already in Supabase

---

## License

MIT
