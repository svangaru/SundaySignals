
# Fantasy FF — Serverless MLOps (Vercel + Supabase + Modal)

This is a cost-first, resume-ready scaffold for a fantasy football app.

## Fresh setup
1. **Create GitHub repo** (MIT).
2. **Vercel** → Import the repo (Next.js 14+). Add env vars from `.env.example` to Vercel (Production & Preview).
3. **Supabase** → New project; grab `SUPABASE_URL`, `SUPABASE_ANON_KEY`, `SUPABASE_SERVICE_ROLE_KEY`.
4. **Supabase Storage** → Create **private** buckets: `raw`, `features`, `models`.
5. **Supabase SQL** → Run `supabase/schema.sql` in the SQL editor.
6. **Modal** → `pipx install modal-client` → `modal token new`.
7. **GitHub Secrets** → add `VERCEL_TRIGGER_URL`, `VERCEL_WAIVERS_URL`, `VERCEL_REFRESH_URL`, `TRIGGER_TOKEN`.

## First data flow
- `modal run ml/modal_app.py::ingest_once` → writes parquet + upserts dims.
- Call `/api/trigger-weekly` (token-gated) for weekly pipeline (stubbed initially).

## Train → Infer → Promote
- Extend files in `ml/stages/*` with real logic (XGBoost/LightGBM; Conformal CV+).
- Predictions are written to `pred_cache`; serving reads from DB/cache only.

## Backtesting
- Implement `ml/stages/backtest_rolling.py`, write `model_runs` rows, and save plots under `models/{model_id}/plots/` in storage.

## Notes
- No synchronous ML during API requests.
- All scheduled jobs are triggered by GitHub Actions hitting Vercel API routes with `x-trigger-token`.
