# ml/modal_app.py
from __future__ import annotations
import modal

# -------------------------------------------------------------------
# App + base image (installs everything from your requirements file)
# -------------------------------------------------------------------
app = modal.App("sundaysignals")

image = (
    modal.Image.debian_slim()
    .pip_install_from_requirements("ml/requirements.txt")
)

# -------------------------------------------------------------------
# Two clear configs:
#  - SMALL_CONFIG: inexpensive functions (sync, ingest, features, infer)
#  - HEAVY_CONFIG: training/backtesting/validation (more RAM/CPU + timeout)
# Adjust these if jobs OOM or time out.
# -------------------------------------------------------------------
SMALL_CONFIG = dict(
    image=image,
    secrets=[modal.Secret.from_name("supabase")],  # SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
    cpu=1.0,
    memory=2048,
    timeout=900,  # 15 min is plenty for sync/feature/infer jobs
)

HEAVY_CONFIG = dict(
    image=image,
    secrets=[modal.Secret.from_name("supabase")],
    cpu=2.0,
    memory=4096,
    timeout=1800,  # up to 30 min for train/backtest/validate
)

# --------------------------- Sleeper sync ------------------------------------
@app.function(**SMALL_CONFIG)
def sync_players():
    """Sync Sleeper /v1/players/nfl -> players.sleeper_id and snapshot raw."""
    from ml.stages import sync_players as mod
    return mod.run()

@app.function(**SMALL_CONFIG)
def sync_league_index(league_id: str, season: int):
    """
    Sync season index for a Sleeper league: league meta, users, and rosters.
    Usage: modal run -m ml.modal_app::sync_league_index -- --league_id <ID> --season 2025
    """
    from ml.stages import sync_league_index as mod
    return mod.run(league_id=league_id, season=season)

@app.function(**SMALL_CONFIG)
def sync_league_week(league_id: str, season: int, week: int):
    """
    Sync weekly matchups/transactions for a Sleeper league.
    Usage: modal run -m ml.modal_app::sync_league_week -- --league_id <ID> --season 2025 --week 3
    """
    from ml.stages import sync_league_week as mod
    return mod.run(league_id=league_id, season=season, week=week)

# --------------------------- NFL data pipeline -------------------------------
@app.function(**SMALL_CONFIG)
def backfill_history(start_season: int, end_season: int):
    """
    Historical backfill using nfl_data_py -> raw parquet + tables.
    Usage: modal run -m ml.modal_app::backfill_history -- --start_season 2015 --end_season 2024
    """
    from ml.stages import backfill_history as mod
    return mod.run(start_season=start_season, end_season=end_season)

@app.function(**SMALL_CONFIG)
def build_features(season: int, week: int):
    """
    Build feature Parquet for (season, week) with trade/role/injury-aware signals.
    Usage: modal run -m ml.modal_app::build_features -- --season 2025 --week 3
    """
    from ml.stages import build_features as mod
    return mod.run(season=season, week=week)

@app.function(**HEAVY_CONFIG)
def train_cvplus(start_season: int, end_season: int, learner: str = "xgb"):
    """
    Train baseline model with time-ordered CV and conformal q_alpha.
    Usage: modal run -m ml.modal_app::train_cvplus -- --start_season 2021 --end_season 2024
    """
    from ml.stages import train_cvplus as mod
    return mod.run(start_season=start_season, end_season=end_season, learner=learner)

@app.function(**SMALL_CONFIG)
def infer_batch(season: int, week: int):
    """
    Score features for (season, week) with prod/latest model -> pred_cache.
    Usage: modal run -m ml.modal_app::infer_batch -- --season 2025 --week 3
    """
    from ml.stages import infer_batch as mod
    return mod.run(season=season, week=week)

@app.function(**HEAVY_CONFIG)
def validate_promote(season: int, week: int):
    """
    Validate predictions vs actuals for completed week; optional auto-promote.
    Usage: modal run -m ml.modal_app::validate_promote -- --season 2025 --week 2
    """
    from ml.stages import validate_promote as mod
    return mod.run(season=season, week=week)

# (Optional) If you add backtesting later, give it HEAVY_CONFIG as well:
# @app.function(**HEAVY_CONFIG)
# def backtest_rolling(start_season: int, end_season: int):
#     from ml.stages import backtest_rolling as mod
#     return mod.run(start_season=start_season, end_season=end_season)

# --------------------------- Local helper entrypoint -------------------------
@app.local_entrypoint()
def main():
    print("Examples:")
    print("  modal run -m ml.modal_app::sync_players")
    print("  modal run -m ml.modal_app::sync_league_index -- --league_id <LEAGUE> --season 2025")
    print("  modal run -m ml.modal_app::sync_league_week -- --league_id <LEAGUE> --season 2025 --week 3")
    print("  modal run -m ml.modal_app::build_features -- --season 2025 --week 3")
    print("  modal run -m ml.modal_app::train_cvplus -- --start_season 2021 --end_season 2024")
    print("  modal run -m ml.modal_app::infer_batch -- --season 2025 --week 3")
    print("  modal run -m ml.modal_app::validate_promote -- --season 2025 --week 2")
