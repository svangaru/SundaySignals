# ml/modal_app.py
from modal import App, Image, Secret

app = App("fantasy_ml")

image = (
    Image.debian_slim()
    .pip_install_from_requirements("ml/requirements.txt")
)

@app.function(image=image, timeout=600, secrets=[Secret.from_name("supabase")])
def ingest_once(season_start: int = 2023, season_end: int = 2024):
    import os
    print("SUPABASE_URL (from Modal env):", repr(os.environ.get("SUPABASE_URL")))
    from ml.stages import ingest_once as ingest_mod
    res = ingest_mod.run(season=2025, week=3)
    print("[ingest] result:", res)
    return res

@app.function(image=image, timeout=600, secrets=[Secret.from_name("supabase")])
def build_features(season: int, week: int):
    from ml.stages import build_features as mod
    df = mod.run(season, week)
    print("[build_features] rows:", len(df))

@app.function(image=image, timeout=1200, secrets=[Secret.from_name("supabase")])
def train_cvplus(season: int, week: int, learner: str = "xgb"):
    from ml.stages import train_cvplus as mod
    res = mod.run(season, week, learner=learner)
    print("[train_cvplus]", res)

@app.function(image=image, timeout=600, secrets=[Secret.from_name("supabase")])
def infer_batch(season: int, week: int, learner: str = "xgb"):
    from ml.stages import infer_batch as mod
    res = mod.run(season, week, learner=learner)
    print("[infer_batch]", res)

@app.function(image=image, timeout=600, secrets=[Secret.from_name("supabase")])
def validate_promote(season: int, week: int):
    print(f"[stub] Validate & promote model for season={season}, week={week}")

@app.function(image=image, timeout=1800, secrets=[Secret.from_name("supabase")])
def backtest_rolling(start_season: int = 2015, end_season: int = 2024):
    print(f"[stub] Backtest rolling from {start_season} to {end_season}")
