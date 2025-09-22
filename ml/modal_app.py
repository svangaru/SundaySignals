# ml/modal_app.py
from modal import App, Image, Secret

app = App("fantasy_ml")

image = (
    Image.debian_slim()
    .pip_install_from_requirements("ml/requirements.txt")
)

# ── Helper: ensure you created a Modal secret named "supabase"
#     that includes SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY.

@app.function(image=image, timeout=600, secrets=[Secret.from_name("supabase")])
def ingest_once(season_start: int = 2023, season_end: int = 2024):
    # Import here (NOT top-level) so modal's snapshot/imports work cleanly
    from ml.stages import ingest_once as ingest_mod
    import os

    print("Modal sees SUPABASE_URL:", "SUPABASE_URL" in os.environ)
    # Adjust your function name as you implement it; this is just a call-through
    return ingest_mod.run(season=2025, week=3)


@app.function(image=image, timeout=600, secrets=[Secret.from_name("supabase")])
def build_features(season: int, week: int):
    print(f"[stub] Build features for season={season}, week={week}")


@app.function(image=image, timeout=1200, secrets=[Secret.from_name("supabase")])
def train_cvplus(season: int, week: int, learner: str = "xgb"):
    print(f"[stub] Train CV+ for season={season}, week={week}, learner={learner}")


@app.function(image=image, timeout=600, secrets=[Secret.from_name("supabase")])
def infer_batch(season: int, week: int):
    print(f"[stub] Inference batch for season={season}, week={week}")


@app.function(image=image, timeout=600, secrets=[Secret.from_name("supabase")])
def validate_promote(season: int, week: int):
    print(f"[stub] Validate & promote model for season={season}, week={week}")


@app.function(image=image, timeout=1800, secrets=[Secret.from_name("supabase")])
def backtest_rolling(start_season: int = 2015, end_season: int = 2024):
    print(f"[stub] Backtest rolling from {start_season} to {end_season}")
