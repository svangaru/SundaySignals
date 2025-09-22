# ml/modal_app.py
from modal import App, Image, Secret, Mount

# Mount the whole repo (or at least the ml/ dir) into the container so imports work
# Option A: mount the repo root (cleanest if you import as "ml.stages...")
MOUNTS = [Mount.from_local_dir(".", remote_path="/root")]

app = App("fantasy_ml")

image = (
    Image.debian_slim()
    .pip_install_from_requirements("ml/requirements.txt")
)

# Import using the package path now that /root contains ml/
from ml.stages import ingest_once as ingest_mod

@app.function(image=image, timeout=600,
              secrets=[Secret.from_name("supabase")],
              mounts=MOUNTS)
def ingest_once(season_start: int = 2023, season_end: int = 2024):
    import os
    print("ENV has SUPABASE_URL?", "SUPABASE_URL" in os.environ)
    out = ingest_mod.run(season=2025, week=3)
    print("Upsert result:", out)


# Repeat mounts+secrets on other functions
@app.function(image=image, timeout=600,
              secrets=[Secret.from_name("supabase")],
              mounts=MOUNTS)
def build_features(season: int, week: int):
    print(f"[stub] Build features for season={season}, week={week}")

@app.function(image=image, timeout=1200,
              secrets=[Secret.from_name("supabase")],
              mounts=MOUNTS)
def train_cvplus(season: int, week: int, learner: str = "xgb"):
    print(f"[stub] Train CV+ for season={season}, week={week}, learner={learner}")

@app.function(image=image, timeout=600,
              secrets=[Secret.from_name("supabase")],
              mounts=MOUNTS)
def infer_batch(season: int, week: int):
    print(f"[stub] Inference batch for season={season}, week={week}")

@app.function(image=image, timeout=600,
              secrets=[Secret.from_name("supabase")],
              mounts=MOUNTS)
def validate_promote(season: int, week: int):
    print(f"[stub] Validate & promote model for season={season}, week={week}")

@app.function(image=image, timeout=1800,
              secrets=[Secret.from_name("supabase")],
              mounts=MOUNTS)
def backtest_rolling(start_season: int = 2015, end_season: int = 2024):
    print(f"[stub] Backtest rolling from {start_season} to {end_season}")
