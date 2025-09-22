from modal import App, Image, Secret

app = App("fantasy_ml")

image = (
    Image.debian_slim()
    .pip_install_from_requirements("ml/requirements.txt")
)

@app.function(image=image, timeout=600, secrets=[Secret.from_name("supabase")])
def ingest_once(season_start: int = 2023, season_end: int = 2024):
    from ml.stages import ingest_once as mod
    return mod.run(season=2025, week=3)

@app.function(image=image, timeout=600, secrets=[Secret.from_name("supabase")])
def build_features(season: int = 2025, week: int = 3):
    from ml.stages import build_features as mod
    return mod.run(season, week)

@app.function(image=image, timeout=1200, secrets=[Secret.from_name("supabase")])
def train_cvplus(season: int = 2025, week: int = 3, learner: str = "xgb"):
    from ml.stages import train_cvplus as mod
    return mod.run(season, week, learner)

@app.function(image=image, timeout=600, secrets=[Secret.from_name("supabase")])
def infer_batch(season: int = 2025, week: int = 3):
    from ml.stages import infer_batch as mod
    return mod.run(season, week)

@app.function(image=image, timeout=600, secrets=[Secret.from_name("supabase")])
def validate_promote(season: int = 2025, week: int = 3):
    from ml.stages import validate_promote as mod
    return mod.run(season, week)

@app.function(image=image, timeout=1800, secrets=[Secret.from_name("supabase")])
def backtest_rolling(start_season: int = 2015, end_season: int = 2024):
    from ml.stages import backtest_rolling as mod
    return mod.run(start_season, end_season)
