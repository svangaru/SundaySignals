# ml/modal_app.py
import modal as _modal

# Support both APIs: App (new) and Stub (old)
App = getattr(_modal, "App", None)
if App is None:
    from modal import Stub as App  # older modal
    Image = _modal.Image
else:
    from modal import Image

app = App("fantasy_ml")

image = (
    Image.debian_slim()
    .pip_install_from_requirements("ml/requirements.txt")
)

@app.function(image=image, timeout=600)
def ingest_once(season_start: int = 2023, season_end: int = 2024):
    print("Ingest sample stub", season_start, season_end)

@app.function(image=image, timeout=600)
def build_features(season: int, week: int):
    print("Build features", season, week)

@app.function(image=image, timeout=1200)
def train_cvplus(season: int, week: int, learner: str = "xgb"):
    print("Train CV+", season, week, learner)

@app.function(image=image, timeout=600)
def infer_batch(season: int, week: int):
    print("Infer batch", season, week)

@app.function(image=image, timeout=600)
def validate_promote(season: int, week: int):
    print("Validate & promote", season, week)

@app.function(image=image, timeout=1800)
def backtest_rolling(start_season: int = 2015, end_season: int = 2024):
    print("Backtest rolling", start_season, end_season)
