# ml/stages/train_cvplus.py
"""
Train a baseline XGBoost model with time-ordered CV and compute conformal q_alpha.
Feature list aligned with build_features.py (trade/role-shock/scheme/injury proxies).

Run:
    modal run -m ml.modal_app::train_cvplus -- --start_season 2021 --end_season 2024 --learner xgb

ENV:
    SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
"""
from __future__ import annotations
import os
import io
import json
import time
import uuid
from typing import List, Tuple

import numpy as np
import pandas as pd
from supabase import create_client
from xgboost import XGBRegressor
import joblib

SB_URL = os.environ["SUPABASE_URL"]
SB_SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
FEATURES_BUCKET = "features"
MODELS_BUCKET = "models"

TARGET = "fantasy_points_ppr"  # supervised target (historical)
ALPHA = 0.15                    # 85% prediction interval

# Keep this EXACTLY in sync with infer_batch.py
BASE_NUMERIC = [
    # core stats if present
    "attempts", "completions", "pass_attempts", "passing_yards", "passing_tds", "interceptions",
    "rush_attempts", "rushing_yards", "rushing_tds",
    "targets", "receptions", "receiving_yards", "receiving_tds",
    "snap_share", "route_participation", "air_yards",
]
ENGINEERED = [
    # trade/role-shock / scheme / injury proxies
    "opp_dvp",
    "team_change",
    "rolling_fp3",
    "rolling_fp3_same_team",
    "games_played_last3",
    "dnp_prev",
    "delta_snap", "delta_targets", "delta_rush_att",
    # context
    "home",
]


def _sb():
    return create_client(SB_URL, SB_SERVICE_KEY)


def _list_weeks(start_season: int, end_season: int) -> List[Tuple[int, int]]:
    weeks = []
    for s in range(start_season, end_season + 1):
        for w in range(1, 19):
            weeks.append((s, w))
    return weeks


def _download_features(sb, season: int, week: int) -> pd.DataFrame:
    path = f"season={season}/week={week}/features.parquet"
    try:
        data = sb.storage.from_(FEATURES_BUCKET).download(path)
    except Exception:
        return pd.DataFrame()
    return pd.read_parquet(io.BytesIO(data))


def _feature_columns(df: pd.DataFrame) -> List[str]:
    cols = [c for c in ENGINEERED + BASE_NUMERIC if c in df.columns]
    return cols


def _prep(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    if TARGET not in df.columns:
        return pd.DataFrame(), pd.Series(dtype=float)
    df = df.dropna(subset=[TARGET])
    if df.empty:
        return pd.DataFrame(), pd.Series(dtype=float)
    cols = _feature_columns(df)
    X = df[cols].astype(float).fillna(0.0)
    y = df[TARGET].astype(float)
    return X, y


def _time_folds(unique_sw: List[Tuple[int, int]], k: int = 5) -> List[List[Tuple[int, int]]]:
    n = len(unique_sw)
    if n < k:
        k = max(2, n)
    size = max(1, n // k)
    folds = [unique_sw[i*size:(i+1)*size] for i in range(k-1)]
    folds.append(unique_sw[(k-1)*size:])
    return folds


def run(start_season: int, end_season: int, learner: str = "xgb"):
    sb = _sb()

    frames: List[pd.DataFrame] = []
    for (s, w) in _list_weeks(start_season, end_season):
        df = _download_features(sb, s, w)
        if df.empty:
            continue
        if TARGET in df.columns and not df[TARGET].isna().all():
            df = df.copy()
            df["_season"] = s
            df["_week"] = w
            frames.append(df)
    if not frames:
        print("[train_cvplus] No training data found.")
        return {"ok": False, "reason": "no_data"}

    data = pd.concat(frames, axis=0, ignore_index=True)
    sw_unique = sorted(set(zip(data["_season"], data["_week"])) )
    folds = _time_folds(sw_unique, k=5)

    residuals = []
    fold_metrics = []
    for i, val_sw in enumerate(folds):
        val_set = set(val_sw)
        is_val = data[["_season", "_week"]].apply(tuple, axis=1).isin(val_set)
        Xtr, ytr = _prep(data[~is_val])
        Xva, yva = _prep(data[is_val])
        if len(Xtr) == 0 or len(Xva) == 0:
            continue

        model = XGBRegressor(
            n_estimators=450,
            max_depth=6,
            learning_rate=0.07,
            subsample=0.9,
            colsample_bytree=0.9,
            reg_lambda=1.0,
            n_jobs=4,
            random_state=42,
        )
        model.fit(Xtr, ytr)
        p50 = model.predict(Xva)
        res = (yva.values - p50)
        residuals.extend(res.tolist())
        fold_metrics.append({"fold": i, "mae": float(np.mean(np.abs(res))), "n_val": int(len(yva))})

    if not residuals:
        print("[train_cvplus] No residuals computed.")
        return {"ok": False, "reason": "no_residuals"}

    q_alpha = float(np.quantile(np.abs(np.array(residuals)), 1.0 - ALPHA))

    # Final fit on all data
    Xall, yall = _prep(data)
    model = XGBRegressor(
        n_estimators=600,
        max_depth=6,
        learning_rate=0.07,
        subsample=0.9,
        colsample_bytree=0.9,
        reg_lambda=1.0,
        n_jobs=4,
        random_state=42,
    )
    model.fit(Xall, yall)

    # Persist artifacts
    model_id = f"xgb-{int(time.time())}-{uuid.uuid4().hex[:8]}"

    # Save pkl (model + q_alpha + feature list)
    obj = {"model": model, "q_alpha": q_alpha, "features": _feature_columns(data)}
    buf = io.BytesIO(); joblib.dump(obj, buf); buf.seek(0)
    path_pkl = f"{model_id}/model.pkl"
    try:
        sb.storage.from_(MODELS_BUCKET).remove([path_pkl])
    except Exception:
        pass
    sb.storage.from_(MODELS_BUCKET).upload(path_pkl, buf.getvalue(), file_options={"content-type": "application/octet-stream"})

    # Save metrics.json
    metrics = {"folds": fold_metrics, "q_alpha": q_alpha, "alpha": ALPHA, "target": TARGET}
    sb.storage.from_(MODELS_BUCKET).upload(
        f"{model_id}/metrics.json",
        json.dumps(metrics).encode("utf-8"),
        file_options={"content-type": "application/json"}
    )

    # Upsert registry row
    sb.table("model_registry").upsert({
        "model_id": model_id,
        "label": f"XGB baseline {start_season}-{end_season}",
        "metrics": metrics,
        "is_prod": False,
        "prod_week": None,
        "prod_season": None,
    }, on_conflict="model_id").execute()

    print(f"[train_cvplus] saved {model_id} q_alpha={q_alpha:.3f}")
    return {"ok": True, "model_id": model_id, "q_alpha": q_alpha, "fold_metrics": fold_metrics}
