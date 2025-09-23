# ml/stages/infer_batch.py
"""
Infer p50 predictions and conformal bounds for a (season, week) using the prod (or latest) model.
Feature list is read from the saved model.pkl so it exactly matches training.

Run:
    modal run -m ml.modal_app::infer_batch -- --season 2025 --week 3

ENV:
    SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
"""
from __future__ import annotations
import os
import io
import json
from datetime import datetime, timedelta, timezone
from typing import List

import numpy as np
import pandas as pd
from supabase import create_client
import joblib

SB_URL = os.environ["SUPABASE_URL"]
SB_SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
FEATURES_BUCKET = "features"
MODELS_BUCKET = "models"


def _sb():
    return create_client(SB_URL, SB_SERVICE_KEY)


def _download(sb, bucket: str, path: str) -> bytes:
    return sb.storage.from_(bucket).download(path)


def _select_model(sb) -> str:
    # Prefer prod model
    r = sb.table("model_registry").select("model_id").eq("is_prod", True).limit(1).execute()
    if r.data:
        return r.data[0]["model_id"]
    # else latest by id (temporal proxy)
    r2 = sb.table("model_registry").select("model_id").order("model_id", desc=True).limit(1).execute()
    if r2.data:
        return r2.data[0]["model_id"]
    return ""


def run(season: int, week: int):
    sb = _sb()

    # Load features parquet
    feat_path = f"season={season}/week={week}/features.parquet"
    try:
        feat_bytes = _download(sb, FEATURES_BUCKET, feat_path)
    except Exception:
        print(f"[infer_batch] Features not found: {feat_path}")
        return {"ok": False, "reason": "no_features"}
    df = pd.read_parquet(io.BytesIO(feat_bytes))

    # Pick model
    model_id = _select_model(sb)
    if not model_id:
        print("[infer_batch] No model in registry")
        return {"ok": False, "reason": "no_model"}

    # Load model object (contains feature list & q_alpha)
    obj_bytes = _download(sb, MODELS_BUCKET, f"{model_id}/model.pkl")
    obj = joblib.load(io.BytesIO(obj_bytes))
    model = obj["model"]
    q_alpha = float(obj.get("q_alpha", 4.0))
    features: List[str] = obj.get("features")

    if not features:
        # Fallback to a conservative list if older model lacks this field
        features = [
            "opp_dvp", "team_change", "rolling_fp3", "rolling_fp3_same_team",
            "games_played_last3", "dnp_prev", "delta_snap", "delta_targets", "delta_rush_att",
            "home", "attempts", "completions", "pass_attempts", "passing_yards", "passing_tds",
            "interceptions", "rush_attempts", "rushing_yards", "rushing_tds", "targets", "receptions",
            "receiving_yards", "receiving_tds", "snap_share", "route_participation", "air_yards",
        ]

    # Build design matrix exactly as in training
    X = df.reindex(columns=features)
    X = X.astype(float).fillna(0.0)

    p50 = model.predict(X)
    lo = p50 - q_alpha
    hi = p50 + q_alpha

    # Upsert into pred_cache
    pk = f"season#{season}#week#{week}"
    now = datetime.now(timezone.utc)
    valid_until = now + timedelta(hours=6)

    rows = []
    for i, pid in enumerate(df["player_id"].astype(str)):
        rows.append({
            "pk": pk,
            "sk": f"player#{pid}",
            "p50": float(p50[i]),
            "lo": float(lo[i]),
            "hi": float(hi[i]),
            "valid_until": valid_until.isoformat(),
        })

    if rows:
        CHUNK = 1000
        for i in range(0, len(rows), CHUNK):
            sb.table("pred_cache").upsert(rows[i:i+CHUNK], on_conflict="sk,pk").execute()

    print(f"[infer_batch] wrote {len(rows)} predictions for {pk} using model {model_id}")
    return {"ok": True, "rows": len(rows), "model_id": model_id}