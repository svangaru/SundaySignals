# ml/stages/validate_promote.py
"""
Validate last completed week against actuals, write model_runs row, and optionally promote model.

Run:
    modal run -m ml.modal_app::validate_promote -- --season 2025 --week 2
"""
from __future__ import annotations
import os
import io
import json
from typing import Dict, Any
import numpy as np
import pandas as pd
from supabase import create_client

SB_URL = os.environ["SUPABASE_URL"]
SB_SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
RAW_BUCKET = "raw"

TARGET = "fantasy_points_ppr"


def _sb():
    return create_client(SB_URL, SB_SERVICE_KEY)


def _download_parquet(sb, bucket: str, path: str) -> pd.DataFrame:
    data = sb.storage.from_(bucket).download(path)
    return pd.read_parquet(io.BytesIO(data))


def _pinball_loss(y: np.ndarray, yhat: np.ndarray, q: float) -> float:
    e = y - yhat
    return float(np.mean(np.maximum(q*e, (q-1)*e)))


def run(season: int, week: int):
    sb = _sb()

    # Actuals from weekly parquet
    weekly_path = f"nfl/{season}/weekly.parquet"
    weekly = _download_parquet(sb, RAW_BUCKET, weekly_path)
    act = weekly[(weekly["season"] == season) & (weekly["week"] == week)][["player_id", TARGET]].dropna()

    # Predictions from pred_cache
    pk = f"season#{season}#week#{week}"
    res = sb.table("pred_cache").select("sk,p50,lo,hi").eq("pk", pk).execute()
    pred = pd.DataFrame(res.data or [])
    if pred.empty or act.empty:
        print("[validate_promote] Missing predictions or actuals")
        return {"ok": False, "reason": "missing_data"}

    pred["player_id"] = pred["sk"].str.replace("player#", "", regex=False)
    df = pred.merge(act, on="player_id", how="inner")
    if df.empty:
        print("[validate_promote] No overlap between preds and actuals")
        return {"ok": False, "reason": "no_overlap"}

    y = df[TARGET].astype(float).values
    p50 = df["p50"].astype(float).values
    lo = df["lo"].astype(float).values
    hi = df["hi"].astype(float).values

    mae = float(np.mean(np.abs(y - p50)))
    pin_p10 = _pinball_loss(y, p50, 0.10)
    pin_p50 = _pinball_loss(y, p50, 0.50)
    pin_p90 = _pinball_loss(y, p50, 0.90)
    coverage = float(np.mean((y >= lo) & (y <= hi)))

    metrics = {
        "season": season,
        "week": week,
        "n": int(len(y)),
        "mae": mae,
        "pinball": {"p10": pin_p10, "p50": pin_p50, "p90": pin_p90},
        "coverage": coverage,
    }

    # Write a model_runs row
    sb.table("model_runs").insert({
        "season": season,
        "week": week,
        "stage": "validate",
        "metrics": metrics,
        "status": "finished",
    }).execute()

    # Optional promotion: compare to current prod (naive rule)
    target_cov = 0.85
    cov_tol = 0.03
    promote = (coverage >= (target_cov - cov_tol)) and (coverage <= (target_cov + cov_tol))

    # If promote, set the latest model to prod (simple policy)
    if promote:
        # Find latest model
        r = sb.table("model_registry").select("model_id").order("model_id", desc=True).limit(1).execute()
        latest = (r.data or [{}])[0].get("model_id")
        if latest:
            # demote all
            sb.table("model_registry").update({"is_prod": False}).neq("model_id", latest).execute()
            # promote latest
            sb.table("model_registry").update({
                "is_prod": True,
                "prod_season": season,
                "prod_week": week,
            }).eq("model_id", latest).execute()

    print(f"[validate_promote] week={week} mae={mae:.3f} coverage={coverage:.3f} promote={promote}")
    return {"ok": True, "metrics": metrics, "promote": promote}
