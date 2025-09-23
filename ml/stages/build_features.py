# ml/stages/build_features.py
import os, io
import pandas as pd
from supabase import create_client

SB_URL = os.environ["SUPABASE_URL"]
SB_SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
FEATURES_BUCKET = "features"
RAW_BUCKET = "raw"

def _download_parquet(sb, bucket: str, path: str) -> pd.DataFrame:
    resp = sb.storage.from_(bucket).download(path)
    return pd.read_parquet(io.BytesIO(resp))

def _upload_parquet(sb, bucket: str, path: str, df: pd.DataFrame):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    sb.storage.from_(bucket).upload(path, buf.read(), {"content-type": "application/octet-stream", "upsert": "true"})

def run(season: int = 2025, week: int = 3):
    sb = create_client(SB_URL, SB_SERVICE_KEY)

    weekly = _download_parquet(sb, RAW_BUCKET, f"nfl/{season}/weekly.parquet")
    w = weekly[weekly["week"] == week].copy()

    # Select/engineer simple, robust features (expand later)
    feats = w[[
        "player_id","player_name","position","recent_team","opponent_team","week","season",
        "home","attempts","completions","passing_yards","passing_tds","interceptions",
        "rushing_yards","rushing_tds","receptions","targets","receiving_yards","receiving_tds",
        "sacks","sack_yards","fantasy_points_ppr"
    ]].copy()

    # Fill missing, add flags
    feats["home"] = feats.get("home", False).fillna(False).astype(int)
    feats["is_rb"] = (feats["position"]=="RB").astype(int)
    feats["is_wr"] = (feats["position"]=="WR").astype(int)
    feats["is_te"] = (feats["position"]=="TE").astype(int)
    feats["is_qb"] = (feats["position"]=="QB").astype(int)

    # Label y (next-step target). For backtests you’ll shift by +1 week; for now use same-week realized points
    feats = feats.rename(columns={"fantasy_points_ppr":"y_actual_ppr"})

    out_path = f"season={season}/week={week}/features.parquet"
    _upload_parquet(sb, FEATURES_BUCKET, out_path, feats)

    print(f"[features] wrote {len(feats)} rows → {FEATURES_BUCKET}/{out_path}")
    return {"ok": True, "rows": len(feats)}
