"""
train_seasonal.py — Train the pre-season XGBoost breakout classifier.

Reads player_seasons from Supabase, trains on historical labeled data,
computes SHAP values and kNN historical comps, upserts predictions for the
target season, and saves model artifacts to Supabase Storage.

age_squared is derived here at train time and is not stored in player_seasons.

Usage:
    python pipelines/train_seasonal.py
    python pipelines/train_seasonal.py --train-seasons 2020 2021 2022 2023
    python pipelines/train_seasonal.py --predict-season 2024
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

from pipelines.constants import (
    SEASONAL_FEATURES,
    SEASONAL_MODEL_PATH,
    SEASONAL_TARGET,
)
from pipelines.model import (
    compute_shap,
    find_comps,
    fit_knn_scaler,
    save_artifacts,
    score_to_tier,
    train_xgb,
)
from pipelines.utils import df_to_records

load_dotenv()

SUPABASE_URL         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

SOURCE_TABLE      = "player_seasons"
PREDICTIONS_TABLE = "predictions"
UPSERT_BATCH_SIZE = 500

DEFAULT_TRAIN_SEASONS  = list(range(2020, 2024))  # 2020–2023
DEFAULT_PREDICT_SEASON = 2024


def main() -> None:
    args   = _parse_args()
    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    train_seasons  = args.train_seasons or DEFAULT_TRAIN_SEASONS
    predict_season = args.predict_season or DEFAULT_PREDICT_SEASON

    all_seasons = sorted(set(train_seasons) | {predict_season})
    df = _load_player_seasons(client, all_seasons)
    df = _add_derived_features(df)

    train_df = df[df["season"].isin(train_seasons)].copy()
    pred_df  = df[df["season"] == predict_season].copy()

    labeled = train_df[train_df[SEASONAL_TARGET].notna()].copy()
    if labeled.empty:
        print("No labeled training rows — cannot train. Exiting.")
        sys.exit(1)

    X_train = labeled[SEASONAL_FEATURES].fillna(0).astype(float)
    y_train = labeled[SEASONAL_TARGET].astype(int)

    print(
        f"Training on {len(labeled)} rows "
        f"(seasons {min(train_seasons)}–{max(train_seasons)}); "
        f"breakout rate = {y_train.mean():.1%}"
    )

    model  = train_xgb(X_train, y_train)
    scaler = fit_knn_scaler(X_train)

    save_artifacts(model, scaler, client.storage, SEASONAL_MODEL_PATH)
    print(f"Artifacts saved → {SEASONAL_MODEL_PATH}")

    if pred_df.empty:
        print(f"No rows found for predict season {predict_season}. Done.")
        return

    X_pred = pred_df[SEASONAL_FEATURES].fillna(0).astype(float)

    proba      = model.predict_proba(X_pred)[:, 1]
    shap_rows  = compute_shap(model, X_pred)
    comps_list = find_comps(
        query_vecs  = scaler.transform(X_pred),
        corpus_vecs = scaler.transform(X_train),
        meta_df     = labeled[["player_id", "season", "player_name", SEASONAL_TARGET]].reset_index(drop=True),
    )

    preds = pred_df[["player_id", "season"]].copy().reset_index(drop=True)
    preds["prediction_type"] = "seasonal"
    preds["breakout_prob"] = proba
    preds["buy_sell_score"] = [int(round(float(p) * 100)) for p in proba]
    preds["risk_tier"]     = None  # DB check constraint only allows null; tier derived from breakout_prob in frontend
    preds["shap_values"]   = shap_rows
    preds["comps"]         = comps_list

    rows  = df_to_records(preds)
    total = _upsert_batched(client, rows)
    print(f"Upserted {total} predictions for season {predict_season}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_player_seasons(client: Client, seasons: list[int]) -> pd.DataFrame:
    # Supabase Python client defaults to 1000 rows per query — paginate to get all.
    rows: list[dict] = []
    page_size = 1000
    offset = 0
    while True:
        page = (
            client.table(SOURCE_TABLE)
            .select("*")
            .in_("season", seasons)
            .range(offset, offset + page_size - 1)
            .execute()
            .data
        )
        rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return pd.DataFrame(rows)


def _add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute age_squared and cast boolean flags to float for XGBoost."""
    df = df.copy()
    df["age_squared"] = df["age"] ** 2
    for col in ("oc_change", "qb_change"):
        if col in df.columns:
            df[col] = df[col].astype(float)
    return df


def _upsert_batched(client: Client, rows: list[dict]) -> int:
    total = 0
    for i in range(0, len(rows), UPSERT_BATCH_SIZE):
        batch = rows[i : i + UPSERT_BATCH_SIZE]
        client.table(PREDICTIONS_TABLE).upsert(batch).execute()
        total += len(batch)
        print(f"  {total}/{len(rows)} rows upserted...")
    return total


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train seasonal breakout model.")
    parser.add_argument(
        "--train-seasons",
        nargs="+",
        type=int,
        default=None,
        help="Seasons to train on (e.g. --train-seasons 2020 2021 2022 2023).",
    )
    parser.add_argument(
        "--predict-season",
        type=int,
        default=None,
        help="Season to generate predictions for (e.g. 2024).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
