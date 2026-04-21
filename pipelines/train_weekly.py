"""
train_weekly.py — Train the in-season XGBoost breakout classifier.

Reads player_weeks from Supabase and joins player_seasons to:
  - derive ppr_points_vs_prior (not stored in player_weeks)
  - attach on_track_breakout label (= broke_out for that player-season)

Trains on historical labeled weeks, scores the target season/week(s),
upserts predictions, and saves model artifacts to Supabase Storage.

Usage:
    python pipelines/train_weekly.py
    python pipelines/train_weekly.py --train-seasons 2020 2021 2022 2023
    python pipelines/train_weekly.py --predict-season 2024
    python pipelines/train_weekly.py --predict-season 2024 --predict-week 12
"""

from __future__ import annotations

import argparse
import os
import sys

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

from pipelines.constants import (
    WEEKLY_FEATURES,
    WEEKLY_MODEL_PATH,
    WEEKLY_TARGET,
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

WEEKS_TABLE       = "player_weeks"
SEASONS_TABLE     = "player_seasons"
PREDICTIONS_TABLE = "predictions"
UPSERT_BATCH_SIZE = 500

DEFAULT_TRAIN_SEASONS  = list(range(2020, 2024))
DEFAULT_PREDICT_SEASON = 2024


def main() -> None:
    args   = _parse_args()
    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    train_seasons  = args.train_seasons or DEFAULT_TRAIN_SEASONS
    predict_season = args.predict_season or DEFAULT_PREDICT_SEASON
    predict_week   = args.predict_week

    all_seasons = sorted(set(train_seasons) | {predict_season})

    weeks_df   = _load_player_weeks(client, all_seasons)
    seasons_df = _load_player_seasons(client, all_seasons)

    weeks_df = _add_ppr_vs_prior(weeks_df, seasons_df)
    weeks_df = _add_label(weeks_df, seasons_df)

    train_df = weeks_df[weeks_df["season"].isin(train_seasons)].copy()
    labeled  = train_df[train_df[WEEKLY_TARGET].notna()].copy()

    if labeled.empty:
        print("No labeled training rows — cannot train. Exiting.")
        sys.exit(1)

    X_train = labeled[WEEKLY_FEATURES].fillna(0).astype(float)
    y_train = labeled[WEEKLY_TARGET].astype(int)

    print(
        f"Training on {len(labeled)} rows "
        f"(seasons {min(train_seasons)}–{max(train_seasons)}); "
        f"on-track rate = {y_train.mean():.1%}"
    )

    model  = train_xgb(X_train, y_train)
    scaler = fit_knn_scaler(X_train)

    save_artifacts(model, scaler, client.storage, WEEKLY_MODEL_PATH)
    print(f"Artifacts saved → {WEEKLY_MODEL_PATH}")

    pred_mask = weeks_df["season"] == predict_season
    if predict_week is not None:
        pred_mask = pred_mask & (weeks_df["week"] == predict_week)
    pred_df = weeks_df[pred_mask].copy().reset_index(drop=True)

    if pred_df.empty:
        print(f"No rows to score for season {predict_season}. Done.")
        return

    X_pred = pred_df[WEEKLY_FEATURES].fillna(0).astype(float)

    proba      = model.predict_proba(X_pred)[:, 1]
    shap_rows  = compute_shap(model, X_pred)
    comps_list = find_comps(
        query_vecs  = scaler.transform(X_pred),
        corpus_vecs = scaler.transform(X_train),
        meta_df     = labeled[["player_id", "season", "week", WEEKLY_TARGET]].reset_index(drop=True),
    )

    preds = pred_df[["player_id", "season", "week"]].copy()
    preds["model_type"]    = "weekly"
    preds["breakout_prob"] = proba
    preds["risk_tier"]     = [score_to_tier(p) for p in proba]
    preds["shap_values"]   = shap_rows
    preds["comps"]         = comps_list

    rows  = df_to_records(preds)
    total = _upsert_batched(client, rows)
    week_label = f"week {predict_week}" if predict_week else "all weeks"
    print(f"Upserted {total} predictions for season {predict_season} {week_label}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_player_weeks(client: Client, seasons: list[int]) -> pd.DataFrame:
    response = (
        client.table(WEEKS_TABLE)
        .select("*")
        .in_("season", seasons)
        .execute()
    )
    return pd.DataFrame(response.data)


def _load_player_seasons(client: Client, seasons: list[int]) -> pd.DataFrame:
    response = (
        client.table(SEASONS_TABLE)
        .select("player_id,season,fantasy_ppg,broke_out")
        .in_("season", seasons)
        .execute()
    )
    return pd.DataFrame(response.data)


def _add_ppr_vs_prior(
    weeks_df: pd.DataFrame,
    seasons_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    ppr_points_vs_prior = current week ppr_points − prior season fantasy_ppg.
    Players without a prior season get NaN (filled to 0 at feature matrix build time).
    """
    prior = seasons_df[["player_id", "season", "fantasy_ppg"]].copy()
    prior["season"] = prior["season"] + 1  # shift so it aligns with the next season
    prior = prior.rename(columns={"fantasy_ppg": "_prior_ppg"})

    df = weeks_df.merge(prior, on=["player_id", "season"], how="left")
    df["ppr_points_vs_prior"] = df["ppr_points"] - df["_prior_ppg"]
    return df.drop(columns=["_prior_ppg"])


def _add_label(
    weeks_df: pd.DataFrame,
    seasons_df: pd.DataFrame,
) -> pd.DataFrame:
    """Join broke_out from player_seasons as the on_track_breakout label."""
    labels = (
        seasons_df[["player_id", "season", "broke_out"]]
        .rename(columns={"broke_out": WEEKLY_TARGET})
    )
    return weeks_df.merge(labels, on=["player_id", "season"], how="left")


def _upsert_batched(client: Client, rows: list[dict]) -> int:
    total = 0
    for i in range(0, len(rows), UPSERT_BATCH_SIZE):
        batch = rows[i : i + UPSERT_BATCH_SIZE]
        client.table(PREDICTIONS_TABLE).upsert(batch).execute()
        total += len(batch)
        print(f"  {total}/{len(rows)} rows upserted...")
    return total


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train weekly in-season breakout model.")
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
        help="Season to score (e.g. 2024).",
    )
    parser.add_argument(
        "--predict-week",
        type=int,
        default=None,
        help="Specific week to score. Defaults to all available weeks in the season.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
