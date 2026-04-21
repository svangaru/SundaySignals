"""
backtest.py — Walk-forward backtest for seasonal and weekly models.

Splits (defined in constants.BACKTEST_SPLITS):
  Split 1: train ≤ 2021, test 2022
  Split 2: train ≤ 2022, test 2023
  Split 3: train ≤ 2023, test 2024  (final holdout)

Metrics per split:
  AUC-ROC, Precision@20, calibration error, comp accuracy

Results are upserted to the model_performance table in Supabase.

Usage:
    python pipelines/backtest.py
    python pipelines/backtest.py --model-type seasonal
    python pipelines/backtest.py --model-type weekly
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sklearn.calibration import calibration_curve
from sklearn.metrics import roc_auc_score
from supabase import create_client, Client

from pipelines.constants import (
    BACKTEST_SPLITS,
    SEASONAL_FEATURES,
    SEASONAL_TARGET,
    WEEKLY_FEATURES,
    WEEKLY_TARGET,
)
from pipelines.model import find_comps, fit_knn_scaler, train_xgb
from pipelines.utils import df_to_records

load_dotenv()

SUPABASE_URL         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

SEASONS_TABLE     = "player_seasons"
WEEKS_TABLE       = "player_weeks"
PERFORMANCE_TABLE = "model_performance"
UPSERT_BATCH_SIZE = 500
PRECISION_AT_K    = 20
COMP_N            = 5


def main() -> None:
    args   = _parse_args()
    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    model_types = (
        ["seasonal", "weekly"] if args.model_type == "both" else [args.model_type]
    )

    all_results: list[dict] = []

    if "seasonal" in model_types:
        print("=== Seasonal backtest ===")
        df = _load_seasonal(client)
        df = _prepare_seasonal(df)
        results = _run_backtest(df, SEASONAL_FEATURES, SEASONAL_TARGET, "seasonal")
        all_results.extend(results)

    if "weekly" in model_types:
        print("=== Weekly backtest ===")
        weeks_df   = _load_weekly(client)
        seasons_df = _load_seasonal(client)
        weeks_df   = _prepare_weekly(weeks_df, seasons_df)
        results    = _run_backtest(weeks_df, WEEKLY_FEATURES, WEEKLY_TARGET, "weekly")
        all_results.extend(results)

    if not all_results:
        print("No backtest results produced. Exiting.")
        sys.exit(0)

    rows  = df_to_records(pd.DataFrame(all_results))
    total = _upsert_batched(client, rows)
    print(f"\nUpserted {total} backtest results to {PERFORMANCE_TABLE}")


# ---------------------------------------------------------------------------
# Core backtest loop
# ---------------------------------------------------------------------------

def _run_backtest(
    df: pd.DataFrame,
    features: list[str],
    target: str,
    model_type: str,
) -> list[dict]:
    """Run all walk-forward splits and return metric rows ready for upsert."""
    results: list[dict] = []
    run_at = datetime.now(tz=timezone.utc).isoformat()

    for split_idx, (train_max, test_season) in enumerate(BACKTEST_SPLITS, start=1):
        train_df = df[(df["season"] <= train_max) & df[target].notna()].copy()
        test_df  = df[(df["season"] == test_season) & df[target].notna()].copy()

        if train_df.empty or test_df.empty:
            print(
                f"  Split {split_idx} (train≤{train_max}, test={test_season}): "
                "insufficient data — skipping."
            )
            continue

        X_train = train_df[features].fillna(0).astype(float)
        y_train = train_df[target].astype(int)
        X_test  = test_df[features].fillna(0).astype(float)
        y_test  = test_df[target].astype(int)

        model  = train_xgb(X_train, y_train)
        scaler = fit_knn_scaler(X_train)
        proba  = model.predict_proba(X_test)[:, 1]

        auc       = _auc_roc(y_test, proba)
        prec_k    = _precision_at_k(y_test, proba, PRECISION_AT_K)
        cal_err   = _calibration_error(y_test, proba)
        comp_acc  = _comp_accuracy(
            X_train, X_test, train_df, test_df, scaler, target
        )

        print(
            f"  Split {split_idx} | train≤{train_max}, test={test_season} | "
            f"n_train={len(train_df)}, n_test={len(test_df)} | "
            f"AUC={auc:.3f}  P@{PRECISION_AT_K}={prec_k:.3f}  "
            f"CalErr={cal_err:.3f}  CompAcc={comp_acc:.3f}"
        )

        results.append({
            "model_type":          model_type,
            "split_index":         split_idx,
            "train_max_season":    train_max,
            "test_season":         test_season,
            "auc_roc":             auc,
            f"precision_at_{PRECISION_AT_K}": prec_k,
            "calibration_error":   cal_err,
            "comp_accuracy":       comp_acc,
            "n_train":             len(train_df),
            "n_test":              len(test_df),
            "run_at":              run_at,
        })

    return results


# ---------------------------------------------------------------------------
# Metric helpers
# ---------------------------------------------------------------------------

def _auc_roc(y_true: pd.Series, proba: np.ndarray) -> float:
    if y_true.nunique() < 2:
        return float("nan")
    return float(roc_auc_score(y_true, proba))


def _precision_at_k(y_true: pd.Series, proba: np.ndarray, k: int) -> float:
    """Fraction of true positives in the top-k predictions ranked by score."""
    k = min(k, len(proba))
    top_idx = np.argsort(proba)[::-1][:k]
    return float(y_true.iloc[top_idx].mean())


def _calibration_error(
    y_true: pd.Series,
    proba: np.ndarray,
    n_bins: int = 10,
) -> float:
    """Mean absolute calibration error across uniform probability bins."""
    try:
        frac_pos, mean_pred = calibration_curve(
            y_true, proba, n_bins=n_bins, strategy="uniform"
        )
        return float(np.mean(np.abs(frac_pos - mean_pred)))
    except ValueError:
        return float("nan")


def _comp_accuracy(
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    scaler,
    target: str,
) -> float:
    """
    Fraction of test rows where ≥1 of the top-COMP_N historical comps
    shares the same breakout label as the test row.
    """
    corpus_vecs = scaler.transform(X_train.fillna(0))
    query_vecs  = scaler.transform(X_test.fillna(0))

    comp_meta = train_df[["player_id", "season", target]].reset_index(drop=True)
    comps_list = find_comps(query_vecs, corpus_vecs, comp_meta, n=COMP_N)

    test_labels = test_df[target].reset_index(drop=True)
    correct = sum(
        any(c[target] == true_lbl for c in comp_row)
        for comp_row, true_lbl in zip(comps_list, test_labels)
    )
    return correct / len(test_labels) if len(test_labels) > 0 else float("nan")


# ---------------------------------------------------------------------------
# Data prep helpers
# ---------------------------------------------------------------------------

def _load_seasonal(client: Client) -> pd.DataFrame:
    response = client.table(SEASONS_TABLE).select("*").execute()
    return pd.DataFrame(response.data)


def _load_weekly(client: Client) -> pd.DataFrame:
    response = client.table(WEEKS_TABLE).select("*").execute()
    return pd.DataFrame(response.data)


def _prepare_seasonal(df: pd.DataFrame) -> pd.DataFrame:
    """Add age_squared and cast boolean flags to float."""
    df = df.copy()
    df["age_squared"] = df["age"] ** 2
    for col in ("oc_change", "qb_change"):
        if col in df.columns:
            df[col] = df[col].astype(float)
    return df


def _prepare_weekly(
    weeks_df: pd.DataFrame,
    seasons_df: pd.DataFrame,
) -> pd.DataFrame:
    """Add ppr_points_vs_prior and on_track_breakout label from player_seasons."""
    prior = seasons_df[["player_id", "season", "fantasy_ppg"]].copy()
    prior["season"] = prior["season"] + 1
    prior = prior.rename(columns={"fantasy_ppg": "_prior_ppg"})

    df = weeks_df.merge(prior, on=["player_id", "season"], how="left")
    df["ppr_points_vs_prior"] = df["ppr_points"] - df["_prior_ppg"]
    df = df.drop(columns=["_prior_ppg"])

    labels = seasons_df[["player_id", "season", "broke_out"]].rename(
        columns={"broke_out": WEEKLY_TARGET}
    )
    return df.merge(labels, on=["player_id", "season"], how="left")


def _upsert_batched(client: Client, rows: list[dict]) -> int:
    total = 0
    for i in range(0, len(rows), UPSERT_BATCH_SIZE):
        batch = rows[i : i + UPSERT_BATCH_SIZE]
        client.table(PERFORMANCE_TABLE).upsert(batch).execute()
        total += len(batch)
        print(f"  {total}/{len(rows)} rows upserted...")
    return total


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Walk-forward backtest.")
    parser.add_argument(
        "--model-type",
        choices=["seasonal", "weekly", "both"],
        default="both",
        help="Which model to backtest (default: both).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
