"""
model.py — Shared ML building blocks: XGBoost training, SHAP computation,
kNN comp finder, and model artifact serialization via Supabase Storage.

Imported by train_seasonal.py, train_weekly.py, backtest.py, and modal_inference.py.
"""

from __future__ import annotations

import io
from typing import Any

import joblib
import numpy as np
import pandas as pd
import shap
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

from pipelines.constants import MODEL_BUCKET, RISK_TIER_HIGH, RISK_TIER_MED

# XGBoost base hyperparameters (scale_pos_weight is set dynamically per dataset)
_XGB_PARAMS: dict[str, Any] = {
    "n_estimators": 300,
    "max_depth": 4,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "eval_metric": "logloss",
    "random_state": 42,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def train_xgb(X: pd.DataFrame, y: pd.Series) -> XGBClassifier:
    """
    Fit an XGBClassifier. Automatically weights positive class to handle imbalance.

    Parameters
    ----------
    X : feature matrix, NaNs must be filled before calling
    y : binary integer labels (0/1), must have no nulls
    """
    n_pos = int(y.sum())
    n_neg = len(y) - n_pos
    scale_pos = n_neg / n_pos if n_pos > 0 else 1.0

    clf = XGBClassifier(**_XGB_PARAMS, scale_pos_weight=scale_pos)
    clf.fit(X.astype(float), y.astype(int))
    return clf


def compute_shap(model: XGBClassifier, X: pd.DataFrame) -> list[dict[str, float]]:
    """
    Compute per-row SHAP values using TreeExplainer.

    Returns a list of dicts (one per row), mapping feature name → SHAP value.
    Handles both single-array (shap ≥ 0.40) and list-of-two-arrays output shapes.
    """
    explainer = shap.TreeExplainer(model)
    vals = explainer.shap_values(X.astype(float))

    # shap < 0.40 returns [neg_class_vals, pos_class_vals] for binary
    if isinstance(vals, list):
        vals = vals[1]

    return [
        {col: float(v) for col, v in zip(X.columns, row)}
        for row in vals
    ]


def fit_knn_scaler(X: pd.DataFrame) -> StandardScaler:
    """
    Fit a StandardScaler on training data for use in cosine-similarity comp search.
    Must be called only on training data; the fitted scaler is serialized with the model.
    """
    scaler = StandardScaler()
    scaler.fit(X.astype(float).fillna(0))
    return scaler


def find_comps(
    query_vecs: np.ndarray,
    corpus_vecs: np.ndarray,
    meta_df: pd.DataFrame,
    n: int = 5,
) -> list[list[dict]]:
    """
    For each row in query_vecs, return the top-n most similar corpus rows
    by cosine similarity.

    Parameters
    ----------
    query_vecs  : (N, F) array — already scaled with fit_knn_scaler
    corpus_vecs : (M, F) array — already scaled with the same scaler
    meta_df     : DataFrame row-aligned with corpus_vecs; all columns are
                  included in each comp dict (e.g. player_id, season, player_name,
                  broke_out). Required: player_id, season.
    n           : number of comps to return per query row

    Returns
    -------
    List of N lists; each inner list has n dicts with meta_df columns + similarity.
    """
    sims = cosine_similarity(query_vecs, corpus_vecs)  # (N_query, N_corpus)
    meta = meta_df.reset_index(drop=True)
    meta_cols = meta.columns.tolist()

    results: list[list[dict]] = []
    for row_sims in sims:
        top_idx = np.argsort(row_sims)[::-1][:n]
        comps: list[dict] = []
        for idx in top_idx:
            rec: dict = {}
            for col in meta_cols:
                val = meta.iloc[idx][col]
                rec[col] = val.item() if hasattr(val, "item") else val
            rec["similarity"] = float(row_sims[idx])
            comps.append(rec)
        results.append(comps)

    return results


def save_artifacts(
    model: XGBClassifier,
    scaler: StandardScaler,
    storage_client: Any,
    path: str,
) -> None:
    """
    Serialize model + scaler via joblib and upload to Supabase Storage.

    Parameters
    ----------
    storage_client : supabase_client.storage  (the storage sub-client)
    path           : object path within MODEL_BUCKET, e.g. "seasonal/latest.joblib"
    """
    buf = io.BytesIO()
    joblib.dump({"model": model, "scaler": scaler}, buf)
    storage_client.from_(MODEL_BUCKET).upload(
        path=path,
        file=buf.getvalue(),
        file_options={"content-type": "application/octet-stream", "upsert": "true"},
    )


def load_artifacts(
    storage_client: Any,
    path: str,
) -> tuple[XGBClassifier, StandardScaler]:
    """
    Download and deserialize model + scaler from Supabase Storage.

    Parameters
    ----------
    storage_client : supabase_client.storage
    path           : same path used in save_artifacts

    Returns
    -------
    (model, scaler) tuple
    """
    data = storage_client.from_(MODEL_BUCKET).download(path)
    artifacts = joblib.load(io.BytesIO(data))
    return artifacts["model"], artifacts["scaler"]


def score_to_tier(p: float) -> str:
    """Map a breakout probability to a risk tier label."""
    if p >= RISK_TIER_HIGH:
        return "high"
    if p >= RISK_TIER_MED:
        return "medium"
    return "low"
