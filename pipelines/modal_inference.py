"""
modal_inference.py — Modal serverless inference service for SundaySignals.

InferenceService loads models on container startup. The seasonal model is
required; the weekly model is optional (loaded only if the artifact exists in
Storage, since it is not trained until the NFL season starts).

Deploy:
    modal deploy pipelines/modal_inference.py

Test locally (requires modal token):
    modal run pipelines/modal_inference.py::score_endpoint
"""

from __future__ import annotations

import os
from typing import Any

import modal

from pipelines.constants import (
    SEASONAL_FEATURES,
    SEASONAL_MODEL_PATH,
    WEEKLY_FEATURES,
    WEEKLY_MODEL_PATH,
)
from pipelines.model import compute_shap, load_artifacts, score_to_tier

# ---------------------------------------------------------------------------
# Modal app + image
# ---------------------------------------------------------------------------

app = modal.App("sunday-signals-inference")

_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "xgboost>=2.0",
        "scikit-learn",
        "shap",
        "joblib",
        "supabase",
        "pandas",
        "numpy",
        "fastapi[standard]",
    )
    # Include the local pipelines package so pipelines.constants / pipelines.model
    # are importable inside the container (Modal 1.x API).
    .add_local_python_source("pipelines")
)

_secrets = [modal.Secret.from_name("supabase-secrets")]


# ---------------------------------------------------------------------------
# Inference service
# ---------------------------------------------------------------------------

@app.cls(
    image=_image,
    secrets=_secrets,
    cpu=1,
    memory=512,
)
class InferenceService:
    """
    Stateful Modal class. The seasonal model is loaded once at container start.
    The weekly model is loaded lazily on first use — it won't exist until the
    in-season pipeline runs (September onwards).
    """

    @modal.enter()
    def load_models(self) -> None:
        from supabase import create_client

        self._client = create_client(
            os.environ["SUPABASE_URL"],
            os.environ["SUPABASE_SERVICE_KEY"],
        )
        self.seasonal_model, self.seasonal_scaler = load_artifacts(
            self._client.storage, SEASONAL_MODEL_PATH
        )
        # Weekly model is optional — not available until the NFL season starts.
        self.weekly_model = None
        self.weekly_scaler = None
        try:
            self.weekly_model, self.weekly_scaler = load_artifacts(
                self._client.storage, WEEKLY_MODEL_PATH
            )
        except Exception:
            pass  # will raise a clear error if a weekly score is requested

    @modal.method()
    def score(
        self,
        features: dict[str, float],
        model_type: str = "seasonal",
        feature_weights: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        """
        Score a single player's feature vector.

        Parameters
        ----------
        features        : flat dict of feature_name → value
        model_type      : "seasonal" or "weekly"
        feature_weights : optional per-feature multipliers from the UI weight slider

        Returns
        -------
        dict with keys: breakout_prob, risk_tier, shap_values, model_type
        """
        import pandas as pd

        if model_type == "weekly":
            if self.weekly_model is None:
                raise ValueError(
                    "Weekly model is not available — run train_weekly.py first "
                    "(NFL season starts in September)."
                )
            feat_cols = WEEKLY_FEATURES
            model     = self.weekly_model
        else:
            feat_cols = SEASONAL_FEATURES
            model     = self.seasonal_model

        X = pd.DataFrame([{f: features.get(f, 0.0) for f in feat_cols}]).astype(float)

        if feature_weights:
            for feat, weight in feature_weights.items():
                if feat in X.columns:
                    X[feat] = X[feat] * weight

        prob      = float(model.predict_proba(X)[0, 1])
        shap_vals = compute_shap(model, X)[0]

        return {
            "breakout_prob": prob,
            "risk_tier":     score_to_tier(prob),
            "shap_values":   shap_vals,
            "model_type":    model_type,
        }


# ---------------------------------------------------------------------------
# HTTP web endpoint (called by Vercel API routes)
# ---------------------------------------------------------------------------

@app.function(
    image=_image,
    secrets=_secrets,
)
@modal.fastapi_endpoint(method="POST")
def score_endpoint(body: dict[str, Any]) -> dict[str, Any]:
    """
    POST /score

    Request body:
        {
          "features":        {"age": 24, "target_share": 0.22, ...},
          "model_type":      "seasonal" | "weekly",   (optional, default "seasonal")
          "feature_weights": {"age": 1.5, ...}         (optional)
        }

    Response:
        {
          "breakout_prob": 0.71,
          "risk_tier":     "high",
          "shap_values":   {"age": 0.12, ...},
          "model_type":    "seasonal"
        }
    """
    service = InferenceService()
    return service.score.remote(
        features        = body.get("features", {}),
        model_type      = body.get("model_type", "seasonal"),
        feature_weights = body.get("feature_weights"),
    )
