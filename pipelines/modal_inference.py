"""
modal_inference.py — Modal serverless inference service for SundaySignals.

InferenceService loads both models once on container startup (warm-start caching)
and exposes a score() method for RPC. The score_endpoint web function wraps it
as an HTTP POST endpoint callable from Vercel API routes.

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
    )
)

# Mount the local pipelines package so pipelines.constants / pipelines.model
# are importable inside the container without a pip install step.
_pipelines_mount = modal.Mount.from_local_python_packages("pipelines")

_secrets = [modal.Secret.from_name("supabase-secrets")]


# ---------------------------------------------------------------------------
# Inference service
# ---------------------------------------------------------------------------

@app.cls(
    image=_image,
    mounts=[_pipelines_mount],
    secrets=_secrets,
    cpu=1,
    memory=512,
)
class InferenceService:
    """
    Stateful Modal class. Both models are loaded once when the container starts
    and reused across all requests to the same warm container instance.
    """

    @modal.enter()
    def load_models(self) -> None:
        from supabase import create_client

        client = create_client(
            os.environ["SUPABASE_URL"],
            os.environ["SUPABASE_SERVICE_KEY"],
        )
        self.seasonal_model, self.seasonal_scaler = load_artifacts(
            client.storage, SEASONAL_MODEL_PATH
        )
        self.weekly_model, self.weekly_scaler = load_artifacts(
            client.storage, WEEKLY_MODEL_PATH
        )

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

        feat_cols = SEASONAL_FEATURES if model_type == "seasonal" else WEEKLY_FEATURES
        model     = self.seasonal_model if model_type == "seasonal" else self.weekly_model

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
    mounts=[_pipelines_mount],
    secrets=_secrets,
)
@modal.web_endpoint(method="POST")
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
