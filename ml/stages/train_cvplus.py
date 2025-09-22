# ml/stages/train_cvplus.py
import os, numpy as np
from typing import Dict, Any

def _client():
    from supabase import create_client
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])

def _load_features(season:int, week:int):
    # In this minimal flow, we re-run feature builder inline to avoid Storage
    from . import build_features
    return build_features.run(season, week)

def run(season:int, week:int, learner:str="xgb") -> Dict[str, Any]:
    fx = _load_features(season, week)
    if fx.empty:
        return {"ok": False, "reason": "no features"}
    # toy target: fabricate a target so the pipeline runs (replace with real y later)
    rng = np.random.default_rng(42)
    y = (10 + 0.4*fx["opp_dvp"].values + 0.2*fx["home"].values - 0.1*fx["spread"].values
         + rng.normal(0, 1.5, size=len(fx))).astype(float)

    X = fx[["home","pos_id","opp_dvp","spread"]].values

    model = None
    try:
        if learner == "xgb":
            from xgboost import XGBRegressor
            model = XGBRegressor(
                max_depth=6, learning_rate=0.05, subsample=0.8, colsample_bytree=0.8,
                min_child_weight=5, reg_lambda=2.0, n_estimators=200, tree_method="hist"
            ).fit(X, y)
        else:
            from lightgbm import LGBMRegressor
            model = LGBMRegressor(
                num_leaves=63, learning_rate=0.05, feature_fraction=0.8,
                bagging_fraction=0.8, bagging_freq=1, min_data_in_leaf=50,
                n_estimators=200, force_col_wise=True
            ).fit(X, y)
    except Exception as e:
        print("[train] fallback to mean due to error:", e)

    # CV+ stub: use residuals from a quick split (or fallback)
    if model is not None:
        preds = model.predict(X)
    else:
        preds = np.full(len(y), y.mean())
    resid = np.abs(y - preds)
    q_alpha = float(np.quantile(resid, 0.80)) if len(resid) else 3.0

    state = {
        "ok": True,
        "learner": learner,
        "q_alpha": q_alpha,
        # pack a tiny serializable “model”: either booster dump or params+coef
        "model_type": "mean" if model is None else learner,
        "model_payload": None if model is None else model.get_booster().save_raw().decode("utf-8") if learner=="xgb" else None
    }
    print(f"[train] q_alpha={q_alpha:.3f}, model={state['model_type']}")
    return state
