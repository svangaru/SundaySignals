# ml/stages/infer_batch.py
import os, numpy as np, datetime as dt
from typing import Dict, Any

def _client():
    from supabase import create_client
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])

def _build_features(season:int, week:int):
    from . import build_features
    return build_features.run(season, week)

def _load_model_state(season:int, week:int, learner:str="xgb") -> Dict[str, Any]:
    # Minimal: retrain inline (stateless). Later: read from Storage.
    from . import train_cvplus
    return train_cvplus.run(season, week, learner=learner)

def run(season:int, week:int, learner:str="xgb"):
    sb = _client()
    fx = _build_features(season, week)
    if fx.empty:
        print("[infer] no features — skipping"); return {"ok": False}

    st = _load_model_state(season, week, learner=learner)
    q_alpha = float(st.get("q_alpha", 3.0))

    X = fx[["home","pos_id","opp_dvp","spread"]].values
    p50 = 10 + 0.4*fx["opp_dvp"].values + 0.2*fx["home"].values - 0.1*fx["spread"].values

    # if we had a real serialized model, we’d load and predict here
    lo = p50 - q_alpha
    hi = p50 + q_alpha

    now = dt.datetime.utcnow()
    valid_until = (now + dt.timedelta(days=7)).isoformat() + "Z"
    pk = f"season#{season}#week#{week}"

    rows = []
    for pid, p, l, h in zip(fx["player_id"].values, p50, lo, hi):
        rows.append({
            "pk": pk,
            "sk": f"player#{pid}",
            "p50": float(p),
            "lo": float(l),
            "hi": float(h),
            "valid_until": valid_until
        })

    # upsert
    for i in range(0, len(rows), 500):
        sb.table("pred_cache").upsert(rows[i:i+500]).execute()

    print(f"[infer] wrote {len(rows)} rows into pred_cache for {pk}")
    return {"ok": True, "count": len(rows)}
