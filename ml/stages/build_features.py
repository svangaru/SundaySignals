# ml/stages/build_features.py
"""
Build a per-(season, week, player_id) feature matrix from raw Parquet + dims, engineered to
capture:
  • Cross-team talent signal (global rolling performance)
  • Scheme/context signal (same-team rolling that resets on team change)
  • Role shocks around trades/injuries (team_change flag, usage deltas, DNP flags, games played)
  • Opponent strength (position-specific DVP)

Outputs to Supabase Storage:
  features/season={YYYY}/week={W}/features.parquet

Expected downstream:
  • train_cvplus.py learns to predict fantasy_points_ppr (p50) and we wrap with conformal intervals.

Run:
  modal run -m ml.modal_app::build_features -- --season 2025 --week 3

ENV (via Modal secret `supabase` or process env):
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
"""
from __future__ import annotations
import os
import io
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from supabase import create_client

SB_URL = os.environ["SUPABASE_URL"]
SB_SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
FEATURES_BUCKET = "features"
RAW_BUCKET = "raw"

# ----------------------------- Supabase Helpers ----------------------------- #

def _sb():
    return create_client(SB_URL, SB_SERVICE_KEY)


def _download_parquet(sb, bucket: str, path: str) -> pd.DataFrame:
    data = sb.storage.from_(bucket).download(path)
    return pd.read_parquet(io.BytesIO(data))


def _upload_parquet(sb, bucket: str, path: str, df: pd.DataFrame):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    try:
        sb.storage.from_(bucket).remove([path])
    except Exception:
        pass
    sb.storage.from_(bucket).upload(path, buf.getvalue(), file_options={"content-type": "application/octet-stream"})


def _fetch_dvp(sb, season: int, week: int) -> pd.DataFrame:
    res = sb.table("defense_vs_pos").select("season,week,team,position,dvp").eq("season", season).eq("week", week).execute()
    return pd.DataFrame(res.data or [])

# --------------------------- Feature Engineering --------------------------- #

TARGET = "fantasy_points_ppr"

BASIC_NUMERIC_CANDIDATES = [
    # volume/efficiency if present in weekly parquet
    "attempts", "completions", "pass_attempts", "passing_yards", "passing_tds", "interceptions",
    "rush_attempts", "rushing_yards", "rushing_tds",
    "targets", "receptions", "receiving_yards", "receiving_tds",
    "snap_share", "route_participation", "air_yards",
]

# Columns that imply DNP/active state if present
SNAP_LIKE = ["snap_share", "snaps", "offensive_snaps"]


def _attach_schedule(weekly: pd.DataFrame, schedule: pd.DataFrame) -> pd.DataFrame:
    # schedule: (season, week, team, opp, home)
    sched = schedule[["season", "week", "team", "opp", "home"]].drop_duplicates()
    return weekly.merge(sched, on=["season", "week", "team"], how="left")


def _attach_dvp(df: pd.DataFrame, dvp: pd.DataFrame) -> pd.DataFrame:
    if dvp.empty:
        df["opp_dvp"] = np.nan
        return df
    dvp2 = dvp.rename(columns={"team": "opp", "dvp": "opp_dvp"})
    return df.merge(
        dvp2[["season", "week", "opp", "position", "opp_dvp"]],
        on=["season", "week", "opp", "position"], how="left"
    )


def _safe_col(df: pd.DataFrame, name: str) -> pd.Series:
    return df[name] if name in df.columns else pd.Series(np.nan, index=df.index)


def _compute_team_change_and_rollings(df: pd.DataFrame) -> pd.DataFrame:
    """Adds:
      team_change (0/1),
      rolling_fp3 (cross-team talent; leak-safe),
      rolling_fp3_same_team (scheme/context; resets on team change),
      games_played_last3 (count of non-DNP prior 3 games),
      dnp_prev (1 if previous week had zero snaps or missing),
      delta_snap, delta_targets, delta_rush_att (last week deltas).
    """
    # Sort for time-aware ops
    df = df.sort_values(["player_id", "season", "week"]).reset_index(drop=True)

    # DNP: try snaps/snap_share proxies
    snap_proxy = None
    for c in SNAP_LIKE:
        if c in df.columns:
            snap_proxy = c
            break
    if snap_proxy is None:
        # fabricate a snap proxy from targets+rush_attempts+pass_attempts presence
        proxy = (
            _safe_col(df, "targets").fillna(0)
            + _safe_col(df, "rush_attempts").fillna(0)
            + _safe_col(df, "pass_attempts").fillna(0)
        )
        df["_snap_proxy"] = proxy
        snap_proxy = "_snap_proxy"

    # team_change flag
    prev_team = df.groupby("player_id")["team"].shift(1)
    df["team_change"] = (df["team"] != prev_team).astype("int8")

    # rolling_fp3 (cross-team talent): shift(1) to avoid leakage
    if TARGET in df.columns:
        df["rolling_fp3"] = (
            df.groupby("player_id")[TARGET]
              .apply(lambda s: s.shift(1).rolling(3, min_periods=1).mean())
        )
    else:
        df["rolling_fp3"] = np.nan

    # rolling_fp3_same_team: reset after team change
    def rolling_same_team_mean(g: pd.DataFrame) -> pd.Series:
        out = []
        window: List[float] = []
        prev_t = None
        for _, row in g.iterrows():
            # produce value BEFORE adding current week (leak-safe)
            out.append(np.mean(window[-3:]) if window else np.nan)
            # reset window if team changed from previous week
            cur_t = row["team"]
            if prev_t is not None and cur_t != prev_t:
                window = []
            # update window with last week's actual if present
            v = row.get(TARGET, np.nan)
            if pd.notna(v):
                window.append(float(v))
            prev_t = cur_t
        return pd.Series(out, index=g.index)

    df["rolling_fp3_same_team"] = (
        df.groupby("player_id", group_keys=False).apply(rolling_same_team_mean)
    )

    # dnp_prev: previous week had zero/NaN snap proxy
    prev_snap = df.groupby("player_id")[snap_proxy].shift(1)
    df["dnp_prev"] = (
        (prev_snap.isna()) | (prev_snap.fillna(0) == 0)
    ).astype("int8")

    # games_played_last3: count of non-DNP prior 3 games
    played_prev = (~((_safe_col(df, snap_proxy).fillna(0) == 0))).astype(int)
    df["games_played_last3"] = (
        df.groupby("player_id")["dnp_prev"]
          .apply(lambda s: (1 - s).rolling(3, min_periods=1).sum().shift(0))  # uses dnp_prev already shifted
    )

    # Usage deltas (last-week deltas)
    for col, newname in [("snap_share", "delta_snap"), ("targets", "delta_targets"), ("rush_attempts", "delta_rush_att")]:
        if col in df.columns:
            prev = df.groupby("player_id")[col].shift(1)
            df[newname] = df[col] - prev
        else:
            df[newname] = np.nan

    # Clean temp
    if "_snap_proxy" in df.columns:
        df.drop(columns=["_snap_proxy"], inplace=True)

    return df


def _final_select(df: pd.DataFrame) -> pd.DataFrame:
    # Minimal stable set (keep what exists)
    base = ["season", "week", "player_id", "team", "position", "opp", "home"]
    engineered = [
        "opp_dvp",
        "team_change",
        "rolling_fp3",
        "rolling_fp3_same_team",
        "games_played_last3",
        "dnp_prev",
        "delta_snap", "delta_targets", "delta_rush_att",
    ]
    numeric = [c for c in BASIC_NUMERIC_CANDIDATES if c in df.columns]
    target = [TARGET] if TARGET in df.columns else []

    keep = [c for c in base + engineered + numeric + target if c in df.columns]
    out = df[keep].copy()

    # Ensure types
    if "home" in out.columns:
        out["home"] = out["home"].astype("float32")  # easier for XGB; 1.0/0.0
    if "team_change" in out.columns:
        out["team_change"] = out["team_change"].astype("float32")
    if "dnp_prev" in out.columns:
        out["dnp_prev"] = out["dnp_prev"].astype("float32")

    return out

# ----------------------------------- Run ----------------------------------- #

def run(season: int, week: int):
    sb = _sb()

    weekly_path = f"nfl/{season}/weekly.parquet"
    schedule_path = f"nfl/{season}/schedule.parquet"

    weekly = _download_parquet(sb, RAW_BUCKET, weekly_path)
    schedule = _download_parquet(sb, RAW_BUCKET, schedule_path)

    # Filter to target week rows and keep common columns
    weekly_w = weekly[(weekly["season"] == season) & (weekly["week"] == week)].copy()

    # Ensure required identity columns exist
    required = ["season", "week", "player_id", "team", "position"]
    missing = [c for c in required if c not in weekly_w.columns]
    if missing:
        raise ValueError(f"weekly parquet missing columns: {missing}")

    # Attach schedule-derived opp/home (team context for that week)
    df = _attach_schedule(weekly_w, schedule)

    # Opponent DVP for position-week
    dvp = _fetch_dvp(sb, season, week)
    df = _attach_dvp(df, dvp)

    # Role/trade/injury-aware engineered features
    df = _compute_team_change_and_rollings(df)

    # Final selection & typing
    out = _final_select(df)

    # Save
    out_path = f"season={season}/week={week}/features.parquet"
    _upload_parquet(sb, FEATURES_BUCKET, out_path, out)

    pos_counts = out["position"].value_counts().to_dict() if "position" in out.columns else {}
    print(
        f"[build_features] season={season} week={week} rows={len(out)} by_pos={pos_counts} -> {FEATURES_BUCKET}/{out_path}"
    )
    return {"ok": True, "season": season, "week": week, "rows": int(len(out)), "by_pos": pos_counts}
