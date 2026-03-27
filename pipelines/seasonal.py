"""
Seasonal feature engineering (pre-season model → player_seasons table).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from pipelines.constants import (
    BREAKOUT_MIN_GAMES,
    BREAKOUT_PPG_THRESHOLD,
    SEASONAL_COLS,
)
from pipelines.loaders import (
    load_context_flags,
    load_pbp,
    load_roster_meta,
    load_seasonal,
    load_snap_counts,
)
from pipelines.utils import three_point_slope


def build_seasonal_features(
    seasons: list[int],
    context_flags_path: str | Path | None = None,
) -> pd.DataFrame:
    """
    Build one row per (player_id, season) with all pre-season model features.
    Covers RB/WR/TE only.

    Parameters
    ----------
    seasons : list[int]
        Target seasons, e.g. [2020, 2021, 2022, 2023, 2024].
        Two extra prior seasons are fetched internally for lag features.
    context_flags_path : path-like or None
        Path to overrides/context_flags.csv. Missing file is silently ignored.

    Returns
    -------
    pd.DataFrame with columns == SEASONAL_COLS, one row per (player_id, season).
    """
    fetch_seasons = sorted(set(seasons) | {min(seasons) - 1, min(seasons) - 2})

    seasonal = load_seasonal(fetch_seasons)
    snaps    = load_snap_counts(fetch_seasons)
    pbp      = load_pbp(fetch_seasons)
    roster   = load_roster_meta(fetch_seasons)
    context  = load_context_flags(context_flags_path)

    base = seasonal[seasonal["season"].isin(seasons)].copy()
    base = base.merge(roster, on=["player_id", "season"], how="left")
    base = _add_target_share_trend(base, seasonal)
    base = _add_snap_share_trend(base, snaps)

    yprr = _compute_yprr(pbp, snaps)
    base = base.merge(yprr, on=["player_id", "season"], how="left")

    team_pass = _compute_team_pass_vol_trend(pbp, seasons)
    base = base.merge(team_pass, on=["team", "season"], how="left")

    if context is not None:
        base = base.merge(context, on=["player_id", "season"], how="left")
    for col in ("oc_change", "qb_change"):
        if col not in base.columns:
            base[col] = False
        base[col] = base[col].fillna(False).astype(bool)

    base["fantasy_ppg"] = base["fantasy_points_ppr"] / base["games"].replace(0, np.nan)
    base = _add_broke_out(base, seasonal)

    for col in SEASONAL_COLS:
        if col not in base.columns:
            base[col] = np.nan

    return base[SEASONAL_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _add_target_share_trend(
    base: pd.DataFrame, seasonal: pd.DataFrame
) -> pd.DataFrame:
    """Merge target_share_y1, target_share_y2, and target_share_slope onto base."""
    ts = seasonal[["player_id", "season", "target_share"]].copy()

    y1 = ts.copy()
    y1["season"] = y1["season"] + 1
    y1 = y1.rename(columns={"target_share": "target_share_y1"})

    y2 = ts.copy()
    y2["season"] = y2["season"] + 2
    y2 = y2.rename(columns={"target_share": "target_share_y2"})

    base = base.merge(y1, on=["player_id", "season"], how="left")
    base = base.merge(y2, on=["player_id", "season"], how="left")
    base["target_share_slope"] = three_point_slope(
        base["target_share_y2"], base["target_share_y1"], base["target_share"]
    )
    return base


def _add_snap_share_trend(
    base: pd.DataFrame, snaps: pd.DataFrame
) -> pd.DataFrame:
    """Compute season-level snap means, then merge y0/y1/y2 lags and slope."""
    annual = (
        snaps.groupby(["player_id", "season"])["snap_pct"]
        .mean()
        .reset_index()
        .rename(columns={"snap_pct": "snap_share_y0"})
    )

    y1 = annual.copy()
    y1["season"] = y1["season"] + 1
    y1 = y1.rename(columns={"snap_share_y0": "snap_share_y1"})

    y2 = annual.copy()
    y2["season"] = y2["season"] + 2
    y2 = y2.rename(columns={"snap_share_y0": "snap_share_y2"})

    base = base.merge(annual, on=["player_id", "season"], how="left")
    base = base.merge(y1, on=["player_id", "season"], how="left")
    base = base.merge(y2, on=["player_id", "season"], how="left")
    base["snap_share_slope"] = three_point_slope(
        base["snap_share_y2"], base["snap_share_y1"], base["snap_share_y0"]
    )
    return base


def _compute_yprr(pbp: pd.DataFrame, snaps: pd.DataFrame) -> pd.DataFrame:
    """
    Yards per route run per (player_id, season).
    Uses snaps['routes_ran'] when available; falls back to target count from PBP.
    """
    rec_yards = (
        pbp[pbp["pass_attempt"] == 1]
        .groupby(["receiver_player_id", "season"])["yards_gained"]
        .sum()
        .reset_index()
        .rename(columns={"receiver_player_id": "player_id", "yards_gained": "rec_yards"})
    )

    if "routes_ran" in snaps.columns:
        routes = (
            snaps.groupby(["player_id", "season"])["routes_ran"]
            .sum()
            .reset_index()
        )
        df = rec_yards.merge(routes, on=["player_id", "season"], how="left")
        df["yprr"] = df["rec_yards"] / df["routes_ran"].replace(0, np.nan)
    else:
        targets = (
            pbp[pbp["pass_attempt"] == 1]
            .groupby(["receiver_player_id", "season"])
            .size()
            .reset_index(name="target_count")
            .rename(columns={"receiver_player_id": "player_id"})
        )
        df = rec_yards.merge(targets, on=["player_id", "season"], how="left")
        df["yprr"] = df["rec_yards"] / df["target_count"].replace(0, np.nan)

    return df[["player_id", "season", "yprr"]]


def _compute_team_pass_vol_trend(
    pbp: pd.DataFrame, target_seasons: list[int]
) -> pd.DataFrame:
    """
    For each (team, target_season S), compute the linear slope of team pass attempts
    over S-3, S-2, S-1. Pre-season safe: no current-season leakage.
    """
    pass_vol = (
        pbp[pbp["pass_attempt"] == 1]
        .groupby(["posteam", "season"])
        .size()
        .reset_index(name="pass_attempts")
        .rename(columns={"posteam": "team"})
    )

    rows: list[dict] = []
    for season in target_seasons:
        prior_seasons = [season - 3, season - 2, season - 1]
        for team, grp in pass_vol.groupby("team"):
            prior = (
                grp[grp["season"].isin(prior_seasons)]
                .sort_values("season")
                .reset_index(drop=True)
            )
            slope = (
                float(np.polyfit(prior.index, prior["pass_attempts"].values, 1)[0])
                if len(prior) >= 2 else np.nan
            )
            rows.append({"team": team, "season": season, "team_pass_vol_trend": slope})

    return pd.DataFrame(rows)


def _add_broke_out(base: pd.DataFrame, seasonal: pd.DataFrame) -> pd.DataFrame:
    """
    broke_out = True if fantasy_ppg increased ≥30% YoY AND games >= BREAKOUT_MIN_GAMES.
    Rows with no prior-year data get pd.NA (unknown ground truth).
    """
    prior = seasonal[["player_id", "season", "fantasy_points_ppr", "games"]].copy()
    prior["fantasy_ppg_prior"] = prior["fantasy_points_ppr"] / prior["games"].replace(0, np.nan)
    prior = prior[["player_id", "season", "fantasy_ppg_prior"]].copy()
    prior["season"] = prior["season"] + 1

    base = base.merge(prior, on=["player_id", "season"], how="left")

    met_threshold = (
        base["fantasy_ppg"] >= base["fantasy_ppg_prior"] * (1 + BREAKOUT_PPG_THRESHOLD)
    )
    base["broke_out"] = (met_threshold & (base["games"] >= BREAKOUT_MIN_GAMES)).astype("boolean")
    base.loc[base["fantasy_ppg_prior"].isna(), "broke_out"] = pd.NA

    return base.drop(columns=["fantasy_ppg_prior"])
