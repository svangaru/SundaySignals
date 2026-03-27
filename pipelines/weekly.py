"""
Weekly feature engineering (in-season model → player_weeks table).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from pipelines.constants import WEEKLY_COLS
from pipelines.loaders import load_pbp, load_snap_counts, load_weekly_stats


def build_weekly_features(
    season: int,
    weeks: list[int] | None = None,
) -> pd.DataFrame:
    """
    Build one row per (player_id, season, week) with all in-season features.
    Covers RB/WR/TE only.

    Parameters
    ----------
    season : int
    weeks : list[int] or None
        Specific weeks to include. None returns all available weeks.

    Returns
    -------
    pd.DataFrame with columns == WEEKLY_COLS.
    """
    weekly = load_weekly_stats(season)
    snaps  = load_snap_counts([season])
    pbp    = load_pbp([season])

    if weeks is not None:
        weekly = weekly[weekly["week"].isin(weeks)]
        snaps  = snaps[snaps["week"].isin(weeks)]
        pbp    = pbp[pbp["week"].isin(weeks)]

    base = weekly.merge(
        snaps[["player_id", "season", "week", "snap_pct"]],
        on=["player_id", "season", "week"],
        how="left",
    )

    rz = _compute_red_zone_targets(pbp)
    base = base.merge(rz, on=["player_id", "season", "week"], how="left")

    rp = _compute_route_participation(snaps)
    base = base.merge(rp, on=["player_id", "season", "week"], how="left")

    base = _add_rolling_features(base)

    for col in WEEKLY_COLS:
        if col not in base.columns:
            base[col] = np.nan

    return base[WEEKLY_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compute_red_zone_targets(pbp: pd.DataFrame) -> pd.DataFrame:
    """Count pass targets inside the opponent 20 per (player_id, season, week)."""
    rz = pbp[
        (pbp["pass_attempt"] == 1)
        & (pbp["yardline_100"] <= 20)
        & pbp["receiver_player_id"].notna()
    ]
    return (
        rz.groupby(["receiver_player_id", "season", "week"])
        .size()
        .reset_index(name="red_zone_targets")
        .rename(columns={"receiver_player_id": "player_id"})
    )


def _compute_route_participation(snaps: pd.DataFrame) -> pd.DataFrame:
    """
    route_participation = routes_ran / offense_snaps per (player_id, season, week).
    Returns empty DataFrame when routes_ran is unavailable in snap counts.
    """
    if "routes_ran" not in snaps.columns or "offense_snaps" not in snaps.columns:
        return pd.DataFrame(columns=["player_id", "season", "week", "route_participation"])

    df = snaps[["player_id", "season", "week", "routes_ran", "offense_snaps"]].copy()
    df["route_participation"] = df["routes_ran"] / df["offense_snaps"].replace(0, np.nan)
    return df[["player_id", "season", "week", "route_participation"]]


def _add_rolling_features(base: pd.DataFrame) -> pd.DataFrame:
    """
    Add rolling 3-week averages (_r3) and week-over-prior-3-week deltas (_delta_r3)
    for snap_pct, target_share, and air_yards.
    """
    base = base.sort_values(["player_id", "season", "week"]).copy()

    for col in ["snap_pct", "target_share", "air_yards"]:
        if col not in base.columns:
            base[col] = np.nan
        base[f"{col}_r3"] = (
            base.groupby("player_id")[col]
            .transform(lambda s: s.rolling(3, min_periods=1).mean())
        )

    for col in ["snap_pct", "target_share"]:
        prior_r3 = base.groupby("player_id")[col].transform(
            lambda s: s.shift(1).rolling(3, min_periods=1).mean()
        )
        base[f"{col}_delta_r3"] = base[col] - prior_r3

    return base
