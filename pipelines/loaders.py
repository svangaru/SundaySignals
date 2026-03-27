"""
Data loaders — thin wrappers around nfl_data_py.
All column normalisation happens here so the rest of the pipeline
can assume consistent names.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import nfl_data_py as nfl

from pipelines.constants import POSITIONS


def load_seasonal(seasons: list[int]) -> pd.DataFrame:
    df = nfl.import_seasonal_data(seasons, s_type="REG")
    df = df[df["position"].isin(POSITIONS)].copy()
    rename: dict[str, str] = {}
    if "recent_team" in df.columns:
        rename["recent_team"] = "team"
    if "wopr_y" in df.columns:
        rename["wopr_y"] = "wopr"
    return df.rename(columns=rename)


def load_snap_counts(seasons: list[int]) -> pd.DataFrame:
    df = nfl.import_snap_counts(seasons)
    df = df[df["position"].isin(POSITIONS)].copy()
    # nfl_data_py uses offense_pct; normalise to snap_pct
    if "offense_pct" in df.columns and "snap_pct" not in df.columns:
        df = df.rename(columns={"offense_pct": "snap_pct"})
    return df


def load_pbp(seasons: list[int]) -> pd.DataFrame:
    cols = [
        "season", "week", "posteam",
        "pass_attempt", "complete_pass",
        "receiver_player_id", "receiver_player_name",
        "air_yards", "yardline_100",
        "yards_gained", "epa",
    ]
    return nfl.import_pbp_data(seasons, columns=cols)


def load_roster_meta(seasons: list[int]) -> pd.DataFrame:
    """Age and draft_number from weekly rosters (earliest week per player-season)."""
    df = nfl.import_weekly_rosters(seasons)
    df = df[df["position"].isin(POSITIONS)].copy()
    df = df.sort_values("week").groupby(["player_id", "season"], as_index=False).first()
    keep = [c for c in ["player_id", "season", "age", "draft_number"] if c in df.columns]
    return df[keep].copy()


def load_weekly_stats(season: int) -> pd.DataFrame:
    df = nfl.import_weekly_data([season])
    df = df[df["position"].isin(POSITIONS)].copy()
    rename: dict[str, str] = {}
    if "recent_team" in df.columns:
        rename["recent_team"] = "team"
    if "fantasy_points_ppr" in df.columns:
        rename["fantasy_points_ppr"] = "ppr_points"
    return df.rename(columns=rename)


def load_context_flags(path: str | Path | None) -> pd.DataFrame | None:
    """Load overrides/context_flags.csv. Returns None if path is missing."""
    if path is None:
        return None
    p = Path(path)
    if not p.exists():
        return None
    df = pd.read_csv(p, dtype={"player_id": str})
    for col in ("oc_change", "qb_change"):
        if col in df.columns:
            df[col] = df[col].astype(bool)
    return df[["player_id", "season", "oc_change", "qb_change"]]
