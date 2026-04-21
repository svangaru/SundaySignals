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


def _filter_positions(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to skill positions, normalising the position column name."""
    for col in ("position", "player_position"):
        if col in df.columns:
            df = df[df[col].isin(POSITIONS)].copy()
            if col != "position":
                df = df.rename(columns={col: "position"})
            return df
    return df.copy()


def load_seasonal(seasons: list[int]) -> pd.DataFrame:
    df = nfl.import_seasonal_data(seasons, s_type="REG")
    df = _filter_positions(df)
    rename: dict[str, str] = {}
    if "recent_team" in df.columns:
        rename["recent_team"] = "team"
    if "wopr_y" in df.columns:
        rename["wopr_y"] = "wopr"
    if "wopr_x" in df.columns and "wopr" not in rename.values() and "wopr" not in df.columns:
        rename["wopr_x"] = "wopr"
    df = df.rename(columns=rename)
    # team column may be absent from newer nfl_data_py seasonal data — backfill from weekly
    if "team" not in df.columns:
        try:
            weekly = nfl.import_weekly_data(seasons)
            team_map = (
                weekly[["player_id", "season", "recent_team", "week"]]
                .sort_values("week")
                .groupby(["player_id", "season"], as_index=False)
                .last()[["player_id", "season", "recent_team"]]
                .rename(columns={"recent_team": "team"})
            )
            df = df.merge(team_map, on=["player_id", "season"], how="left")
        except Exception:
            df["team"] = pd.NA
    return df


def load_snap_counts(seasons: list[int]) -> pd.DataFrame:
    df = nfl.import_snap_counts(seasons)
    df = _filter_positions(df)
    if "offense_pct" in df.columns and "snap_pct" not in df.columns:
        df = df.rename(columns={"offense_pct": "snap_pct"})
    # snap counts use pfr_player_id; map to GSIS player_id so downstream joins work
    if "player_id" not in df.columns and "pfr_player_id" in df.columns:
        try:
            id_map = nfl.import_ids()[["pfr_id", "gsis_id"]].dropna()
            id_map = id_map.rename(columns={"pfr_id": "pfr_player_id", "gsis_id": "player_id"})
            id_map = id_map.drop_duplicates("pfr_player_id")
            df = df.merge(id_map, on="pfr_player_id", how="left")
        except Exception:
            df["player_id"] = pd.NA
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
    """Age and draft_number from weekly rosters (earliest week per player-season).

    nfl_data_py + pandas 2.x can raise a ValueError due to duplicate index
    labels inside import_weekly_rosters. We reset the index as a precaution
    and fall back to an empty DataFrame so the pipeline continues — age and
    draft_number will be null but all other features are unaffected.
    """
    try:
        df = nfl.import_weekly_rosters(seasons)
        df = df.reset_index(drop=True)
        df = _filter_positions(df)
        df = df.sort_values("week").groupby(["player_id", "season"], as_index=False).first()
        keep = [c for c in ["player_id", "season", "age", "draft_number"] if c in df.columns]
        return df[keep].copy()
    except Exception:
        return pd.DataFrame(columns=["player_id", "season", "age", "draft_number"])


def load_weekly_stats(season: int) -> pd.DataFrame:
    df = nfl.import_weekly_data([season])
    df = _filter_positions(df)
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
