"""
Schema validation tests.

Validates that the DataFrames produced by the feature engineering pipelines
match the exact column lists and types expected by the Supabase schema.

These tests mock nfl_data_py loaders — no network or DB calls needed.
Run with: pytest tests/test_schema.py -v
"""

from __future__ import annotations

import pandas as pd
import numpy as np
import pytest
from unittest.mock import patch

from pipelines.constants import SEASONAL_COLS, WEEKLY_COLS


# ---------------------------------------------------------------------------
# Expected Supabase column types (mirrors schema.sql)
# ---------------------------------------------------------------------------

SEASONAL_SCHEMA: dict[str, type | tuple] = {
    "player_id":           str,
    "season":              (int, np.integer),
    "player_name":         (str, type(None)),
    "position":            (str, type(None)),
    "team":                (str, type(None)),
    "age":                 (float, int, np.floating, np.integer, type(None)),
    "draft_number":        (float, int, np.floating, np.integer, type(None)),
    "games":               (int, float, np.integer, np.floating, type(None)),
    "targets":             (int, float, np.integer, np.floating, type(None)),
    "receptions":          (int, float, np.integer, np.floating, type(None)),
    "receiving_yards":     (float, int, np.floating, np.integer, type(None)),
    "receiving_tds":       (int, float, np.integer, np.floating, type(None)),
    "carries":             (int, float, np.integer, np.floating, type(None)),
    "rushing_yards":       (float, int, np.floating, np.integer, type(None)),
    "target_share":        (float, np.floating, type(None)),
    "air_yards_share":     (float, np.floating, type(None)),
    "wopr":                (float, np.floating, type(None)),
    "dom":                 (float, np.floating, type(None)),
    "w8dom":               (float, np.floating, type(None)),
    "receiving_epa":       (float, np.floating, type(None)),
    "racr":                (float, np.floating, type(None)),
    "fantasy_points_ppr":  (float, np.floating, type(None)),
    "target_share_y1":     (float, np.floating, type(None)),
    "target_share_y2":     (float, np.floating, type(None)),
    "target_share_slope":  (float, np.floating, type(None)),
    "snap_share_y0":       (float, np.floating, type(None)),
    "snap_share_y1":       (float, np.floating, type(None)),
    "snap_share_y2":       (float, np.floating, type(None)),
    "snap_share_slope":    (float, np.floating, type(None)),
    "yprr":                (float, np.floating, type(None)),
    "team_pass_vol_trend": (float, np.floating, type(None)),
    "oc_change":           (bool, np.bool_, type(None)),
    "qb_change":           (bool, np.bool_, type(None)),
    "fantasy_ppg":         (float, np.floating, type(None)),
    "broke_out":           (bool, np.bool_, int, np.integer, type(None)),
}

WEEKLY_SCHEMA: dict[str, type | tuple] = {
    "player_id":             str,
    "season":                (int, np.integer),
    "week":                  (int, np.integer),
    "snap_pct":              (float, np.floating, type(None)),
    "targets":               (int, float, np.integer, np.floating, type(None)),
    "target_share":          (float, np.floating, type(None)),
    "air_yards":             (float, np.floating, type(None)),
    "air_yards_share":       (float, np.floating, type(None)),
    "red_zone_targets":      (int, float, np.integer, np.floating, type(None)),
    "route_participation":   (float, np.floating, type(None)),
    "ppr_points":            (float, np.floating, type(None)),
    "receiving_epa":         (float, np.floating, type(None)),
    "snap_pct_r3":           (float, np.floating, type(None)),
    "target_share_r3":       (float, np.floating, type(None)),
    "air_yards_r3":          (float, np.floating, type(None)),
    "snap_pct_delta_r3":     (float, np.floating, type(None)),
    "target_share_delta_r3": (float, np.floating, type(None)),
}


def _check_column_types(df: pd.DataFrame, schema: dict) -> list[str]:
    """Return a list of error messages for any type mismatches."""
    errors = []
    for col, expected in schema.items():
        if col not in df.columns:
            errors.append(f"Column '{col}' missing from DataFrame")
            continue
        for val in df[col].dropna():
            if not isinstance(val, expected):
                errors.append(
                    f"Column '{col}': expected {expected}, got {type(val).__name__} (value={val!r})"
                )
            break  # one sample is enough per column
    return errors


# ---------------------------------------------------------------------------
# Shared mock data (same as test_features.py, kept self-contained)
# ---------------------------------------------------------------------------

def _make_seasonal_df():
    rows = []
    for s in [2020, 2021, 2022, 2023, 2024]:
        for pid, name, pos in [("ABC123", "Justin Jefferson", "WR"),
                                ("DEF456", "Davante Adams", "WR"),
                                ("GHI789", "Dalvin Cook", "RB")]:
            rows.append({
                "player_id": pid, "season": s, "player_name": name,
                "position": pos, "recent_team": "MIN",
                "target_share": 0.28, "air_yards_share": 0.35,
                "wopr_y": 0.55, "dom": 0.40, "w8dom": 0.42,
                "receiving_epa": 18.0, "racr": 1.8,
                "fantasy_points_ppr": 280.0, "games": 17,
                "targets": 140, "receptions": 105,
                "receiving_yards": 1400.0, "receiving_tds": 9,
                "carries": 0, "rushing_yards": 0.0,
            })
    return pd.DataFrame(rows)


def _make_snap_df():
    rows = []
    for s in [2020, 2021, 2022, 2023, 2024]:
        for pid in ["ABC123", "DEF456", "GHI789"]:
            for wk in range(1, 18):
                rows.append({
                    "player_id": pid, "season": s, "week": wk,
                    "offense_pct": 0.85, "position": "WR",
                })
    return pd.DataFrame(rows)


def _make_roster_df():
    rows = []
    for s in [2020, 2021, 2022, 2023, 2024]:
        for pid, draft in [("ABC123", 22), ("DEF456", 14), ("GHI789", 5)]:
            rows.append({
                "gsis_id": pid, "season": s,
                "birth_date": "1998-06-16", "entry_year": 2020,
                "draft_number": float(draft),
            })
    return pd.DataFrame(rows)


def _make_pbp_df():
    rows = []
    for s in [2020, 2021, 2022, 2023, 2024]:
        for pid in ["ABC123", "DEF456", "GHI789"]:
            for wk in range(1, 18):
                rows.append({
                    "receiver_player_id": pid, "season": s, "week": wk,
                    "pass_attempt": 1, "complete_pass": 1,
                    "receiving_yards": 10, "yardline_100": 25,
                    "route": "SLANT", "play_type": "pass",
                    "posteam": "MIN", "pass_attempt_team": 1,
                })
    return pd.DataFrame(rows)


def _make_weekly_df():
    rows = []
    for pid, name, pos in [("ABC123", "Justin Jefferson", "WR"),
                            ("DEF456", "Davante Adams", "WR"),
                            ("GHI789", "Dalvin Cook", "RB")]:
        for wk in range(1, 10):
            rows.append({
                "player_id": pid, "season": 2024, "week": wk,
                "player_name": name, "position": pos,
                "recent_team": "MIN",
                "targets": 8, "target_share": 0.28,
                "air_yards": 85.0, "air_yards_share": 0.32,
                "fantasy_points_ppr": 18.5, "receiving_epa": 2.1,
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Schema tests — seasonal
# ---------------------------------------------------------------------------

class TestSeasonalSchema:
    @pytest.fixture(autouse=True)
    def patch_loaders(self):
        with patch("pipelines.loaders.nfl.import_seasonal_data", return_value=_make_seasonal_df()), \
             patch("pipelines.loaders.nfl.import_snap_counts", return_value=_make_snap_df()), \
             patch("pipelines.loaders.nfl.import_weekly_rosters", return_value=_make_roster_df()), \
             patch("pipelines.loaders.nfl.import_pbp_data", return_value=_make_pbp_df()):
            yield

    def test_exact_column_set(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        missing = set(SEASONAL_COLS) - set(df.columns)
        extra   = set(df.columns) - set(SEASONAL_COLS)
        assert not missing, f"Missing columns: {missing}"
        assert not extra,   f"Extra columns not in schema: {extra}"

    def test_column_count(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        assert len(df.columns) == len(SEASONAL_COLS)

    def test_player_id_never_null(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        assert df["player_id"].notna().all()

    def test_season_never_null(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        assert df["season"].notna().all()

    def test_position_values(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        assert df["position"].isin(["RB", "WR", "TE"]).all()

    def test_column_types(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        errors = _check_column_types(df, SEASONAL_SCHEMA)
        assert not errors, "\n".join(errors)

    def test_upsert_records_json_safe(self):
        """df_to_records must produce JSON-serialisable dicts (no numpy scalars)."""
        import json
        from pipelines.features import build_seasonal_features
        from pipelines.utils import df_to_records
        df = build_seasonal_features([2022, 2023, 2024])
        records = df_to_records(df)
        # Should not raise
        json.dumps(records)

    def test_no_duplicate_primary_key(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        dupes = df.duplicated(subset=["player_id", "season"])
        assert not dupes.any(), f"{dupes.sum()} duplicate (player_id, season) rows"


# ---------------------------------------------------------------------------
# Schema tests — weekly
# ---------------------------------------------------------------------------

class TestWeeklySchema:
    @pytest.fixture(autouse=True)
    def patch_loaders(self):
        with patch("pipelines.loaders.nfl.import_weekly_data", return_value=_make_weekly_df()), \
             patch("pipelines.loaders.nfl.import_snap_counts", return_value=_make_snap_df()), \
             patch("pipelines.loaders.nfl.import_pbp_data", return_value=_make_pbp_df()):
            yield

    def test_exact_column_set(self):
        from pipelines.features import build_weekly_features
        df = build_weekly_features(2024)
        missing = set(WEEKLY_COLS) - set(df.columns)
        extra   = set(df.columns) - set(WEEKLY_COLS)
        assert not missing, f"Missing columns: {missing}"
        assert not extra,   f"Extra columns not in schema: {extra}"

    def test_column_count(self):
        from pipelines.features import build_weekly_features
        df = build_weekly_features(2024)
        assert len(df.columns) == len(WEEKLY_COLS)

    def test_player_id_never_null(self):
        from pipelines.features import build_weekly_features
        df = build_weekly_features(2024)
        assert df["player_id"].notna().all()

    def test_season_never_null(self):
        from pipelines.features import build_weekly_features
        df = build_weekly_features(2024)
        assert df["season"].notna().all()

    def test_week_never_null(self):
        from pipelines.features import build_weekly_features
        df = build_weekly_features(2024)
        assert df["week"].notna().all()

    def test_week_range(self):
        from pipelines.features import build_weekly_features
        df = build_weekly_features(2024)
        assert df["week"].between(1, 22).all()

    def test_column_types(self):
        from pipelines.features import build_weekly_features
        df = build_weekly_features(2024)
        errors = _check_column_types(df, WEEKLY_SCHEMA)
        assert not errors, "\n".join(errors)

    def test_upsert_records_json_safe(self):
        import json
        from pipelines.features import build_weekly_features
        from pipelines.utils import df_to_records
        df = build_weekly_features(2024)
        records = df_to_records(df)
        json.dumps(records)

    def test_no_duplicate_primary_key(self):
        from pipelines.features import build_weekly_features
        df = build_weekly_features(2024)
        dupes = df.duplicated(subset=["player_id", "season", "week"])
        assert not dupes.any(), f"{dupes.sum()} duplicate (player_id, season, week) rows"
