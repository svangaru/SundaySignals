"""
Unit tests for feature engineering pipelines.

These tests mock nfl_data_py loaders so no network calls are made.
Run with: pytest tests/test_features.py -v
"""

from __future__ import annotations

import pandas as pd
import numpy as np
import pytest
from unittest.mock import patch, MagicMock

from pipelines.constants import SEASONAL_COLS, WEEKLY_COLS, POSITIONS
from pipelines.utils import three_point_slope, df_to_records


# ---------------------------------------------------------------------------
# Fixtures — minimal synthetic DataFrames that match nfl_data_py column shapes
# ---------------------------------------------------------------------------

def _make_seasonal_df(seasons=(2022, 2023, 2024)):
    rows = []
    for s in seasons:
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
                "receiving_yards": 1400, "receiving_tds": 9,
                "carries": 0, "rushing_yards": 0,
            })
    return pd.DataFrame(rows)


def _make_snap_df(seasons=(2022, 2023, 2024)):
    rows = []
    for s in seasons:
        for pid in ["ABC123", "DEF456", "GHI789"]:
            for wk in range(1, 18):
                rows.append({
                    "player_id": pid, "season": s, "week": wk,
                    "offense_pct": 0.85, "position": "WR",
                })
    return pd.DataFrame(rows)


def _make_roster_df(seasons=(2022, 2023, 2024)):
    rows = []
    for s in seasons:
        for pid, draft in [("ABC123", 22), ("DEF456", 14), ("GHI789", 5)]:
            rows.append({
                "gsis_id": pid, "season": s,
                "birth_date": "1998-06-16", "entry_year": 2020,
                "draft_number": draft,
            })
    return pd.DataFrame(rows)


def _make_pbp_df(seasons=(2022, 2023, 2024)):
    rows = []
    for s in seasons:
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


def _make_weekly_df(season=2024):
    rows = []
    for pid, name, pos in [("ABC123", "Justin Jefferson", "WR"),
                            ("DEF456", "Davante Adams", "WR"),
                            ("GHI789", "Dalvin Cook", "RB")]:
        for wk in range(1, 10):
            rows.append({
                "player_id": pid, "season": season, "week": wk,
                "player_name": name, "position": pos,
                "recent_team": "MIN",
                "targets": 8, "target_share": 0.28,
                "air_yards": 85.0, "air_yards_share": 0.32,
                "fantasy_points_ppr": 18.5, "receiving_epa": 2.1,
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Tests — utils
# ---------------------------------------------------------------------------

class TestThreePointSlope:
    def test_increasing_trend(self):
        s = three_point_slope(0.10, 0.15, 0.20)
        assert s > 0

    def test_decreasing_trend(self):
        s = three_point_slope(0.20, 0.15, 0.10)
        assert s < 0

    def test_flat_trend(self):
        s = three_point_slope(0.15, 0.15, 0.15)
        assert s == pytest.approx(0.0, abs=1e-6)

    def test_series_input(self):
        y0 = pd.Series([0.10, 0.20])
        y1 = pd.Series([0.15, 0.25])
        y2 = pd.Series([0.20, 0.30])
        result = three_point_slope(y0, y1, y2)
        assert len(result) == 2
        assert all(result > 0)

    def test_nan_propagation(self):
        result = three_point_slope(np.nan, 0.15, 0.20)
        assert np.isnan(result)


class TestDfToRecords:
    def test_basic_conversion(self):
        df = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        records = df_to_records(df)
        assert isinstance(records, list)
        assert len(records) == 2
        assert records[0]["a"] == 1

    def test_nan_becomes_none(self):
        df = pd.DataFrame({"a": [1, np.nan]})
        records = df_to_records(df)
        assert records[1]["a"] is None

    def test_numpy_scalars_serializable(self):
        df = pd.DataFrame({"a": np.array([1, 2], dtype=np.int64)})
        records = df_to_records(df)
        assert isinstance(records[0]["a"], int)

    def test_boolean_preserved(self):
        df = pd.DataFrame({"flag": [True, False]})
        records = df_to_records(df)
        assert records[0]["flag"] is True
        assert records[1]["flag"] is False


# ---------------------------------------------------------------------------
# Tests — seasonal feature builder
# ---------------------------------------------------------------------------

class TestBuildSeasonalFeatures:
    """Patches all nfl_data_py loaders — no network calls."""

    @pytest.fixture(autouse=True)
    def patch_loaders(self):
        seasonal_df = _make_seasonal_df(seasons=[2020, 2021, 2022, 2023, 2024])
        snap_df = _make_snap_df(seasons=[2020, 2021, 2022, 2023, 2024])
        roster_df = _make_roster_df(seasons=[2020, 2021, 2022, 2023, 2024])
        pbp_df = _make_pbp_df(seasons=[2020, 2021, 2022, 2023, 2024])

        with patch("pipelines.loaders.nfl.import_seasonal_data", return_value=seasonal_df), \
             patch("pipelines.loaders.nfl.import_snap_counts", return_value=snap_df), \
             patch("pipelines.loaders.nfl.import_weekly_rosters", return_value=roster_df), \
             patch("pipelines.loaders.nfl.import_pbp_data", return_value=pbp_df):
            yield

    def test_returns_dataframe(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        assert isinstance(df, pd.DataFrame)

    def test_has_required_columns(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        for col in SEASONAL_COLS:
            assert col in df.columns, f"Missing column: {col}"

    def test_no_extra_columns(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        assert set(df.columns) == set(SEASONAL_COLS)

    def test_only_skill_positions(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        assert df["position"].isin(POSITIONS).all()

    def test_primary_key_unique(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        assert not df.duplicated(subset=["player_id", "season"]).any()

    def test_season_column_matches_input(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        assert set(df["season"].unique()).issubset({2022, 2023, 2024})

    def test_broke_out_is_boolean_or_null(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        non_null = df["broke_out"].dropna()
        assert non_null.dtype == bool or set(non_null.unique()).issubset({True, False, 0, 1})

    def test_target_share_slope_computed(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        # At least some rows should have a non-null slope (need 3 years of data)
        assert df["target_share_slope"].notna().any()

    def test_snap_share_slope_computed(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        assert df["snap_share_slope"].notna().any()

    def test_fantasy_ppg_positive(self):
        from pipelines.features import build_seasonal_features
        df = build_seasonal_features([2022, 2023, 2024])
        positive = df["fantasy_ppg"].dropna()
        assert (positive >= 0).all()


# ---------------------------------------------------------------------------
# Tests — weekly feature builder
# ---------------------------------------------------------------------------

class TestBuildWeeklyFeatures:
    """Patches all nfl_data_py loaders — no network calls."""

    @pytest.fixture(autouse=True)
    def patch_loaders(self):
        weekly_df = _make_weekly_df(season=2024)
        snap_df = _make_snap_df(seasons=[2024])
        pbp_df = _make_pbp_df(seasons=[2024])

        with patch("pipelines.loaders.nfl.import_weekly_data", return_value=weekly_df), \
             patch("pipelines.loaders.nfl.import_snap_counts", return_value=snap_df), \
             patch("pipelines.loaders.nfl.import_pbp_data", return_value=pbp_df):
            yield

    def test_returns_dataframe(self):
        from pipelines.features import build_weekly_features
        df = build_weekly_features(2024)
        assert isinstance(df, pd.DataFrame)

    def test_has_required_columns(self):
        from pipelines.features import build_weekly_features
        df = build_weekly_features(2024)
        for col in WEEKLY_COLS:
            assert col in df.columns, f"Missing column: {col}"

    def test_no_extra_columns(self):
        from pipelines.features import build_weekly_features
        df = build_weekly_features(2024)
        assert set(df.columns) == set(WEEKLY_COLS)

    def test_primary_key_unique(self):
        from pipelines.features import build_weekly_features
        df = build_weekly_features(2024)
        assert not df.duplicated(subset=["player_id", "season", "week"]).any()

    def test_week_filter_respected(self):
        from pipelines.features import build_weekly_features
        df = build_weekly_features(2024, weeks=[1, 2, 3])
        assert set(df["week"].unique()).issubset({1, 2, 3})

    def test_rolling_r3_not_all_null(self):
        from pipelines.features import build_weekly_features
        df = build_weekly_features(2024)
        # Rolling features require week >= 3; some should be non-null
        assert df["snap_pct_r3"].notna().any()
        assert df["target_share_r3"].notna().any()

    def test_delta_r3_not_all_null(self):
        from pipelines.features import build_weekly_features
        df = build_weekly_features(2024)
        assert df["snap_pct_delta_r3"].notna().any()
        assert df["target_share_delta_r3"].notna().any()

    def test_ppr_points_non_negative(self):
        from pipelines.features import build_weekly_features
        df = build_weekly_features(2024)
        non_null = df["ppr_points"].dropna()
        assert (non_null >= 0).all()
