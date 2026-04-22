"""
Shared utility functions used across pipeline modules.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd


def three_point_slope(
    y_old: pd.Series, y_mid: pd.Series, y_new: pd.Series
) -> pd.Series:
    """
    Vectorized slope of a 3-point equally-spaced time series [y_old, y_mid, y_new]
    at x = [0, 1, 2]. Requires ≥2 valid (non-NaN) points; returns NaN otherwise.

    Closed-form per missing-data case:
      all 3 present  → (y_new - y_old) / 2
      only old + new → (y_new - y_old) / 2
      only mid + new → y_new - y_mid
      only old + mid → y_mid - y_old
    """
    slope = pd.Series(np.nan, index=y_old.index)

    has_old = y_old.notna()
    has_mid = y_mid.notna()
    has_new = y_new.notna()

    all3    = has_old & has_mid & has_new
    old_new = has_old & has_new & ~has_mid
    mid_new = has_mid & has_new & ~has_old
    old_mid = has_old & has_mid & ~has_new

    slope[all3]    = (y_new[all3]    - y_old[all3])    / 2
    slope[old_new] = (y_new[old_new] - y_old[old_new]) / 2
    slope[mid_new] =  y_new[mid_new] - y_mid[mid_new]
    slope[old_mid] =  y_mid[old_mid] - y_old[old_mid]

    return slope


def _clean_value(v: object, int_col: bool = False) -> object:
    """Recursively convert a value to a JSON-safe Python native type."""
    if v is pd.NA or v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, bool):
        return bool(v)
    if hasattr(v, "item"):
        # numpy scalar → Python native (int or float)
        native = v.item()
        return None if (isinstance(native, float) and math.isnan(native)) else native
    if int_col and isinstance(v, float):
        return int(v)
    if isinstance(v, dict):
        return {dk: _clean_value(dv) for dk, dv in v.items()}
    if isinstance(v, list):
        return [_clean_value(item) for item in v]
    return v


def df_to_records(
    df: pd.DataFrame,
    int_cols: list[str] | None = None,
) -> list[dict]:
    """
    Convert a DataFrame to a JSON-safe list of dicts for Supabase upserts.
    Handles NaN, pd.NA, numpy scalars, booleans, and nested dicts/lists
    (e.g. shap_values and comps JSONB columns).

    int_cols: column names that must be sent as Python ints (for DB smallint/int
    columns). float values that are whole numbers are cast; NaN becomes None.
    """
    int_set = set(int_cols or [])
    records = []
    for row in df.to_dict(orient="records"):
        records.append({k: _clean_value(v, int_col=k in int_set) for k, v in row.items()})
    return records
