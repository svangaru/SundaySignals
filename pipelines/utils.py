"""
Shared utility functions used across pipeline modules.
"""

from __future__ import annotations

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
