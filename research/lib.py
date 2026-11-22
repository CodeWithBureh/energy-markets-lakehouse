"""Helpers for candidate models. Everything here only sees what an Issue gives it."""
from __future__ import annotations

import numpy as np
import pandas as pd


def ols(X: pd.DataFrame | np.ndarray, y: pd.Series | np.ndarray,
        weights: np.ndarray | None = None) -> np.ndarray:
    """Least squares with an intercept: returns [a, b1, b2, ...]."""
    X = np.asarray(X, dtype=float)
    A = np.c_[np.ones(len(X)), X]
    y = np.asarray(y, dtype=float)
    if weights is not None:
        w = np.sqrt(np.asarray(weights, dtype=float))
        A, y = A * w[:, None], y * w
    b, *_ = np.linalg.lstsq(A, y, rcond=None)
    return b


def fit_window(daily: pd.DataFrame, issue_date: pd.Timestamp, days: int) -> pd.DataFrame:
    """Complete days in [issue_date - days, issue_date), as the report's fit window."""
    return daily[daily.date >= issue_date - pd.Timedelta(days=days)]


def last_days_avg(daily: pd.DataFrame, issue_date: pd.Timestamp, days: int = 7,
                  col: str = "price") -> float:
    """Average of `col` over the `days` days before the issue (the report's baseline)."""
    return float(daily.loc[daily.date >= issue_date - pd.Timedelta(days=days), col].mean())


def nan_like(rows: pd.DataFrame) -> pd.Series:
    return pd.Series(np.nan, index=rows.index)
