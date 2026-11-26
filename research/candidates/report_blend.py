"""The week-ahead report as in sql/week_ahead_report.sql (PR #23): price = a + b1 * residual
demand (GW) + b2 * SAP (previous gas day), fitted on the 60 complete days before the issue
and applied to the forecast residual demand and the latest SAP, then blended 75/25 with the
last 7 days' average price.
"""
import pandas as pd

from research.lib import fit_window, last_days_avg, nan_like, ols

DESCRIPTION = __doc__

FIT_DAYS = 60
LAST_7_DAYS_WEIGHT = 0.25
MIN_FIT_DAYS = 30


def estimate(issue, fit_days: int = FIT_DAYS, weight: float = LAST_7_DAYS_WEIGHT) -> pd.Series:
    h = fit_window(issue.daily, issue.issue_date, fit_days).dropna(
        subset=["price", "resid_gw", "sap_prev"])
    if len(h) < MIN_FIT_DAYS:
        return nan_like(issue.rows)
    a, b1, b2 = ols(h[["resid_gw", "sap_prev"]], h.price)
    model = a + b1 * issue.rows.resid_f_gw + b2 * issue.rows.latest_sap_gbp_mwh
    if not weight:
        return model
    return (1 - weight) * model + weight * last_days_avg(issue.daily, issue.issue_date, 7)


def predict(issue) -> pd.Series:
    return estimate(issue)
