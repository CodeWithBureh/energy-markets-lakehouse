"""Reference: the report before PR #23, the 60-day residual demand and gas model alone, with no
blend with the last 7 days.
"""
from research.candidates.report_blend import estimate

DESCRIPTION = __doc__


def predict(issue):
    return estimate(issue, weight=0)
