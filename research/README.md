# Week-ahead model bench

This bench tests changes to the week-ahead price report (`sql/week_ahead_report.sql`)
against the full backtest before any of them reach the SQL. It's local research code. It
isn't part of the ingest wheel or the Databricks bundle.

## Use

```
python -m research.export --warehouse-id <id> --profile <profile>   # snapshot prod, read-only
python -m research.bench run report_blend                           # dev scores vs the champion
python -m research.bench check <candidate>                          # holdout and promotion gates
python -m research.bench promote <candidate>                        # after a passing check
python -m research.bench log
```

The snapshot goes to `research/data/`, which is gitignored. A candidate is a module in
`research/candidates/` with a docstring and `predict(issue)`. The function returns one price
per row of `issue.rows` (days 1-7).

`issue` holds only what was known at the issue:
- `issue.daily`: complete days before the issue, with columns `price`, `resid_gw`,
  `sap_prev` and `wind_gw`, plus every column of `fact_market_daily`.
- `issue.rows`: that issue's outlook rows.
- `issue.hh`: the half-hourly data before the issue.

Helpers are in `research/lib.py`.

## How a model is judged

- **dev** covers issues up to 30 Jun 2026. Compare on these as often as you like.
- **holdout** covers issues from 1 Jul 2026 onwards, and grows as new weeks arrive. Only
  `check` reads it, and every check is logged in `experiments.jsonl`.

To be promoted, a candidate must pass all of these against the champion:
- it predicts at least 99% of the rows the champion does;
- its holdout error is lower;
- it is no worse in either half of dev;
- it has a lower error in at least 55% of months.

`promote` refuses unless the same code passed a check on the current snapshot.

The starting champion, `report_blend`, reproduces `sql/week_ahead_backtest.sql` exactly: 13.33
to 17.17 GBP/MWh for days 1-7, 15.18 overall.

`/improve-model` (`.claude/skills/improve-model/`) runs the loop. Ideas are in `ideas.md`.
