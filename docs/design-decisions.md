# Design decisions

The choices that shape this lakehouse, why each was made, and what it costs. Each entry
points to the code that implements it.

| # | Decision | Area |
|---|---|---|
| 1 | Land immutable files before any table | Ingestion |
| 2 | Keep every revision in silver (SCD type 2) | Modelling |
| 3 | Key forecasts by publish time, pick vintages in gold | Modelling |
| 4 | Derive settlement times from first principles | Modelling |
| 5 | Skip data that is settled or unchanged | Ingestion, cost |
| 6 | Serverless, triggered, deterministic gold | Cost |
| 7 | One workspace, three environments | Platform |
| 8 | Promote only after a full run passes data checks | CI/CD |
| 9 | Week-ahead outlook in SQL, changed only through a backtest | Data product |
| 10 | Rebuild past outlooks from archived forecasts | Data product |
| 11 | Test pipeline code on local Spark with a stand-in `dlt` | Testing |

## 1. Land immutable files before any table

Every API response is written as-is to the `raw.landing` Volume, under a unique, timestamped
name (`landing.py`), and bronze reads those files with Auto Loader (`pipeline/bronze.py`).

- **Why:** any table can be rebuilt from the files, a bad parse can be fixed and replayed,
  and the files are an audit trail of exactly what each source said and when.
- **Alternative:** write API rows straight into Delta tables. Simpler, but a parsing bug
  then loses data for good.
- **Cost:** storage for the files (a few GB), and two hops before data is queryable.

## 2. Keep every revision in silver (SCD type 2)

Silver tables are merged with Lakeflow AUTO CDC on their business keys, stored as SCD type 2
(`pipeline/silver.py`, `upsert`). Only data columns are compared: lineage columns (file name,
landing time) and Elexon publish times order the versions but don't count as a change.

- **Why:** Elexon re-settles past periods and sources re-publish unchanged data. This keeps
  the full history for "what did we know on date X" questions, without a new version every
  time an identical file lands.
- **Cost:** every reader must filter `__END_AT IS NULL` (gold's `s()` does), and storage
  grows with revisions.

## 3. Key forecasts by publish time, pick vintages in gold

Forecast tables (demand, wind, solar, NDFD, weather) keep every vintage: the publish time is
part of the key, so a newer forecast is a new row, not a revision. Gold chooses the vintage
it needs: the latest published before the delivery day for `fact_gb_day_ahead_forecasts`,
and the latest before each issue for the week-ahead outlook (`_latest_vintage`).

- **Why:** features must only use what was known at the time. Mixing in later forecasts
  (look-ahead) makes a model look better in backtests than it can be live.
- **Cost:** larger tables, and every consumer must choose a cutoff deliberately.

## 4. Derive settlement times from first principles

A settlement period's UTC start is local (Europe/London) midnight converted to UTC, plus
(period - 1) x 30 minutes (`settlement_start_utc`). An expectation checks it against
Elexon's own start time.

- **Why:** clock-change days have 46 or 50 periods. This needs no special cases, and
  `dim_date` uses the same rule to know how many periods each day should have, which drives
  the completeness checks.

## 5. Skip data that is settled or unchanged

Each source has a settle period (`SETTLE_DAYS` in `landing.py`). Past it, a day already
landed isn't fetched again, and a payload identical to the last landing isn't written.

- **Why:** fewer API calls, smaller bronze, and pipeline updates that do nothing when nothing
  changed. A full backfill re-run only fetches what is missing.
- **Risk:** a revision after the settle period would be missed. `--refetch` forces a fetch.

## 6. Serverless, triggered, deterministic gold

Jobs and the pipeline run on serverless compute and only when triggered. Gold views avoid
`current_date()` and in-memory DataFrames (`dim_date` is a fixed range, `dim_company` is SQL
`VALUES`).

- **Why:** cost. A non-deterministic view is rewritten on every refresh, and that rewrite
  looks like changed data to everything downstream. Deterministic views refresh as no-ops.
- **Measured:** the pipeline and jobs cost about $39 at list price for 1-26 Nov 2026,
  backfills included. See the cost section of the README.

## 7. One workspace, three environments

`dev`, `ci` and `prod` are bundle targets in one Azure workspace. They are separated by
catalog (`energy_dev`, `energy_dev.ci_*`, `energy`) and by identity: prod and ci run as
their own service principals with grants on their catalog only.

- **Why:** one workspace keeps the fixed cost near zero.
- **Cost:** weaker isolation than separate workspaces. Workspace and grants are created by
  hand, which Terraform would fix (on the roadmap).

## 8. Promote only after a full run passes data checks

A push to `dev` deploys the `ci` copy, runs the whole daily job against real sources, then
runs `energy_checks` (freshness, uniqueness, completeness, price ranges). Only then does the
`promote` job open the `dev` to `main` pull request, whose merge deploys prod.

- **Why:** unit tests can't catch a source changing its format or a join that drops rows.
  Running the real job first means prod only gets code that has just worked end to end.
- **Cost:** each promotion takes a full daily run (about 10 minutes of serverless compute).
  The promotion PR needs a personal access token, because merges made with `GITHUB_TOKEN`
  don't trigger workflows.

## 9. Week-ahead outlook in SQL, changed only through a backtest

The price outlook is a two-variable regression (residual demand and gas) refitted daily,
blended with the last 7 days' price, in plain SQL (`sql/week_ahead_report.sql`). Changes go
through the research bench first (`research/`), which scores them on a held-out period.

- **Why:** every number in the report can be traced and explained, and a backtest over 980
  past issues says how far to trust it. A more complex model only earns its place by
  beating this one on data it was never tuned on.
- **Result:** 13.3 GBP/MWh average error at 1 day ahead, 17.2 at 7, against 20.2 to 21.4 for
  "same as the last 7 days".

## 10. Rebuild past outlooks from archived forecasts

Issues before the live forecast started are rebuilt from Open-Meteo's Previous Runs API: for
each day, what the 07:40 forecast would have said, using only forecasts made by then
(`sources/weather_forecast.py`). Where the archive's 7-day limit forces a forecast made
later (late on day 7), the row says so (`lead_shortfall_hours`).

- **Why:** a backtest on weather observations instead of forecasts would overstate accuracy.
- **Cost:** about 11,000 API calls for the full history against a free limit of 10,000 a
  day, so the first backfill may need two days.

## 11. Test pipeline code on local Spark with a stand-in `dlt`

`tests/pipeline/` runs the real bronze/silver/gold code on local PySpark. A stand-in `dlt`
module passes the decorators through and records each table's AUTO CDC settings, and table
reads return small DataFrames each test defines.

- **Why:** the transformations hold most of the logic, and testing them in a workspace
  would be slow and cost money on every push.
- **Limit:** AUTO CDC's merge itself and Auto Loader are Databricks' and aren't run. The ci
  deploy (decision 8) covers them.
