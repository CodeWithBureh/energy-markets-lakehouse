"""Build the AI/BI dashboard (energy_lakehouse.lvdash.json) from SQL files.

    python dashboards/build.py                 # rewrite the .lvdash.json
    python dashboards/build.py --check         # also run every dataset on a warehouse

The datasets are SQL: dashboards/sql/*.sql, plus sql/week_ahead_report.sql and the scoring
part of sql/week_ahead_backtest.sql, so the dashboard shows exactly what those files
compute. Table names are left unqualified; the bundle sets each dataset's catalog and schema
per target (resources/dashboards.yml). The layout is defined below and the JSON is
generated, so a change is reviewed as SQL and Python, not as a JSON diff.
--check uses DATABRICKS_WAREHOUSE_ID and DATABRICKS_CONFIG_PROFILE, and reads the prod
catalog unless told otherwise.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SQL = ROOT / "dashboards" / "sql"
OUT = ROOT / "dashboards" / "energy_lakehouse.lvdash.json"
BACKTEST_MARKER = "-- Summary by days ahead."

# --- Datasets ------------------------------------------------------------------------------


def _unqualify(sql: str) -> str:
    return sql.replace("energy.gold.", "").rstrip().rstrip(";")


def _backtest_cte() -> str:
    text = (ROOT / "sql" / "week_ahead_backtest.sql").read_text(encoding="utf-8")
    head, sep, _ = text.partition(BACKTEST_MARKER)
    if not sep:
        raise ValueError(f"sql/week_ahead_backtest.sql has no {BACKTEST_MARKER!r} line")
    return head


def dataset_sql(name: str) -> str:
    if name == "outlook":
        return _unqualify((ROOT / "sql" / "week_ahead_report.sql").read_text(encoding="utf-8"))
    text = (SQL / f"{name}.sql").read_text(encoding="utf-8")
    if text.startswith("-- needs: backtest"):
        text = _backtest_cte() + text
    return _unqualify(text)


DATASETS = {
    "outlook": "Week-ahead outlook (sql/week_ahead_report.sql)",
    "backtest_kpis": "Backtest headline numbers",
    "backtest_by_horizon": "Backtest error by days ahead",
    "backtest_monthly": "Backtest day-ahead error by month",
    "backtest_recent": "Day-ahead estimate against actual, last 120 days",
    "market_kpis": "Market headline numbers",
    "market_daily": "Daily price and drivers, last two years",
    "neighbours": "GB and neighbouring markets, last 180 days",
    "platform_kpis": "Platform headline numbers",
    "freshness": "Latest data per source",
    "incomplete_days": "Incomplete settlement days, last 30",
    "job_runs": "Prod job runs, last 30 days",
    "cost_daily": "Project cost per day, last 30 days",
}

# --- Widgets -------------------------------------------------------------------------------


def _query(dataset: str, fields: list[tuple[str, str]], aggregated: bool = False) -> list[dict]:
    return [{
        "name": "main_query",
        "query": {
            "datasetName": dataset,
            "fields": [{"name": n, "expression": e} for n, e in fields],
            "disaggregated": not aggregated,
        },
    }]


def _frame(title: str, description: str | None = None) -> dict:
    frame = {"showTitle": True, "title": title}
    if description:
        frame.update(showDescription=True, description=description)
    return frame


def text(name: str, *lines: str) -> dict:
    return {"name": name, "multilineTextboxSpec": {"lines": list(lines)}}


def counter(name: str, dataset: str, column: str, title: str, description: str | None = None,
            fmt: dict | None = None) -> dict:
    value = {"fieldName": column, "displayName": title}
    if fmt:
        value["format"] = fmt
    return {
        "name": name,
        "queries": _query(dataset, [(column, f"`{column}`")]),
        "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": value},
                 "frame": _frame(title, description)},
    }


def chart(name: str, kind: str, dataset: str, title: str, x: tuple[str, str, str],
          ys: list[tuple[str, str]], y_title: str, color: tuple[str, str] | None = None,
          description: str | None = None, mark: dict | None = None) -> dict:
    """x = (column, scale type, axis title); ys = [(column, legend name)]."""
    columns = [x[0]] + [c for c, _ in ys] + ([color[0]] if color else [])
    if len(ys) == 1:
        y = {"fieldName": ys[0][0], "scale": {"type": "quantitative"}, "displayName": y_title}
    else:
        y = {"scale": {"type": "quantitative"}, "axis": {"title": y_title},
             "fields": [{"fieldName": c, "displayName": d} for c, d in ys]}
    encodings = {"x": {"fieldName": x[0], "scale": {"type": x[1]}, "displayName": x[2]}, "y": y}
    if color:
        encodings["color"] = {"fieldName": color[0], "scale": {"type": "categorical"},
                              "displayName": color[1]}
    spec = {"version": 3, "widgetType": kind, "encodings": encodings,
            "frame": _frame(title, description)}
    if mark:
        spec["mark"] = mark
    return {"name": name, "queries": _query(dataset, [(c, f"`{c}`") for c in columns]),
            "spec": spec}


def table(name: str, dataset: str, title: str, columns: list[tuple[str, str]],
          description: str | None = None) -> dict:
    """columns = [(column, header)]; the SQL does any rounding. Table spec version 2: the
    published dashboard shows version 1 tables as "Visualization has no fields selected"."""
    return {
        "name": name,
        "queries": _query(dataset, [(c, f"`{c}`") for c, _ in columns]),
        "spec": {"version": 2, "widgetType": "table",
                 "encodings": {"columns": [{"fieldName": c, "displayName": h}
                                           for c, h in columns]},
                 "frame": _frame(title, description)},
    }


def place(widget: dict, x: int, y: int, w: int, h: int) -> dict:
    return {"widget": widget, "position": {"x": x, "y": y, "width": w, "height": h}}


GBP = {"type": "number-plain", "decimalPlaces": {"type": "exact", "places": 1}}
GBP0 = {"type": "number-plain", "decimalPlaces": {"type": "exact", "places": 0}}
PCT = {"type": "number-percent", "decimalPlaces": {"type": "exact", "places": 0}}
GROUPED = {"layout": "group"}


def outlook_page() -> list[dict]:
    return [
        place(text("outlook_intro",
                   "## Week-ahead GB power price outlook",
                   "Each morning: an estimated daily average price (GBP/MWh) for each of the "
                   "next 7 days, with a range that held about 72% of outcomes in the backtest. "
                   "Below it, every past forecast since March 2024, rebuilt from what was known "
                   "at the time and scored against what happened."), 0, 0, 6, 2),
        place(counter("kpi_day1", "backtest_kpis", "day1_error", "Day-ahead error, GBP/MWh",
                      "Mean absolute error over every past forecast", GBP), 0, 2, 2, 3),
        place(counter("kpi_improve", "backtest_kpis", "day1_improvement",
                      "Better than the naive forecast",
                      "Against 'same as the last 7 days', day ahead", PCT), 2, 2, 2, 3),
        place(counter("kpi_issues", "backtest_kpis", "issues_scored", "Past forecasts scored",
                      "Each one a 7-day outlook"), 4, 2, 2, 3),
        place(chart("outlook_chart", "line", "outlook", "This week's outlook",
                    ("day", "temporal", "Day"),
                    [("price_estimate_gbp_mwh", "Estimate"), ("price_low_gbp_mwh", "Low"),
                     ("price_high_gbp_mwh", "High"),
                     ("last_7_days_avg_price_gbp_mwh", "Last 7 days' average")],
                    "GBP/MWh"), 0, 5, 3, 7),
        place(table("outlook_table", "outlook", "Estimates and what drives them", [
            ("day", "Day"), ("weekday", "Weekday"),
            ("price_estimate_gbp_mwh", "Estimate"),
            ("price_low_gbp_mwh", "Low"), ("price_high_gbp_mwh", "High"),
            ("residual_demand_gw", "Residual demand, GW"),
            ("wind_gw", "Wind, GW"), ("gas_sap_gbp_mwh", "Gas SAP"),
            ("wind_source", "Wind forecast"),
        ]), 3, 5, 3, 7),
        place(chart("horizon_chart", "bar", "backtest_by_horizon", "Error by days ahead",
                    ("days_ahead", "categorical", "Days ahead"),
                    [("model_error", "This model"), ("last_7_days_error", "Same as last 7 days")],
                    "Mean absolute error, GBP/MWh", mark=GROUPED,
                    description="Every past forecast since March 2024"), 0, 12, 3, 7),
        place(chart("monthly_chart", "line", "backtest_monthly", "Day-ahead error by month",
                    ("month", "temporal", "Month"),
                    [("model_error", "This model"), ("last_7_days_error", "Same as last 7 days")],
                    "Mean absolute error, GBP/MWh",
                    description="Errors grow with the price level: see the Market page"),
              3, 12, 3, 7),
        place(chart("recent_chart", "line", "backtest_recent",
                    "Day-ahead estimate against the actual price, last 120 days",
                    ("day", "temporal", "Day"),
                    [("actual_price", "Actual"), ("day_ahead_estimate", "Day-ahead estimate")],
                    "GBP/MWh"), 0, 19, 6, 7),
    ]


def market_page() -> list[dict]:
    return [
        place(text("market_intro",
                   "## The GB power market",
                   "Daily market index price (GBP/MWh). Price follows residual demand (demand "
                   "left after wind) and gas, the fuel that usually sets the price."), 0, 0, 6, 2),
        place(counter("kpi_latest", "market_kpis", "latest_price", "Latest complete day, GBP/MWh",
                      None, GBP0), 0, 2, 2, 3),
        place(counter("kpi_30d", "market_kpis", "avg_price_30d", "Last 30 days, GBP/MWh",
                      None, GBP0), 2, 2, 2, 3),
        place(counter("kpi_30d_ly", "market_kpis", "avg_price_30d_last_year",
                      "Same 30 days last year, GBP/MWh", None, GBP0), 4, 2, 2, 3),
        place(chart("price_chart", "line", "market_daily", "Daily price, last two years",
                    ("date", "temporal", "Date"), [("price", "Price")], "GBP/MWh"), 0, 5, 6, 6),
        place(chart("scatter_chart", "scatter", "market_daily",
                    "Price against residual demand, by year",
                    ("residual_demand_gw", "quantitative", "Residual demand, GW"),
                    [("price", "Price")], "GBP/MWh", color=("year", "Year"),
                    description="The relationship the forecast relies on, and how it moves"),
              0, 11, 3, 8),
        place(chart("gas_chart", "line", "market_daily", "Gas prices",
                    ("date", "temporal", "Date"),
                    [("gas_sap", "GB SAP"), ("gas_ttf", "Dutch TTF")], "GBP/MWh"), 3, 11, 3, 8),
        place(chart("wind_chart", "line", "market_daily", "Wind's share of generation",
                    ("date", "temporal", "Date"), [("wind_share_pct", "Wind share")], "%"),
              0, 19, 3, 7),
        place(chart("neighbours_chart", "line", "neighbours", "GB and its neighbours, last 180 days",
                    ("date", "temporal", "Date"), [("price", "Price")], "GBP/MWh",
                    color=("market", "Market"),
                    description="Day-ahead prices; GB is the market index price"), 3, 19, 3, 7),
    ]


def platform_page() -> list[dict]:
    return [
        place(text("platform_intro",
                   "## Platform health",
                   "Is the data fresh and complete, did the prod jobs run, and what does it "
                   "cost? Job runs and cost come from Databricks system tables."), 0, 0, 6, 2),
        place(counter("kpi_runs", "platform_kpis", "daily_runs_ok",
                      "Successful daily runs, last 30 days"), 0, 2, 2, 3),
        place(counter("kpi_incomplete", "platform_kpis", "incomplete_days_30d",
                      "Incomplete settlement days, last 30",
                      "Days with fewer half-hours than they should have"), 2, 2, 2, 3),
        place({
            "name": "kpi_cost",
            "queries": _query("cost_daily", [("sum(usd)", "SUM(`usd`)")], aggregated=True),
            "spec": {"version": 2, "widgetType": "counter",
                     "encodings": {"value": {"fieldName": "sum(usd)", "displayName": "USD",
                                             "format": GBP0}},
                     "frame": _frame("Cost, last 30 days (USD)",
                                     "Databricks list price; excludes Azure storage")},
        }, 4, 2, 2, 3),
        place(table("freshness_table", "freshness", "Latest data per source", [
            ("source", "Source"), ("latest_day", "Latest day"),
            ("days_behind", "Days behind"),
            ("allowed_days_behind", "Allowed"), ("status", "Status"),
        ], "ERA5 is published about 6 days behind and loaded weekly"), 0, 5, 3, 7),
        place(table("incomplete_table", "incomplete_days", "Incomplete settlement days", [
            ("date", "Day"), ("expected_settlement_periods", "Expected"),
            ("periods_loaded", "Loaded"),
        ], "Empty when every day in the last 30 is complete"), 3, 5, 3, 7),
        place(chart("cost_env_chart", "bar", "cost_daily", "Cost per day by environment",
                    ("usage_date", "temporal", "Day"), [("usd", "USD")], "USD, list price",
                    color=("environment", "Environment"),
                    description="CI runs on every merge to dev; the warehouse is shared"),
              0, 12, 3, 7),
        place(chart("cost_product_chart", "bar", "cost_daily", "Cost per day by product",
                    ("usage_date", "temporal", "Day"), [("usd", "USD")], "USD, list price",
                    color=("product", "Product"),
                    description="Jobs: ingest and checks code. Pipeline: bronze, silver and "
                                "gold processing"), 3, 12, 3, 7),
        place(chart("runs_chart", "bar", "job_runs", "Prod job runs by outcome",
                    ("run_date", "temporal", "Day"), [("runs", "Runs")], "Runs",
                    color=("result_state", "Outcome")), 0, 19, 6, 6),
    ]


def build() -> dict:
    pages = [("outlook", "Week-ahead outlook", outlook_page()),
             ("market", "Market", market_page()),
             ("platform", "Platform health", platform_page())]
    return {
        "datasets": [{"name": n, "displayName": d,
                      "queryLines": dataset_sql(n).splitlines(keepends=True)}
                     for n, d in DATASETS.items()],
        "pages": [{"name": n, "displayName": d, "pageType": "PAGE_TYPE_CANVAS", "layout": layout}
                  for n, d, layout in pages],
    }


def render() -> str:
    return json.dumps(build(), indent=2, ensure_ascii=False) + "\n"


def check(catalog: str, schema: str) -> int:
    """Run every dataset and every widget's columns against a warehouse."""
    sys.path.insert(0, str(ROOT))
    from research.export import Warehouse

    wh = Warehouse(os.environ["DATABRICKS_WAREHOUSE_ID"], os.environ.get("DATABRICKS_CONFIG_PROFILE"))
    dash, failed = build(), 0
    used: dict[str, set[str]] = {}
    for page in dash["pages"]:
        for item in page["layout"]:
            for q in item["widget"].get("queries", []):
                cols = {re.sub(r"^.*`(.+)`.*$", r"\1", f["expression"]) for f in q["query"]["fields"]}
                used.setdefault(q["query"]["datasetName"], set()).update(cols)
    for ds in dash["datasets"]:
        name = ds["name"]
        try:
            df = wh.query("".join(ds["queryLines"]), catalog=catalog, schema=schema)
        except RuntimeError as exc:
            print(f"FAIL {name}: {str(exc)[:300]}")
            failed += 1
            continue
        missing = used.get(name, set()) - set(df.columns)
        status = f"FAIL missing {sorted(missing)}" if missing else "ok"
        failed += bool(missing)
        print(f"{status:4} {name}: {len(df)} rows")
    return failed


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--check", action="store_true", help="run every dataset on a warehouse")
    p.add_argument("--catalog", default="energy")
    p.add_argument("--schema", default="gold")
    args = p.parse_args()
    OUT.write_text(render(), encoding="utf-8", newline="\n")
    print(f"wrote {OUT.relative_to(ROOT)}")
    return 1 if args.check and check(args.catalog, args.schema) else 0


if __name__ == "__main__":
    raise SystemExit(main())
