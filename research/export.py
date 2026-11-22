"""Snapshot the prod tables the model bench needs into research/data/ (read-only).

    python -m research.export --warehouse-id <id> [--profile <profile>] [--catalog energy]

Uses the Databricks CLI's SQL statements API, so it runs as you, with no extra packages.
The warehouse and profile can also come from DATABRICKS_WAREHOUSE_ID and
DATABRICKS_CONFIG_PROFILE. Writes daily.parquet, outlook.parquet, hh.parquet and meta.json.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

DATA = Path(__file__).parent / "data"
NUMERIC = {"DOUBLE", "FLOAT", "DECIMAL", "INT", "BIGINT", "LONG", "SHORT", "SMALLINT", "TINYINT"}


class Warehouse:
    def __init__(self, warehouse_id: str, profile: str | None):
        self.warehouse_id, self.profile = warehouse_id, profile

    def _api(self, method: str, path: str, body: dict | None = None) -> dict:
        cmd = ["databricks", "api", method, path]
        if self.profile:
            cmd += ["--profile", self.profile]
        tmp = None
        if body is not None:
            with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
                json.dump(body, f)
            tmp = f.name
            cmd += ["--json", f"@{tmp}"]
        try:
            p = subprocess.run(cmd, capture_output=True, text=True, check=False)
        finally:
            if tmp:
                os.unlink(tmp)
        if p.returncode:
            raise RuntimeError(p.stderr[-800:])
        return json.loads(p.stdout.lstrip("﻿"))

    def query(self, sql: str) -> pd.DataFrame:
        r = self._api("post", "/api/2.0/sql/statements",
                      {"warehouse_id": self.warehouse_id, "statement": sql, "wait_timeout": "50s",
                       "disposition": "INLINE", "format": "JSON_ARRAY"})
        while r["status"]["state"] in ("PENDING", "RUNNING"):
            r = self._api("get", f"/api/2.0/sql/statements/{r['statement_id']}")
        if r["status"]["state"] != "SUCCEEDED":
            raise RuntimeError(r["status"])
        cols = r["manifest"]["schema"]["columns"]
        rows, res = [], r.get("result", {})
        while True:
            rows += res.get("data_array", [])
            nxt = res.get("next_chunk_internal_link")
            if not nxt:
                break
            res = self._api("get", nxt)
        df = pd.DataFrame(rows, columns=[c["name"] for c in cols])
        for c in cols:
            t, name = c["type_name"], c["name"]
            if t in NUMERIC:
                df[name] = pd.to_numeric(df[name])
            elif t == "BOOLEAN":
                df[name] = df[name].map({"true": True, "false": False})
            elif t == "DATE":
                df[name] = pd.to_datetime(df[name])
            elif t == "TIMESTAMP":
                df[name] = pd.to_datetime(df[name], utc=True)
        return df


def export(wh: Warehouse, catalog: str, out: Path = DATA) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    g = f"`{catalog}`.gold"
    tables = {
        "daily": f"SELECT * FROM {g}.fact_market_daily ORDER BY date",
        "outlook": f"SELECT * FROM {g}.fact_gb_week_ahead_outlook ORDER BY issue_date, target_date",
    }
    frames = {k: wh.query(q) for k, q in tables.items()}
    # Half-hourly, a quarter at a time to keep each result small.
    quarters = pd.period_range("2024-01", pd.Timestamp.today(), freq="Q")
    frames["hh"] = pd.concat([wh.query(
        f"""SELECT h.settlement_date, h.settlement_period, h.start_time_utc,
                   h.market_index_price_gbp_mwh, h.system_price_gbp_mwh, h.demand_mw, h.wind_mw,
                   h.residual_demand_mw, h.net_imports_mw, h.gas_share,
                   f.demand_forecast_mw, f.wind_forecast_adjusted_mw, f.solar_forecast_mw
            FROM {g}.fact_gb_half_hourly h
            LEFT JOIN {g}.fact_gb_day_ahead_forecasts f
              USING (settlement_date, settlement_period)
            WHERE h.settlement_date BETWEEN '{q.start_time.date()}' AND '{q.end_time.date()}'""")
        for q in quarters], ignore_index=True)
    for k, df in frames.items():
        df.to_parquet(out / f"{k}.parquet")
    meta = {"exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "catalog": catalog, "rows": {k: len(v) for k, v in frames.items()},
            "last_issue": str(frames["outlook"].issue_date.max().date())}
    (out / "meta.json").write_text(json.dumps(meta, indent=2))
    return meta


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--warehouse-id", default=os.environ.get("DATABRICKS_WAREHOUSE_ID"))
    p.add_argument("--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE"))
    p.add_argument("--catalog", default="energy")
    a = p.parse_args(argv)
    if not a.warehouse_id:
        p.error("--warehouse-id or DATABRICKS_WAREHOUSE_ID is required")
    print(json.dumps(export(Warehouse(a.warehouse_id, a.profile), a.catalog), indent=2))


if __name__ == "__main__":
    main()
