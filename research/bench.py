"""Model bench for the week-ahead GB price report.

Every candidate is scored the same way as sql/week_ahead_backtest.sql: for each past issue
in gold.fact_gb_week_ahead_outlook, it sees only the complete days before the issue and that
issue's forecasts, predicts the daily market index price for days 1-7, and is scored on
mean absolute error (GBP/MWh) against the price once known.

The history is split so ideas can't be tuned to the data they're judged on:
  dev      issues up to DEV_END. Develop and compare on these as often as you like.
  holdout  issues after DEV_END, growing as new weeks arrive. Only `check` reads it, and
           every check is logged.

    python -m research.bench run <candidate> [--note "..."]   dev scores vs the champion
    python -m research.bench check <candidate>                holdout and the promotion gates
    python -m research.bench promote <candidate>              after a passing check
    python -m research.bench log                              every experiment so far

Candidates are modules in research/candidates/ with DESCRIPTION and predict(issue).
"""
from __future__ import annotations

import argparse
import hashlib
import importlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).parent
DATA = ROOT / "data"
LOG = ROOT / "experiments.jsonl"
CHAMPION = ROOT / "champion.json"

DEV_END = pd.Timestamp("2026-06-30")
# Promotion gates, all against the champion on the rows both predict.
MIN_COVERAGE = 0.99          # share of the champion's scored rows the candidate predicts
MIN_MONTH_WIN_SHARE = 0.55   # share of months (dev and holdout) with a lower error


@dataclass
class Snapshot:
    daily: pd.DataFrame       # complete days, with price, resid_gw, sap_prev, wind_gw
    outlook: pd.DataFrame     # gold.fact_gb_week_ahead_outlook, with resid_f_gw
    hh: pd.DataFrame | None   # half-hourly actuals and day-ahead forecasts
    meta: dict

    @classmethod
    def load(cls, path: Path = DATA) -> Snapshot:
        if not (path / "meta.json").exists():
            raise SystemExit(f"No snapshot in {path}: run python -m research.export first")
        daily = pd.read_parquet(path / "daily.parquet")
        outlook = pd.read_parquet(path / "outlook.parquet")
        hh = pd.read_parquet(path / "hh.parquet") if (path / "hh.parquet").exists() else None
        return cls(*prepare(daily, outlook, hh), json.loads((path / "meta.json").read_text()))


def prepare(daily: pd.DataFrame, outlook: pd.DataFrame, hh: pd.DataFrame | None = None):
    d = daily[daily.is_complete_day.astype(bool)].sort_values("date").copy()
    d["date"] = pd.to_datetime(d.date)
    d["price"] = d.gb_market_index_price_gbp_mwh
    d["resid_gw"] = d.gb_avg_residual_demand_mw / 1000
    d["wind_gw"] = (d.gb_avg_demand_mw - d.gb_avg_residual_demand_mw) / 1000
    d["sap_prev"] = d.gb_gas_sap_gbp_mwh.shift(1)          # previous gas day, as the report
    o = outlook.copy()
    o["issue_date"] = pd.to_datetime(o.issue_date)
    o["target_date"] = pd.to_datetime(o.target_date)
    o["resid_f_gw"] = o.residual_demand_forecast_mw / 1000
    if hh is not None:
        hh = hh.copy()
        hh["settlement_date"] = pd.to_datetime(hh.settlement_date)
    return d.reset_index(drop=True), o, hh


@dataclass
class Issue:
    """What a candidate may use for one forecast issue: nothing on or after issue_date."""
    issue_date: pd.Timestamp
    rows: pd.DataFrame          # this issue's outlook rows (days_ahead 1-7, forecasts)
    daily: pd.DataFrame         # complete days before issue_date
    _hh: Callable[[], pd.DataFrame | None] = field(repr=False, default=lambda: None)

    @property
    def hh(self) -> pd.DataFrame | None:
        """Half-hourly rows before issue_date (built on first use)."""
        return self._hh()


def issues(snap: Snapshot):
    for day, rows in snap.outlook.groupby("issue_date", sort=True):
        def hh(day=day):
            return None if snap.hh is None else snap.hh[snap.hh.settlement_date < day]
        yield Issue(day, rows, snap.daily[snap.daily.date < day], hh)


def load_candidate(name: str):
    return importlib.import_module(f"research.candidates.{name}")


def code_hash(name: str) -> str:
    h = hashlib.sha256()
    for p in (ROOT / "candidates" / f"{name}.py", ROOT / "lib.py"):
        h.update(p.read_bytes())
    return h.hexdigest()[:12]


def score(name: str, snap: Snapshot, cache: bool = True) -> pd.DataFrame:
    """One row per (issue, target day) with an actual price: prediction and actual."""
    key = DATA / "cache" / f"{name}_{code_hash(name)}_{snap.meta['exported_at'][:19]}.parquet"
    key = key.with_name(key.name.replace(":", ""))
    if cache and key.exists():
        return pd.read_parquet(key)
    mod = load_candidate(name)
    out = []
    for issue in issues(snap):
        pred = pd.Series(np.asarray(mod.predict(issue), dtype=float), index=issue.rows.index)
        out.append(issue.rows[["issue_date", "target_date", "days_ahead"]].assign(pred=pred))
    s = pd.concat(out, ignore_index=True)
    actual = snap.daily.set_index("date").price.rename("actual")
    s = s.join(actual, on="target_date").dropna(subset=["actual"])
    if cache:
        key.parent.mkdir(parents=True, exist_ok=True)
        s.to_parquet(key)
    return s


def compare(cand: pd.DataFrame, champ: pd.DataFrame) -> pd.DataFrame:
    k = ["issue_date", "target_date", "days_ahead", "actual"]
    m = champ[k + ["pred"]].rename(columns={"pred": "champ"}).merge(
        cand[["issue_date", "target_date", "pred"]].rename(columns={"pred": "cand"}),
        on=["issue_date", "target_date"], how="left")
    m["e_champ"] = (m.champ - m.actual).abs()
    m["e_cand"] = (m.cand - m.actual).abs()
    m["month"] = m.issue_date.dt.to_period("M").astype(str)
    return m


def summary(m: pd.DataFrame) -> dict:
    both = m.dropna(subset=["e_champ", "e_cand"])
    by_day = both.groupby("days_ahead")[["e_cand", "e_champ"]].mean()
    months = both.groupby("month")[["e_cand", "e_champ"]].mean()
    return {
        "rows": len(both),
        "coverage": round(float(m.e_cand.notna().sum() / max(m.e_champ.notna().sum(), 1)), 4),
        "mae": round(float(both.e_cand.mean()), 3),
        "champion_mae": round(float(both.e_champ.mean()), 3),
        "mae_by_day": [round(float(x), 2) for x in by_day.e_cand],
        "champion_mae_by_day": [round(float(x), 2) for x in by_day.e_champ],
        "month_win_share": round(float((months.e_cand < months.e_champ).mean()), 3),
        "bias": round(float((both.cand - both.actual).mean()), 2),
    }


def dev_halves(m: pd.DataFrame) -> list[dict]:
    dev = m[m.issue_date <= DEV_END]
    mid = dev.issue_date.drop_duplicates().median()
    return [summary(dev[dev.issue_date <= mid]), summary(dev[dev.issue_date > mid])]


def champion_name() -> str:
    return json.loads(CHAMPION.read_text())["candidate"]


def log(record: dict) -> None:
    with LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def base_record(stage: str, name: str, snap: Snapshot, note: str | None) -> dict:
    return {"time": datetime.now(timezone.utc).isoformat(timespec="seconds"), "stage": stage,
            "candidate": name, "code": code_hash(name), "champion": champion_name(),
            "champion_code": code_hash(champion_name()), "snapshot": snap.meta["exported_at"],
            "description": load_candidate(name).DESCRIPTION.strip(), "note": note}


def run(name: str, note: str | None = None, snap: Snapshot | None = None) -> dict:
    snap = snap or Snapshot.load()
    m = compare(score(name, snap), score(champion_name(), snap))
    dev = m[m.issue_date <= DEV_END]
    rec = base_record("dev", name, snap, note) | {"dev": summary(dev), "dev_halves": dev_halves(m)}
    log(rec)
    return rec


def gates(m: pd.DataFrame) -> dict:
    hold = summary(m[m.issue_date > DEV_END])
    halves = dev_halves(m)
    allm = summary(m)
    checks = {
        "coverage": allm["coverage"] >= MIN_COVERAGE,
        "holdout_lower": hold["mae"] < hold["champion_mae"],
        "dev_first_half_not_worse": halves[0]["mae"] <= halves[0]["champion_mae"],
        "dev_second_half_not_worse": halves[1]["mae"] <= halves[1]["champion_mae"],
        "month_win_share": allm["month_win_share"] >= MIN_MONTH_WIN_SHARE,
    }
    return {"holdout": hold, "dev_halves": halves, "all": allm, "gates": checks,
            "passed": all(checks.values())}


def check(name: str, note: str | None = None, snap: Snapshot | None = None) -> dict:
    snap = snap or Snapshot.load()
    if name == champion_name():
        raise SystemExit(f"{name} is already the champion")
    m = compare(score(name, snap), score(champion_name(), snap))
    rec = base_record("check", name, snap, note) | gates(m)
    log(rec)
    return rec


def promote(name: str) -> dict:
    snap_meta = json.loads((DATA / "meta.json").read_text())
    code = code_hash(name)
    ok = [r for r in read_log() if r["stage"] == "check" and r["candidate"] == name
          and r["code"] == code and r["snapshot"] == snap_meta["exported_at"]
          and r["champion"] == champion_name() and r.get("passed")]
    if not ok:
        raise SystemExit(f"No passing check of {name} (code {code}) on this snapshot "
                         f"against the current champion: run `check` first")
    last = ok[-1]
    champ = {"candidate": name, "code": code, "promoted_at": last["time"],
             "snapshot": last["snapshot"], "replaced": champion_name(),
             "holdout_mae": last["holdout"]["mae"], "all_mae": last["all"]["mae"]}
    CHAMPION.write_text(json.dumps(champ, indent=2) + "\n")
    log({"time": datetime.now(timezone.utc).isoformat(timespec="seconds"), "stage": "promote",
         "candidate": name, "code": code, "replaced": champ["replaced"],
         "snapshot": last["snapshot"]})
    return champ


def read_log() -> list[dict]:
    if not LOG.exists():
        return []
    return [json.loads(x) for x in LOG.read_text(encoding="utf-8").splitlines() if x.strip()]


def show_log() -> str:
    rows = []
    for r in read_log():
        row = {"time": r["time"][:16], "stage": r["stage"], "candidate": r["candidate"],
               "code": r.get("code")}
        if r["stage"] == "dev":
            row |= {"dev_mae": r["dev"]["mae"], "champ_dev_mae": r["dev"]["champion_mae"],
                    "month_wins": r["dev"]["month_win_share"]}
        elif r["stage"] == "check":
            row |= {"holdout_mae": r["holdout"]["mae"],
                    "champ_holdout_mae": r["holdout"]["champion_mae"], "passed": r["passed"]}
        rows.append(row)
    return pd.DataFrame(rows).to_string(index=False) if rows else "(no experiments yet)"


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Week-ahead model bench")
    sub = p.add_subparsers(dest="cmd", required=True)
    for c in ("run", "check", "promote"):
        s = sub.add_parser(c)
        s.add_argument("candidate")
        if c != "promote":
            s.add_argument("--note")
    sub.add_parser("log")
    a = p.parse_args(argv)
    if a.cmd == "log":
        print(show_log())
        return
    res = {"run": lambda: run(a.candidate, a.note), "check": lambda: check(a.candidate, a.note),
           "promote": lambda: promote(a.candidate)}[a.cmd]()
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
