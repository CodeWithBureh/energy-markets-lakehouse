import json
import types

import numpy as np
import pandas as pd
import pytest

from research import bench
from research.candidates import report_blend


def _snapshot(days=500, seed=0):
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2025-03-01", periods=days, freq="D")
    resid = rng.uniform(10, 30, days)
    sap = 30 + np.cumsum(rng.normal(0, 0.5, days))
    sap_prev = np.r_[np.nan, sap[:-1]]
    price = 5 + 4 * resid + 1.5 * sap_prev
    daily = pd.DataFrame({
        "date": dates, "is_complete_day": True, "gb_market_index_price_gbp_mwh": price,
        "gb_avg_residual_demand_mw": resid * 1000, "gb_avg_demand_mw": resid * 1000 + 8000,
        "gb_gas_sap_gbp_mwh": sap})
    rows = []
    for i in range(70, days - 7):
        for k in range(1, 8):
            rows.append({"issue_date": dates[i], "target_date": dates[i + k], "days_ahead": k,
                         "residual_demand_forecast_mw": resid[i + k] * 1000,
                         "latest_sap_gbp_mwh": sap_prev[i + k]})
    hh = pd.DataFrame({"settlement_date": dates, "settlement_period": 1})
    d, o, h = bench.prepare(daily, pd.DataFrame(rows), hh)
    return bench.Snapshot(d, o, h, {"exported_at": "2026-11-22T00:00:00+00:00"})


@pytest.fixture
def tmp_bench(tmp_path, monkeypatch):
    monkeypatch.setattr(bench, "DATA", tmp_path / "data")
    monkeypatch.setattr(bench, "LOG", tmp_path / "experiments.jsonl")
    monkeypatch.setattr(bench, "CHAMPION", tmp_path / "champion.json")
    monkeypatch.setattr(bench, "DEV_END", pd.Timestamp("2026-01-31"))
    (tmp_path / "champion.json").write_text(json.dumps({"candidate": "champ"}))
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "meta.json").write_text(
        json.dumps({"exported_at": "2026-11-22T00:00:00+00:00"}))
    mods = {
        "champ": lambda i: i.rows.resid_f_gw * 4 + 100,                  # rough
        "good": lambda i: report_blend.estimate(i, weight=0),            # exact on this data
    }
    monkeypatch.setattr(bench, "load_candidate", lambda n: types.SimpleNamespace(
        predict=mods[n], DESCRIPTION=n))
    monkeypatch.setattr(bench, "code_hash", lambda n: f"{n}-hash")
    return tmp_path


def test_issues_only_see_the_past():
    snap = _snapshot()
    for issue in list(bench.issues(snap))[::50]:
        assert (issue.daily.date < issue.issue_date).all()
        assert (issue.hh.settlement_date < issue.issue_date).all()
        assert (issue.rows.issue_date == issue.issue_date).all()


def test_report_blend_recovers_an_exact_linear_price():
    snap = _snapshot()
    issue = list(bench.issues(snap))[100]
    pred = report_blend.estimate(issue, weight=0)
    actual = snap.daily.set_index("date").price.reindex(issue.rows.target_date).to_numpy()
    assert np.allclose(pred.to_numpy(), actual)


def test_gates_and_promote(tmp_bench):
    snap = _snapshot()
    rec = bench.run("good", "exact model", snap=snap)
    assert rec["dev"]["mae"] < rec["dev"]["champion_mae"]
    with pytest.raises(SystemExit):                   # no check yet
        bench.promote("good")
    chk = bench.check("good", snap=snap)
    assert chk["passed"] and all(chk["gates"].values())
    champ = bench.promote("good")
    assert champ["candidate"] == "good" and champ["replaced"] == "champ"
    stages = [r["stage"] for r in bench.read_log()]
    assert stages == ["dev", "check", "promote"]


def test_a_worse_candidate_fails_its_check(tmp_bench):
    snap = _snapshot()
    (tmp_bench / "champion.json").write_text(json.dumps({"candidate": "good"}))
    chk = bench.check("champ", snap=snap)
    assert not chk["passed"] and not chk["gates"]["holdout_lower"]
