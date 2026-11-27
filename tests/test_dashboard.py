"""The dashboard JSON is generated from SQL and dashboards/build.py; keep the two in step."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "dashboards"))

import build


def test_committed_json_matches_the_build():
    committed = build.OUT.read_text(encoding="utf-8")
    assert committed == build.render(), "run: python dashboards/build.py"


def test_widgets_use_existing_datasets_and_fit_the_grid():
    dash = json.loads(build.render())
    datasets = {d["name"] for d in dash["datasets"]}
    for page in dash["pages"]:
        cells = set()
        for item in page["layout"]:
            pos = item["position"]
            assert pos["x"] + pos["width"] <= 6, item["widget"]["name"]
            for x in range(pos["x"], pos["x"] + pos["width"]):
                for y in range(pos["y"], pos["y"] + pos["height"]):
                    assert (x, y) not in cells, f"{item['widget']['name']} overlaps"
                    cells.add((x, y))
            for q in item["widget"].get("queries", []):
                assert q["query"]["datasetName"] in datasets


def test_datasets_are_unqualified_and_reuse_the_report():
    for name in build.DATASETS:
        sql = build.dataset_sql(name)
        assert "energy.gold." not in sql
        assert not sql.rstrip().endswith(";")
    assert "past_error" in build.dataset_sql("outlook")      # the report, not a copy
    assert "FROM scored" in build.dataset_sql("backtest_kpis")
    assert "-- Summary by days ahead" not in build.dataset_sql("backtest_kpis")
