"""Write docs/data-dictionary.md from Unity Catalog (read-only).

    python scripts/data_dictionary.py --warehouse-id <id> [--profile <profile>] [--catalog energy]

Table descriptions come from the comments in the pipeline code, so regenerate this after
changing a table. Warehouse and profile can also come from DATABRICKS_WAREHOUSE_ID and
DATABRICKS_CONFIG_PROFILE.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from research.export import Warehouse

OUT = ROOT / "docs" / "data-dictionary.md"
HIDDEN = {"__START_AT", "__END_AT"}          # SCD2 columns, described once in the intro
LAYERS = {
    "silver": "Typed, deduplicated, UTC. Merged tables keep every revision (SCD type 2): "
              "current rows have `__END_AT IS NULL`, and `__START_AT` records the delivery "
              "that first brought each version.",
    "gold": "Star schema, marts and the week-ahead outlook. Materialized views rebuilt "
            "from silver's current rows.",
}
# Unit from the column name's suffix, longest first.
UNITS = [("_gbp_mwh", "GBP/MWh"), ("_eur_mwh", "EUR/MWh"), ("_usd_mmbtu", "USD/MMBtu"),
         ("_p_kwh", "p/kWh"), ("_mwh", "MWh"), ("_mw", "MW"), ("_ms", "m/s"), ("_wm2", "W/m2"),
         ("_temp_c", "deg C"), ("_utc", "UTC"), ("_gbp", "GBP"), ("_hours", "hours"),
         ("_minutes", "minutes"), ("_share", "fraction 0-1")]
UNITS_EXACT = {"temp_c": "deg C", "avg_temp_c": "deg C"}
# Descriptions for columns whose meaning isn't plain from the name (UC comments win).
NOTES = {
    "settlement_date": "GB settlement day (UK local): 46, 48 or 50 half-hour periods",
    "settlement_period": "Half-hour period of the settlement day, from 1",
    "start_time_utc": "Start of the period (or hour)",
    "system_price_gbp_mwh": "Imbalance (system) price; sell and buy prices are equal since 2015",
    "net_imbalance_volume_mwh": "Net imbalance volume: positive means the system was short",
    "market_index_price_gbp_mwh": "APX market index price; NULL when no volume traded",
    "gb_market_index_price_gbp_mwh": "Daily market index price, volume-weighted over periods",
    "residual_demand_mw": "Demand (INDO, net of embedded wind and solar) minus transmission wind",
    "residual_demand_forecast_mw": "Forecast demand minus forecast wind (same definition)",
    "demand_mw": "National demand outturn (INDO)",
    "transmission_demand_mw": "Transmission system demand outturn (ITSDO)",
    "wind_forecast_scale": "Least-squares factor of metered wind on WINDFOR, days D-365 to D-2",
    "wind_forecast_adjusted_mw": "WINDFOR x wind_forecast_scale",
    "is_complete_day": "Every expected settlement period is loaded",
    "expected_settlement_periods": "46, 48 or 50, from the clock change",
    "settlement_periods_loaded": "Settlement periods with a system price that day",
    "issue_date": "UK day the outlook was issued (its weather forecast fetched)",
    "issued_utc": "When the weather forecast was fetched, or 07:40 UK for archived issues",
    "target_date": "Day being forecast",
    "days_ahead": "target_date minus issue_date, 1 to 7",
    "is_archived_forecast": "Rebuilt from Open-Meteo's Previous Runs API, not a live fetch",
    "windfor_hours": "Hours of the day covered by Elexon's WINDFOR",
    "forecast_hours": "Hours of weather forecast for the day (23, 24 or 25)",
    "wind_source": "Where the day's wind came from: WINDFOR, the power curve, or both",
    "wind_weather_mw": "Weather-forecast wind speed through the power curve",
    "forecast_lead_shortfall_hours": "Most hours any archived value was forecast after the issue",
    "lead_shortfall_hours": "Hours after the issue time this archived value was forecast",
    "lead_days": "Days before valid time the archived forecast was made (1-7)",
    "demand_avg_to_peak_ratio": "Average/peak demand over the 60 complete days before issue",
    "latest_sap_gas_day": "Newest gas day with a SAP known at issue (the day before)",
    "gas_day": "Gas day, 05:00-05:00 UK",
    "data_provider": "APXMIDP (carries the trades) or N2EXMIDP (zero volume)",
    "psr_type": "Solar, Wind Onshore or Wind Offshore",
    "process_type": "Day ahead or Intraday forecast run",
    "publish_time_utc": "When the source published this version or vintage",
    "classification_sequence": "ENTSO-E sequence; 1 is the main auction",
    "is_forward_filled": "ENTSO-E A03 curve point filled from the previous position",
    "weather_hour_utc": "ERA5 hour joined to the period (ERA5 stamps the end of its hour)",
    "median_wind_mw": "Median metered wind at this wind speed over the last 365 days",
    "wind_speed_bin_ms": "Lower edge of a 0.5 m/s bin of GB-average 100 m wind speed",
    "corr60_return_vs_gb_power": "60-trading-day correlation of the return with the GB price change",
    "corr60_return_vs_henry_hub": "60-trading-day correlation of the return with the Henry Hub change",
    "obs_in_window": "Returns in the 60-day window",
    "wind_forecast_mw": "Forecast wind: WINDFOR x scale where it covers the hour, else the power curve",
    "avg_wind_speed_100m_ms": "GB-average wind speed at 100 m",
    "avg_temp_c": "GB-average temperature at 2 m",
    "avg_solar_irradiance_wm2": "GB-average surface solar irradiance",
    "demand_avg_forecast_mw": "NDFD peak forecast x demand_avg_to_peak_ratio",
    "demand_peak_forecast_mw": "Elexon NDFD forecast of the day's peak demand",
}


def unit(col: str) -> str:
    if col in UNITS_EXACT:
        return UNITS_EXACT[col]
    return next((u for suffix, u in UNITS if col.endswith(suffix)), "")


def internal(name: str) -> bool:
    """Databricks' own tables behind pipelines: materializations and event logs."""
    return name.startswith(("__", "event_log_"))


def build(wh: Warehouse, catalog: str) -> str:
    tables = wh.query(f"""
        SELECT table_schema, table_name, comment FROM `{catalog}`.information_schema.tables
        WHERE table_schema IN ('bronze', 'silver', 'gold') ORDER BY table_schema, table_name""")
    cols = wh.query(f"""
        SELECT table_schema, table_name, column_name, full_data_type, comment, ordinal_position
        FROM `{catalog}`.information_schema.columns
        WHERE table_schema IN ('silver', 'gold')
        ORDER BY table_schema, table_name, ordinal_position""")
    tables = tables[~tables.table_name.map(internal)].copy()
    tables["comment"] = tables.comment.fillna("").str.strip()
    cols["comment"] = cols.comment.fillna("").str.strip()
    ended = cols[cols.column_name == "__END_AT"]
    scd2 = set(zip(ended.table_schema, ended.table_name))
    bronze = tables[tables.table_schema == "bronze"].table_name

    out = [
        "# Data dictionary",
        "",
        (f"Generated from Unity Catalog (`{catalog}`) on {date.today().isoformat()} by "
        "`scripts/data_dictionary.py`. Table descriptions are the comments set in "
        "`src/pipeline/`; units come from column-name suffixes, and descriptions of the "
        "less obvious columns from the script."),
        "",
        "**Bronze** (`bronze.*`) holds the landed files as they arrived, append-only, with "
        "`_file_modified_at` and `_source_file` on every row: "
        + ", ".join(f"`{t}`" for t in bronze) + ".",
        "",
    ]
    for layer, intro in LAYERS.items():
        out += [f"## {layer.title()}", "", intro, ""]
        lt = tables[tables.table_schema == layer]
        out += ["| Table | Description |", "|---|---|"]
        out += [f"| [`{r.table_name}`](#{layer}{r.table_name}) | {r.comment}"
                f"{' (SCD type 2)' if (layer, r.table_name) in scd2 else ''} |"
                for r in lt.itertuples()]
        out.append("")
        for r in lt.itertuples():
            c = cols[(cols.table_schema == layer) & (cols.table_name == r.table_name)
                     & ~cols.column_name.isin(HIDDEN)]
            out += [f"### {layer}.{r.table_name}", ""]
            if r.comment:
                out += [r.comment, ""]
            out += ["| Column | Type | Unit | Description |", "|---|---|---|---|"]
            out += [f"| `{x.column_name}` | {x.full_data_type.lower()} | {unit(x.column_name)} | "
                    f"{x.comment or NOTES.get(x.column_name, '')} |" for x in c.itertuples()]
            out.append("")
    return "\n".join(out)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--warehouse-id", default=os.environ.get("DATABRICKS_WAREHOUSE_ID"))
    p.add_argument("--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE"))
    p.add_argument("--catalog", default="energy")
    a = p.parse_args(argv)
    if not a.warehouse_id:
        p.error("--warehouse-id or DATABRICKS_WAREHOUSE_ID is required")
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(build(Warehouse(a.warehouse_id, a.profile), a.catalog) + "\n", encoding="utf-8")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
