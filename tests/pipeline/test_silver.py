from datetime import date, datetime

import pytest
from conftest import collect_utc
from pyspark.sql import functions as F

ELEXON = "data ARRAY<STRUCT<{}>>, _file_modified_at TIMESTAMP, _source_file STRING"
LANDED = datetime(2026, 11, 1, 6, 0)


def test_settlement_periods_start_in_utc_on_clock_change_days(pipeline, df):
    silver, _, _ = pipeline("silver")
    rows = [(date(2026, 3, 29), 1), (date(2026, 3, 29), 46),     # spring: 46 periods
            (date(2026, 10, 25), 1), (date(2026, 10, 25), 50),   # autumn: 50 periods
            (date(2026, 7, 1), 1)]
    out = collect_utc(df(rows, "d DATE, p INT")
                      .select(silver.settlement_start_utc("d", F.col("p")).alias("t")))
    assert [r.t for r in out] == [
        datetime(2026, 3, 29, 0, 0),     # GMT midnight
        datetime(2026, 3, 29, 22, 30),   # last period, 23:30 BST
        datetime(2026, 10, 24, 23, 0),   # BST midnight is 23:00 UTC the day before
        datetime(2026, 10, 25, 23, 30),  # last period, 23:30 GMT
        datetime(2026, 6, 30, 23, 0),
    ]


def test_system_prices_are_typed_and_start_times_derived(pipeline, df):
    silver, _, spark = pipeline("silver")
    spark.tables["bronze.elexon_system_prices"] = df(
        [([{"settlementDate": "2026-10-25", "settlementPeriod": 50, "startTime": "2026-10-25T23:30:00Z",
            "systemSellPrice": "85.5", "systemBuyPrice": "85.5", "netImbalanceVolume": "-120.0"}],
          LANDED, "f1.json")],
        ELEXON.format("settlementDate STRING, settlementPeriod INT, startTime STRING, "
                      "systemSellPrice STRING, systemBuyPrice STRING, netImbalanceVolume STRING"))
    r = collect_utc(silver.gb_system_prices_updates())[0]
    assert (r.settlement_date, r.settlement_period, r.system_price_gbp_mwh) == (
        date(2026, 10, 25), 50, 85.5)
    assert r.start_time_utc == r.api_start_time_utc == datetime(2026, 10, 25, 23, 30)


def test_merged_tables_use_scd2_on_their_business_keys(pipeline):
    _, dlt, _ = pipeline("silver")
    flows = dlt.cdc_flows
    assert flows["silver.gb_system_prices"]["keys"] == ["settlement_date", "settlement_period"]
    assert all(f["stored_as_scd_type"] == 2 for f in flows.values())
    # Lineage never counts as a change; Elexon publish time orders versions but isn't compared.
    gen = flows["silver.gb_generation_by_fuel"]
    assert {"_file_modified_at", "_source_file", "publish_time_utc"} <= set(gen["except_column_list"])
    # Forecasts keep every vintage: publish time is part of the key.
    assert flows["silver.gb_wind_forecast"]["keys"][0] == "publish_time_utc"
    assert set(flows) == set(dlt.streaming_tables)


def test_share_prices_quoted_in_pence_become_pounds(pipeline, df):
    silver, _, spark = pipeline("silver")
    cols = ("ticker STRING, trade_date STRING, quote_currency STRING, open DOUBLE, high DOUBLE, "
            "low DOUBLE, close DOUBLE, adj_close DOUBLE, volume DOUBLE, dividends DOUBLE, "
            "_file_modified_at TIMESTAMP, _source_file STRING")
    spark.tables["bronze.stock_daily_prices"] = df(
        [("SSE.L", "2026-11-20", "GBp", 1800.0, 1820.0, 1790.0, 1810.0, 1810.0, 1e6, 0.0, LANDED, "a"),
         ("BP.L", "2026-11-20", "GBP", 4.1, 4.2, 4.0, 4.15, 4.15, 2e6, 0.0, LANDED, "a")], cols)
    out = {r.ticker: r for r in silver.stock_prices_daily_updates().collect()}
    assert out["SSE.L"].close_gbp == pytest.approx(18.10)
    assert out["BP.L"].close_gbp == pytest.approx(4.15)


def test_era5_is_converted_to_friendly_units(pipeline, df):
    silver, _, spark = pipeline("silver")
    spark.tables["bronze.era5_gb_hourly"] = df(
        [("2026-01-01T12:00:00", 52.004, -1.249, 283.15, 3.0, 4.0, 6.0, 8.0, 360000.0, LANDED, "e")],
        "valid_time_utc STRING, latitude DOUBLE, longitude DOUBLE, t2m DOUBLE, u10 DOUBLE, "
        "v10 DOUBLE, u100 DOUBLE, v100 DOUBLE, ssrd DOUBLE, _file_modified_at TIMESTAMP, "
        "_source_file STRING")
    r = silver.era5_gb_grid_hourly_updates().collect()[0]
    assert r.temp_c == pytest.approx(10.0)
    assert (r.wind_speed_10m_ms, r.wind_speed_100m_ms) == (5.0, 10.0)
    assert r.solar_irradiance_wm2 == pytest.approx(100.0)          # J/m^2 per hour -> W/m^2
    assert (r.latitude, r.longitude) == (52.0, -1.25)


def test_fred_missing_values_become_null_and_fx_is_derived(pipeline, df):
    silver, _, spark = pipeline("silver")
    spark.tables["bronze.fred_observations"] = df(
        [("DEXUSUK", [{"date": "2026-11-20", "value": "1.25"}, {"date": "2026-11-21", "value": "."}],
          LANDED, "f"),
         ("DEXUSEU", [{"date": "2026-11-20", "value": "1.10"}], LANDED, "f")],
        "series_id STRING, observations ARRAY<STRUCT<date STRING, value STRING>>, "
        "_file_modified_at TIMESTAMP, _source_file STRING")
    obs = silver.fred_observations_updates()
    assert obs.filter("value IS NULL").count() == 1
    spark.tables["silver.fred_observations"] = obs.withColumn("__END_AT", F.lit(None))
    fx = silver.fx_rates_daily().filter("fx_date = '2026-11-20'").collect()[0]
    assert fx.gbp_per_eur == pytest.approx(1.10 / 1.25)
