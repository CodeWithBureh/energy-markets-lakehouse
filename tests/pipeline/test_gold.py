from datetime import date, datetime, timedelta

import pytest
from conftest import collect_utc


@pytest.fixture
def gold(pipeline):
    mod, _, spark = pipeline("gold")
    return mod, spark.tables


def test_s_reads_only_current_rows_of_scd2_tables(gold, df):
    g, t = gold
    t["silver.gb_gas_sap_daily"] = df(
        [(date(2026, 11, 1), 70.0, None), (date(2026, 11, 1), 69.0, datetime(2026, 11, 3))],
        "gas_day DATE, sap_gbp_mwh DOUBLE, __END_AT TIMESTAMP")
    assert [r.sap_gbp_mwh for r in g.s("gb_gas_sap_daily").collect()] == [70.0]


def test_dim_date_knows_46_48_and_50_period_days(gold):
    g, _ = gold
    periods = {r.date: r.expected_settlement_periods for r in g.dim_date()
               .filter("date IN ('2026-03-29', '2026-07-01', '2026-10-25')").collect()}
    assert periods == {date(2026, 3, 29): 46, date(2026, 7, 1): 48, date(2026, 10, 25): 50}


def test_half_hourly_fact_builds_the_mix_residual_demand_and_weather(gold, df):
    g, t = gold
    d, start = date(2026, 11, 20), datetime(2026, 11, 20, 10, 30)
    key = "settlement_date DATE, settlement_period INT"
    t["silver.gb_generation_by_fuel"] = df(
        [(d, 22, f, mw) for f, mw in [("WIND", 5000.0), ("CCGT", 3000.0), ("OCGT", 500.0),
                                      ("NUCLEAR", 4000.0), ("INTFR", 1500.0), ("INTNOR", -200.0)]],
        f"{key}, fuel_type STRING, generation_mw DOUBLE")
    t["silver.gb_system_prices"] = df([(d, 22, start, 90.0, -50.0)],
                                      f"{key}, start_time_utc TIMESTAMP, "
                                      "system_price_gbp_mwh DOUBLE, net_imbalance_volume_mwh DOUBLE")
    t["silver.gb_demand_outturn"] = df([(d, 22, 20000.0, 22000.0)],
                                       f"{key}, demand_mw DOUBLE, transmission_demand_mw DOUBLE")
    t["silver.gb_market_index_prices"] = df(
        [(d, 22, "APXMIDP", 88.0, 0.0), (d, 22, "N2EXMIDP", 0.0, 0.0)],
        f"{key}, data_provider STRING, price_gbp_mwh DOUBLE, volume_mwh DOUBLE")
    # ERA5 is stamped at the end of its hour: 10:30 takes the 11:00 record.
    t["gold.gb_weather_hourly"] = df(
        [(datetime(2026, 11, 20, 10), 1.0, 1.0, 1.0, 9), (datetime(2026, 11, 20, 11), 7.0, 9.5, 50.0, 9)],
        "valid_time_utc TIMESTAMP, avg_temp_c DOUBLE, avg_wind_speed_100m_ms DOUBLE, "
        "avg_solar_irradiance_wm2 DOUBLE, grid_cells INT")
    r = collect_utc(g.fact_gb_half_hourly())[0]
    assert (r.wind_mw, r.gas_mw, r.net_imports_mw, r.domestic_generation_mw) == (
        5000.0, 3500.0, 1300.0, 12500.0)
    assert r.residual_demand_mw == 15000.0
    assert r.market_index_price_gbp_mwh is None          # no volume, no real price
    assert (r.weather_hour_utc, r.avg_temp_c) == (datetime(2026, 11, 20, 11), 7.0)
    assert r.wind_share == pytest.approx(0.4)


def test_day_ahead_forecasts_use_the_last_vintage_before_the_day_and_scale_windfor(gold, df):
    g, t = gold
    days = [date(2026, 7, 1) + timedelta(days=i) for i in range(100)]
    t["silver.gb_wind_solar_forecast"] = df(
        [(datetime(2026, 6, 30, 15, 45), "Day ahead", "Solar", days[0], 1, 0.0)],
        "publish_time_utc TIMESTAMP, process_type STRING, psr_type STRING, "
        "settlement_date DATE, settlement_period INT, forecast_mw DOUBLE")
    # Metered wind is 80% of WINDFOR on every period.
    t["gold.fact_gb_half_hourly"] = df(
        [(d, p, 8000.0) for d in days for p in range(1, 49)],
        "settlement_date DATE, settlement_period INT, wind_mw DOUBLE")
    t["silver.gb_wind_forecast"] = df(
        [(datetime(d.year, d.month, d.day) - timedelta(hours=10),
          datetime(d.year, d.month, d.day) + timedelta(minutes=30 * (p - 1)), 10000.0)
         for d in days for p in range(1, 49, 2)],
        "publish_time_utc TIMESTAMP, start_time_utc TIMESTAMP, wind_mw DOUBLE")
    t["silver.gb_demand_forecast"] = df(
        [(datetime(d.year, d.month, d.day) - timedelta(hours=10), d, p,
          datetime(d.year, d.month, d.day) + timedelta(minutes=30 * (p - 1)), 30000.0)
         for d in days for p in range(1, 49)]
        + [(datetime(2026, 9, 1, 6), date(2026, 9, 1), 1, datetime(2026, 9, 1), 99999.0)],
        "publish_time_utc TIMESTAMP, settlement_date DATE, settlement_period INT, "
        "start_time_utc TIMESTAMP, demand_mw DOUBLE")
    out = {(r.settlement_date, r.settlement_period): r
           for r in g.fact_gb_day_ahead_forecasts().filter("settlement_period = 1").collect()}
    # 1 Sep (summer): local midnight is 23:00 UTC on 31 Aug, so 06:00 is too late.
    sep1 = out[(date(2026, 9, 1), 1)]
    assert sep1.demand_forecast_mw == 30000.0
    assert sep1.residual_demand_forecast_mw == 20000.0
    # Under 90 days of history: no correction. Once there is: k = 0.8.
    assert out[(date(2026, 8, 1), 1)].wind_forecast_scale is None
    late = out[(days[-1], 1)]
    assert late.wind_forecast_scale == pytest.approx(0.8)
    assert late.residual_demand_forecast_adjusted_mw == pytest.approx(22000.0)


def _outlook_inputs(t, df):
    issue = date(2026, 11, 22)
    d1 = datetime(2026, 11, 23)                       # GMT, so UK days are UTC days
    hours = [d1 + timedelta(hours=h) for h in range(24)]
    wx = "issued_utc TIMESTAMP, valid_time_utc TIMESTAMP, latitude DOUBLE, longitude DOUBLE, " \
         "wind_speed_100m_ms DOUBLE, temp_c DOUBLE"
    live = [(datetime(2026, 11, 22, 7, 40), h, 55.0, lon, 2.0, 1.0, 100.0)
            for h in hours for lon in (-3.0, -2.0)]
    live += [(datetime(2026, 11, 22, 12, 0), h, 55.0, lon, ws, 5.0, 100.0)
             for h in hours for lon, ws in ((-3.0, 8.2), (-2.0, 8.6))]
    t["silver.weather_forecast_gb_hourly"] = df(live, wx + ", solar_irradiance_wm2 DOUBLE")
    # The archive has the same day (ignored: live wins) and 21 Nov (used).
    arch = [(datetime(2026, 11, 22, 7, 40), h, 55.0, -3.0, 20.0, 0.0, 0) for h in hours]
    arch += [(datetime(2026, 11, 21, 7, 40), h - timedelta(days=1), 55.0, -3.0, 4.2, 3.0, 0)
             for h in hours]
    t["silver.weather_forecast_gb_hourly_archive"] = df(arch, wx + ", lead_shortfall_hours INT")
    t["gold.gb_wind_power_curve"] = df([(b / 2, b / 2 * 1000.0) for b in range(25)],
                                       "wind_speed_bin_ms DOUBLE, median_wind_mw DOUBLE")
    # WINDFOR from 22:30 the night before covers 22 of the 24 hours; one published after
    # the issue must be ignored.
    t["silver.gb_wind_forecast"] = df(
        [(datetime(2026, 11, 21, 22, 30), h, 7000.0) for h in hours[:22]]
        + [(datetime(2026, 11, 22, 13, 0), h, 1.0) for h in hours],
        "publish_time_utc TIMESTAMP, start_time_utc TIMESTAMP, wind_mw DOUBLE")
    t["gold.fact_gb_day_ahead_forecasts"] = df([(issue, 0.9), (date(2026, 11, 21), 0.9)],
                                               "settlement_date DATE, wind_forecast_scale DOUBLE")
    t["silver.gb_demand_forecast_daily"] = df(
        [(datetime(2026, 11, 21, 14, 45), date(2026, 11, 23), 40000.0),
         (datetime(2026, 11, 20, 14, 45), date(2026, 11, 22), 41000.0),
         (datetime(2026, 11, 22, 14, 45), date(2026, 11, 23), 45000.0)],
        "publish_time_utc TIMESTAMP, forecast_date DATE, demand_peak_mw DOUBLE")
    t["gold.fact_market_daily"] = df(
        [(issue - timedelta(days=i), True, 30000.0, 40000.0) for i in range(1, 61)],
        "date DATE, is_complete_day BOOLEAN, gb_avg_demand_mw DOUBLE, gb_peak_demand_mw DOUBLE")
    t["silver.gb_gas_sap_daily"] = df(
        [(date(2026, 11, 20), 70.0), (date(2026, 11, 21), 75.0), (date(2026, 11, 22), 99.0)],
        "gas_day DATE, sap_gbp_mwh DOUBLE")
    t["silver.ttf_gas_daily"] = df([(date(2026, 11, 20), 88.0), (date(2026, 11, 21), 89.0)],
                                   "trade_date DATE, close_eur_mwh DOUBLE")


def test_week_ahead_outlook(gold, df):
    g, t = gold
    _outlook_inputs(t, df)
    rows = {r.issue_date: r for r in collect_utc(g.fact_gb_week_ahead_outlook())}
    assert set(rows) == {date(2026, 11, 21), date(2026, 11, 22)}

    r = rows[date(2026, 11, 22)]
    # The last live fetch of the day wins over the earlier one and the archive.
    assert (r.issued_utc, r.is_archived_forecast) == (datetime(2026, 11, 22, 12), False)
    assert (r.target_date, r.days_ahead, r.forecast_hours) == (date(2026, 11, 23), 1, 24)
    assert r.avg_wind_speed_100m_ms == pytest.approx(8.4)
    # WINDFOR x 0.9 for its 22 hours, the power curve (8.4 m/s -> 8.0 bin -> 8000 MW) for 2.
    assert r.windfor_hours == 22
    assert r.wind_forecast_mw == pytest.approx((22 * 6300 + 2 * 8000) / 24)
    assert r.wind_source == "WINDFOR x scale for 22 of 24 hours, power curve for the rest"
    # NDFD published before the issue, scaled by the 60-day average/peak ratio.
    assert (r.demand_peak_forecast_mw, r.demand_avg_to_peak_ratio) == (40000.0, 0.75)
    assert r.residual_demand_forecast_mw == pytest.approx(30000 - r.wind_forecast_mw)
    # Gas known at issue: SAP for the day before, the last TTF close.
    assert (r.latest_sap_gas_day, r.latest_sap_gbp_mwh, r.latest_ttf_eur_mwh) == (
        date(2026, 11, 21), 75.0, 89.0)

    a = rows[date(2026, 11, 21)]
    assert a.is_archived_forecast and a.avg_solar_irradiance_wm2 is None
    assert a.wind_source == "weather forecast x power curve"
    assert a.wind_forecast_mw == 4000.0


def test_market_daily_weights_the_index_price_and_flags_complete_days(gold, df):
    g, t = gold
    t["gold.dim_date"] = g.dim_date()
    spring, fri, sat = date(2026, 3, 29), date(2026, 11, 20), date(2026, 11, 21)
    hh = [(spring, p, 50.0, 0.0, 80.0 if p == 1 else 100.0, 1.0 if p == 1 else 3.0)
          for p in range(1, 47)]
    hh += [(d, p, 60.0, 0.0, 90.0, 1.0) for d in (fri, sat) for p in range(1, 48)]   # 47 of 48
    t["gold.fact_gb_half_hourly"] = df(
        [r + (25000.0, 15000.0, 0.3, 0.3, 8.0, 9.0) for r in hh],
        "settlement_date DATE, settlement_period INT, system_price_gbp_mwh DOUBLE, "
        "net_imbalance_volume_mwh DOUBLE, market_index_price_gbp_mwh DOUBLE, "
        "market_index_volume_mwh DOUBLE, demand_mw DOUBLE, residual_demand_mw DOUBLE, "
        "wind_share DOUBLE, gas_share DOUBLE, avg_temp_c DOUBLE, avg_wind_speed_100m_ms DOUBLE")
    t["gold.fact_eu_day_ahead_hourly"] = df([], "zone STRING, hour_utc TIMESTAMP, price_gbp_mwh DOUBLE")
    t["gold.dim_fx_daily"] = df([(d, 1.25, 1.1, 0.88) for d in (spring, fri, sat)],
                                "date DATE, usd_per_gbp DOUBLE, usd_per_eur DOUBLE, gbp_per_eur DOUBLE")
    t["silver.gas_prices"] = df([], "benchmark STRING, obs_date DATE, price_usd_mmbtu DOUBLE")
    t["silver.gb_gas_sap_daily"] = df([(fri, 74.0)], "gas_day DATE, sap_gbp_mwh DOUBLE")
    t["silver.ttf_gas_daily"] = df([(fri, 88.0)], "trade_date DATE, close_eur_mwh DOUBLE")
    out = {r.date: r for r in g.fact_market_daily().collect()}
    s = out[spring]
    assert s.is_complete_day and s.settlement_periods_loaded == 46
    # Volume-weighted: (80 x 1 + 100 x 3 x 45) / (1 + 3 x 45)
    assert s.gb_market_index_price_gbp_mwh == pytest.approx((80 + 300 * 45) / 136)
    assert not out[fri].is_complete_day
    # TTF trades on weekdays: Saturday carries Friday's close.
    assert out[sat].ttf_eur_mwh == 88.0 and out[sat].ttf_gbp_mwh == pytest.approx(88 * 0.88)


def test_dq_lists_only_days_with_missing_periods(gold, df):
    g, t = gold
    t["gold.dim_date"] = g.dim_date()
    t["silver.gb_system_prices"] = df(
        [(date(2026, 10, 25), p) for p in range(1, 51)] + [(date(2026, 11, 1), p) for p in range(1, 47)],
        "settlement_date DATE, settlement_period INT")
    rows = g.dq_settlement_period_completeness().collect()
    assert [(r.date, r.expected_settlement_periods, r.periods_loaded) for r in rows] == [
        (date(2026, 11, 1), 48, 46)]


def test_stock_returns(gold, df):
    g, t = gold
    t["silver.stock_prices_daily"] = df(
        [("SSE.L", date(2026, 11, 19), 18.0, 18.0, 1), ("SSE.L", date(2026, 11, 20), 18.9, None, 1)],
        "ticker STRING, trade_date DATE, close_gbp DOUBLE, adj_close_gbp DOUBLE, volume LONG")
    t["gold.dim_company"] = g.dim_company()
    r = g.fact_stock_returns_daily().filter("trade_date = '2026-11-20'").collect()[0]
    assert r.daily_return == pytest.approx(0.05) and r.company == "SSE"
