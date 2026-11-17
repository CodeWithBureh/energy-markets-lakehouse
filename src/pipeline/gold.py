# Lakeflow Declarative Pipeline: GOLD layer
# ---------------------------------------------------------------------------------------
# Business-ready star schema and marts for the dashboard. All materialized views:
# serverless refreshes them incrementally where it can, and the data is small enough
# that a full recompute is cheap anyway.

import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F

SILVER = spark.conf.get("energy.silver_schema")  # noqa: F821
GOLD = spark.conf.get("energy.gold_schema")  # noqa: F821


def s(name):
    return spark.read.table(f"{SILVER}.{name}")  # noqa: F821


def g(name):
    return spark.read.table(f"{GOLD}.{name}")  # noqa: F821


def _ffill(col):
    w = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    return F.last(col, ignorenulls=True).over(w)


# --- Dimensions ------------------------------------------------------------------------

@dlt.table(name=f"{GOLD}.dim_date",
           comment="Calendar with the expected number of GB settlement periods per day.")
def dim_date():
    days = spark.sql(  # noqa: F821
        "SELECT explode(sequence(DATE'2020-01-01', date_add(current_date(), 7), "
        "INTERVAL 1 DAY)) AS date"
    )
    midnight = lambda c: F.to_utc_timestamp(F.to_timestamp(c), "Europe/London")
    return days.select(
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.date_format("date", "E").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        (
            (F.unix_timestamp(midnight(F.date_add("date", 1))) - F.unix_timestamp(midnight("date")))
            / 1800
        ).cast("int").alias("expected_settlement_periods"),
    )


@dlt.table(name=f"{GOLD}.dim_company", comment="Energy companies tracked.")
def dim_company():
    rows = [
        ("SSE.L", "SSE", "Generation & networks"),
        ("CNA.L", "Centrica", "Supply & trading"),
        ("NG.L", "National Grid", "Networks"),
        ("DRX.L", "Drax", "Generation (biomass)"),
        ("BP.L", "BP", "Oil & gas"),
        ("SHEL.L", "Shell", "Oil & gas"),
    ]
    return spark.createDataFrame(rows, "ticker STRING, company STRING, segment STRING")  # noqa: F821


@dlt.table(name=f"{GOLD}.dim_fx_daily",
           comment="Daily FX with weekends/holidays forward-filled from the last published rate.")
def dim_fx_daily():
    fx = s("fx_rates_daily").withColumnRenamed("fx_date", "date")
    joined = g("dim_date").select("date").join(fx, "date", "left")
    return joined.select(
        "date",
        _ffill("usd_per_gbp").alias("usd_per_gbp"),
        _ffill("usd_per_eur").alias("usd_per_eur"),
        _ffill("gbp_per_eur").alias("gbp_per_eur"),
    )


# --- Weather ---------------------------------------------------------------------------

@dlt.table(name=f"{GOLD}.gb_weather_hourly",
           comment="GB-average hourly weather (simple mean over the ERA5 grid box).")
def gb_weather_hourly():
    # Next step: weight wind by wind-farm locations (UK REPD dataset) and temperature by
    # population, rather than a flat mean that includes sea cells.
    return s("era5_gb_grid_hourly").groupBy("valid_time_utc").agg(
        F.avg("temp_c").alias("avg_temp_c"),
        F.avg("wind_speed_100m_ms").alias("avg_wind_speed_100m_ms"),
        F.avg("solar_irradiance_wm2").alias("avg_solar_irradiance_wm2"),
        F.count("*").alias("grid_cells"),
    )


# --- Facts -----------------------------------------------------------------------------

@dlt.table(name=f"{GOLD}.fact_gb_half_hourly",
           comment="One row per GB settlement period: price, imbalance, generation mix, weather.")
def fact_gb_half_hourly():
    gen = s("gb_generation_by_fuel")
    ft = F.col("fuel_type")
    mix = gen.groupBy("settlement_date", "settlement_period").agg(
        F.sum(F.when(ft == "WIND", F.col("generation_mw"))).alias("wind_mw"),
        F.sum(F.when(ft.isin("CCGT", "OCGT"), F.col("generation_mw"))).alias("gas_mw"),
        F.sum(F.when(ft == "NUCLEAR", F.col("generation_mw"))).alias("nuclear_mw"),
        F.sum(F.when(ft == "BIOMASS", F.col("generation_mw"))).alias("biomass_mw"),
        F.sum(F.when(ft == "COAL", F.col("generation_mw"))).alias("coal_mw"),
        F.sum(F.when(ft.isin("NPSHYD", "PS"), F.col("generation_mw"))).alias("hydro_and_storage_mw"),
        F.sum(F.when(ft.startswith("INT"), F.col("generation_mw"))).alias("net_imports_mw"),
        F.sum(F.when(~ft.startswith("INT"), F.col("generation_mw"))).alias("domestic_generation_mw"),
    )
    prices = s("gb_system_prices").select(
        "settlement_date", "settlement_period", "start_time_utc",
        "system_price_gbp_mwh", "net_imbalance_volume_mwh",
    )
    # ERA5 values are stamped at the END of their hour, so a half-hour starting at 10:00
    # or 10:30 maps to the ERA5 record valid at 11:00.
    weather = g("gb_weather_hourly")
    df = (
        prices.join(mix, ["settlement_date", "settlement_period"], "left")
        .withColumn("weather_hour_utc", F.date_trunc("hour", "start_time_utc") + F.expr("INTERVAL 1 HOUR"))
        .join(weather.withColumnRenamed("valid_time_utc", "weather_hour_utc"), "weather_hour_utc", "left")
        .withColumn("wind_share", F.col("wind_mw") / F.col("domestic_generation_mw"))
        .withColumn("gas_share", F.col("gas_mw") / F.col("domestic_generation_mw"))
    )
    return df.drop("grid_cells")


@dlt.table(name=f"{GOLD}.fact_eu_day_ahead_hourly",
           comment="Hourly day-ahead prices per neighbour zone in EUR and GBP. Where a zone "
                   "publishes several resolutions, the finest one is used.")
def fact_eu_day_ahead_hourly():
    p = s("eu_day_ahead_prices").filter("classification_sequence = 1")
    p = p.withColumn("hour_utc", F.date_trunc("hour", "delivery_start_utc"))
    w = Window.partitionBy("zone", "hour_utc")
    finest = p.withColumn("_min_res", F.min("resolution_minutes").over(w)).filter(
        "resolution_minutes = _min_res"
    )
    hourly = finest.groupBy("zone", "hour_utc").agg(
        F.avg("price_eur_mwh").alias("price_eur_mwh"),
        F.min("resolution_minutes").alias("source_resolution_minutes"),
    )
    fx = g("dim_fx_daily").select(F.col("date").alias("_d"), "gbp_per_eur")
    return (
        hourly.join(fx, F.to_date("hour_utc") == F.col("_d"), "left")
        .withColumn("price_gbp_mwh", F.col("price_eur_mwh") * F.col("gbp_per_eur"))
        .drop("_d")
    )


@dlt.table(name=f"{GOLD}.fact_market_daily",
           comment="One row per day: GB price stats, neighbour prices, gas, FX, mix and weather.")
def fact_market_daily():
    gb = g("fact_gb_half_hourly").groupBy(F.col("settlement_date").alias("date")).agg(
        F.avg("system_price_gbp_mwh").alias("gb_avg_price_gbp_mwh"),
        F.min("system_price_gbp_mwh").alias("gb_min_price_gbp_mwh"),
        F.max("system_price_gbp_mwh").alias("gb_max_price_gbp_mwh"),
        F.stddev("system_price_gbp_mwh").alias("gb_price_volatility"),
        F.sum("net_imbalance_volume_mwh").alias("gb_net_imbalance_mwh"),
        F.avg("wind_share").alias("avg_wind_share"),
        F.avg("gas_share").alias("avg_gas_share"),
        F.avg("avg_temp_c").alias("avg_temp_c"),
        F.avg("avg_wind_speed_100m_ms").alias("avg_wind_speed_100m_ms"),
        F.count("*").alias("settlement_periods_loaded"),
    )
    zones = ["FR", "NL", "BE", "NO2", "DK1", "IE_SEM"]
    eu = (
        g("fact_eu_day_ahead_hourly")
        .withColumn("date", F.to_date(F.from_utc_timestamp("hour_utc", "Europe/London")))
        # Conditional aggregation instead of pivot (unsupported in Lakeflow pipelines).
        .groupBy("date").agg(*[
            F.avg(F.when(F.col("zone") == z, F.col("price_gbp_mwh")))
            .alias(f"{z.lower()}_avg_price_gbp_mwh")
            for z in zones
        ])
    )

    gas = s("gas_prices")
    hh = gas.filter("benchmark = 'henry_hub'").select(
        F.col("obs_date").alias("date"), F.col("price_usd_mmbtu").alias("henry_hub_usd_mmbtu"))
    eug = gas.filter("benchmark = 'eu_gas'").select(
        F.col("obs_date").alias("month_start"), F.col("price_usd_mmbtu").alias("eu_gas_usd_mmbtu"))

    base = (
        g("dim_date").select("date", "is_weekend", "expected_settlement_periods")
        .join(gb, "date", "inner")
        .join(eu, "date", "left")
        .join(hh, "date", "left")
        .join(g("dim_fx_daily"), "date", "left")
        .withColumn("month_start", F.trunc("date", "month"))
        .join(eug, "month_start", "left")
        .drop("month_start")
    )
    return (
        base.withColumn("henry_hub_usd_mmbtu", _ffill("henry_hub_usd_mmbtu"))
        .withColumn("gb_fr_spread_gbp_mwh", F.col("gb_avg_price_gbp_mwh") - F.col("fr_avg_price_gbp_mwh"))
        .withColumn("is_complete_day", F.col("settlement_periods_loaded") == F.col("expected_settlement_periods"))
    )


@dlt.table(name=f"{GOLD}.fact_stock_returns_daily", comment="Daily returns per ticker.")
def fact_stock_returns_daily():
    px = s("stock_prices_daily")
    w = Window.partitionBy("ticker").orderBy("trade_date")
    price = F.coalesce("adj_close_gbp", "close_gbp")
    return (
        px.withColumn("_px", price)
        .withColumn("_prev", F.lag("_px").over(w))
        .select(
            "ticker", "trade_date", "close_gbp", "adj_close_gbp", "volume",
            (F.col("_px") / F.col("_prev") - 1).alias("daily_return"),
            F.log(F.col("_px") / F.col("_prev")).alias("log_return"),
        )
        .join(g("dim_company"), "ticker", "left")
    )


@dlt.table(name=f"{GOLD}.mart_stocks_vs_market",
           comment="Trading days only: each ticker's return next to power and gas moves, plus "
                   "rolling 60-trading-day correlations. This is the table the dashboard asks about.")
def mart_stocks_vs_market():
    mkt = g("fact_market_daily")
    wm = Window.orderBy("date")
    mkt = mkt.select(
        "date", "gb_avg_price_gbp_mwh", "gb_price_volatility", "avg_wind_share",
        "henry_hub_usd_mmbtu", "eu_gas_usd_mmbtu", "gb_fr_spread_gbp_mwh",
        (F.col("gb_avg_price_gbp_mwh") / F.lag("gb_avg_price_gbp_mwh").over(wm) - 1)
        .alias("gb_price_change"),
        (F.col("henry_hub_usd_mmbtu") / F.lag("henry_hub_usd_mmbtu").over(wm) - 1)
        .alias("henry_hub_change"),
    )
    r = g("fact_stock_returns_daily").withColumnRenamed("trade_date", "date")
    joined = r.join(mkt, "date", "inner")
    w60 = Window.partitionBy("ticker").orderBy("date").rowsBetween(-59, 0)
    return (
        joined.withColumn("corr60_return_vs_gb_power", F.corr("daily_return", "gb_price_change").over(w60))
        .withColumn("corr60_return_vs_henry_hub", F.corr("daily_return", "henry_hub_change").over(w60))
        .withColumn("obs_in_window", F.count("daily_return").over(w60))
    )


# --- Data quality --------------------------------------------------------------------

@dlt.table(name=f"{GOLD}.dq_settlement_period_completeness",
           comment="Days where loaded GB settlement periods don't match the expected 46/48/50.")
def dq_settlement_period_completeness():
    loaded = s("gb_system_prices").groupBy(F.col("settlement_date").alias("date")).agg(
        F.countDistinct("settlement_period").alias("periods_loaded"))
    return (
        g("dim_date").filter(F.col("date") < F.current_date())
        .join(loaded, "date", "inner")
        .filter(F.col("periods_loaded") != F.col("expected_settlement_periods"))
        .select("date", "expected_settlement_periods", "periods_loaded")
    )
