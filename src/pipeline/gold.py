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
    """Current rows of a silver table. Merged silver tables are SCD type 2 and keep every
    revision; only rows with __END_AT NULL are current."""
    df = spark.read.table(f"{SILVER}.{name}")  # noqa: F821
    return df.filter("__END_AT IS NULL") if "__END_AT" in df.columns else df


def g(name):
    return spark.read.table(f"{GOLD}.{name}")  # noqa: F821


def _ffill(col):
    w = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    return F.last(col, ignorenulls=True).over(w)


# --- Dimensions ------------------------------------------------------------------------

# Skip-when-unchanged: gold views refresh as NO_OP only if their inputs haven't changed and
# their plans are deterministic. A view built on current_date() or an in-memory list is
# rewritten on every run, and that rewrite then looks like "changed data" to everything
# downstream. So the dimensions below avoid both.

@dlt.table(name=f"{GOLD}.dim_date",
           comment="Calendar 2020-2035 with the expected number of GB settlement periods per day.")
def dim_date():
    # A fixed range, not current_date(): deterministic, so this never needs recomputing.
    # Facts inner-join to dates that have data, so the future dates are harmless.
    days = spark.sql(  # noqa: F821
        "SELECT explode(sequence(DATE'2020-01-01', DATE'2035-12-31', INTERVAL 1 DAY)) AS date"
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
    # SQL VALUES rather than createDataFrame: an in-memory DataFrame isn't a Delta input,
    # so it was recomputed (and rewritten) on every run.
    return spark.sql(  # noqa: F821
        """
        SELECT * FROM VALUES
            ('SSE.L',  'SSE',           'Generation & networks'),
            ('CNA.L',  'Centrica',      'Supply & trading'),
            ('NG.L',   'National Grid', 'Networks'),
            ('DRX.L',  'Drax',          'Generation (biomass)'),
            ('BP.L',   'BP',            'Oil & gas'),
            ('SHEL.L', 'Shell',         'Oil & gas')
        AS t(ticker, company, segment)
        """
    )


@dlt.table(name=f"{GOLD}.dim_fx_daily",
           comment="Daily FX with weekends/holidays forward-filled from the last published rate.")
def dim_fx_daily():
    fx = s("fx_rates_daily").withColumnRenamed("fx_date", "date")
    # Forward-fill up to the latest day with power prices (FRED lags by a few days), not
    # to "today", so the view only changes when the data does.
    last_day = (
        s("gb_system_prices").select(F.max("settlement_date").alias("d"))
        .unionByName(s("eu_day_ahead_prices").select(
            F.max(F.to_date(F.from_utc_timestamp("delivery_start_utc", "Europe/Brussels"))).alias("d")))
        .agg(F.max("d").alias("last_day"))
    )
    spine = (g("dim_date").select("date").crossJoin(last_day)
             .filter(F.col("date") <= F.col("last_day")).drop("last_day"))
    joined = spine.join(fx, "date", "left")
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
    demand = s("gb_demand_outturn").select(
        "settlement_date", "settlement_period", "demand_mw", "transmission_demand_mw")
    # APX is the only provider with trades; a period with no volume has no real price.
    apx = s("gb_market_index_prices").filter("data_provider = 'APXMIDP'").select(
        "settlement_date", "settlement_period",
        F.when(F.col("volume_mwh") > 0, F.col("price_gbp_mwh")).alias("market_index_price_gbp_mwh"),
        F.col("volume_mwh").alias("market_index_volume_mwh"),
    )
    # ERA5 values are stamped at the END of their hour, so a half-hour starting at 10:00
    # or 10:30 maps to the ERA5 record valid at 11:00.
    weather = g("gb_weather_hourly")
    df = (
        prices.join(mix, ["settlement_date", "settlement_period"], "left")
        .join(demand, ["settlement_date", "settlement_period"], "left")
        .join(apx, ["settlement_date", "settlement_period"], "left")
        # Demand left for dispatchable plant and imports once wind has run. INDO is already
        # net of embedded wind and solar, so only transmission-connected wind comes off.
        .withColumn("residual_demand_mw", F.col("demand_mw") - F.col("wind_mw"))
        .withColumn("weather_hour_utc", F.date_trunc("hour", "start_time_utc") + F.expr("INTERVAL 1 HOUR"))
        .join(weather.withColumnRenamed("valid_time_utc", "weather_hour_utc"), "weather_hour_utc", "left")
        .withColumn("wind_share", F.col("wind_mw") / F.col("domestic_generation_mw"))
        .withColumn("gas_share", F.col("gas_mw") / F.col("domestic_generation_mw"))
    )
    return df.drop("grid_cells")


def _day_start_utc(date_col):
    """UTC instant of local (Europe/London) midnight at the start of a settlement day."""
    return F.to_utc_timestamp(F.to_timestamp(date_col), "Europe/London")


def _latest_vintage(df, keys, cutoff):
    """Per ``keys``, the newest row published strictly before ``cutoff``."""
    w = Window.partitionBy(*keys).orderBy(F.col("publish_time_utc").desc())
    return (df.filter(F.col("publish_time_utc") < cutoff)
            .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn"))


# Trailing window for the wind forecast correction: days D-365 to D-2. D-1's metered wind
# isn't complete when the day-ahead forecast for D is made, so it stays out.
WIND_SCALE_WINDOW_DAYS = (365, 2)
# No correction until this many half-hours of history (about 90 days) are in the window.
WIND_SCALE_MIN_PERIODS = 48 * 90


def _wind_forecast_scale(fc):
    """Per settlement date, the least-squares factor k minimising sum((metered - k * WINDFOR)^2)
    over the trailing window. WINDFOR overshoots metered wind by about 15%, most in strong
    winds (curtailment); scaling it by k cut the 2026 error from 1.7 to 1.0 GW."""
    actual = g("fact_gb_half_hourly").select("settlement_date", "settlement_period", "wind_mw")
    daily = (
        fc.join(actual, ["settlement_date", "settlement_period"])
        .filter("wind_mw IS NOT NULL AND wind_forecast_mw IS NOT NULL")
        .groupBy("settlement_date")
        .agg(F.sum(F.col("wind_mw") * F.col("wind_forecast_mw")).alias("_sxy"),
             F.sum(F.col("wind_forecast_mw") ** 2).alias("_sxx"),
             F.count("*").alias("_n"))
    )
    # Every forecast date, including days ahead that have no metered wind yet.
    back, gap = WIND_SCALE_WINDOW_DAYS
    w = Window.orderBy("_day").rangeBetween(-back, -gap)
    return (
        fc.select("settlement_date").distinct()
        .join(daily, "settlement_date", "left")
        .withColumn("_day", F.datediff("settlement_date", F.lit("2000-01-01")))
        .select(
            "settlement_date",
            F.when(F.sum("_n").over(w) >= WIND_SCALE_MIN_PERIODS,
                   F.sum("_sxy").over(w) / F.sum("_sxx").over(w)).alias("wind_forecast_scale"),
        )
    )


@dlt.table(name=f"{GOLD}.fact_gb_day_ahead_forecasts",
           comment="One row per GB settlement period: demand, wind and solar forecasts as known "
                   "before the delivery day began (latest vintage published before local "
                   "midnight), so they can be used as model features without look-ahead. "
                   "The *_adjusted_mw columns correct WINDFOR's overshoot with a factor fitted "
                   "on days D-365 to D-2.")
def fact_gb_day_ahead_forecasts():
    # Cutoff = start of the delivery day. Everything here was public the day before. A model
    # of the 09:20 day-ahead auction needs an earlier cutoff: filter the silver vintages
    # directly (the publish times below show how old each forecast was).
    period = ["settlement_date", "settlement_period"]

    ndf = s("gb_demand_forecast")
    ndf = _latest_vintage(ndf, period, _day_start_utc("settlement_date")).select(
        *period, "start_time_utc",
        F.col("demand_mw").alias("demand_forecast_mw"),
        F.col("publish_time_utc").alias("demand_forecast_published_utc"),
    )

    # WINDFOR is hourly with no settlement date: date each hour by its London day.
    wind = s("gb_wind_forecast").withColumn(
        "_day", F.to_date(F.from_utc_timestamp("start_time_utc", "Europe/London")))
    wind = _latest_vintage(wind, ["start_time_utc"], _day_start_utc("_day")).select(
        F.col("start_time_utc").alias("_wind_hour_utc"),
        F.col("wind_mw").alias("wind_forecast_mw"),
        F.col("publish_time_utc").alias("wind_forecast_published_utc"),
    )

    # DGWS day-ahead run (published ~16:45 UK the day before), one row per technology.
    dgws = s("gb_wind_solar_forecast").filter("process_type = 'Day ahead'")
    dgws = _latest_vintage(dgws, [*period, "psr_type"], _day_start_utc("settlement_date"))
    psr = F.col("psr_type")
    dgws = dgws.groupBy(*period).agg(
        F.first(F.when(psr == "Solar", F.col("forecast_mw")), ignorenulls=True)
        .alias("solar_forecast_mw"),
        F.first(F.when(psr == "Wind Onshore", F.col("forecast_mw")), ignorenulls=True)
        .alias("wind_onshore_forecast_mw"),
        F.first(F.when(psr == "Wind Offshore", F.col("forecast_mw")), ignorenulls=True)
        .alias("wind_offshore_forecast_mw"),
        F.max("publish_time_utc").alias("wind_solar_forecast_published_utc"),
    )

    fc = (
        ndf.withColumn("_wind_hour_utc", F.date_trunc("hour", "start_time_utc"))
        .join(wind, "_wind_hour_utc", "left")
        .join(dgws, period, "left")
        .drop("_wind_hour_utc")
        # Same definition as fact_gb_half_hourly.residual_demand_mw, from forecasts: NDF is
        # net of embedded generation, WINDFOR covers transmission-connected wind.
        .withColumn("residual_demand_forecast_mw",
                    F.col("demand_forecast_mw") - F.col("wind_forecast_mw"))
    )
    adjusted = F.col("wind_forecast_mw") * F.col("wind_forecast_scale")
    return (
        fc.join(_wind_forecast_scale(fc), "settlement_date", "left")
        .withColumn("wind_forecast_adjusted_mw", adjusted)
        .withColumn("residual_demand_forecast_adjusted_mw", F.col("demand_forecast_mw") - adjusted)
    )


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
        (F.sum(F.col("market_index_price_gbp_mwh") * F.col("market_index_volume_mwh"))
         / F.sum(F.when(F.col("market_index_price_gbp_mwh").isNotNull(),
                        F.col("market_index_volume_mwh"))))
        .alias("gb_market_index_price_gbp_mwh"),               # volume-weighted
        F.avg("demand_mw").alias("gb_avg_demand_mw"),
        F.max("demand_mw").alias("gb_peak_demand_mw"),
        F.avg("residual_demand_mw").alias("gb_avg_residual_demand_mw"),
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

    sap = s("gb_gas_sap_daily").select(
        F.col("gas_day").alias("date"), F.col("sap_gbp_mwh").alias("gb_gas_sap_gbp_mwh"))
    ttf = s("ttf_gas_daily").select(
        F.col("trade_date").alias("date"), F.col("close_eur_mwh").alias("ttf_eur_mwh"))

    base = (
        g("dim_date").select("date", "is_weekend", "expected_settlement_periods")
        .join(gb, "date", "inner")
        .join(eu, "date", "left")
        .join(hh, "date", "left")
        .join(sap, "date", "left")
        .join(ttf, "date", "left")
        .join(g("dim_fx_daily"), "date", "left")
        .withColumn("month_start", F.trunc("date", "month"))
        .join(eug, "month_start", "left")
        .drop("month_start")
    )
    return (
        base.withColumn("henry_hub_usd_mmbtu", _ffill("henry_hub_usd_mmbtu"))
        # TTF trades on weekdays only: weekends and holidays carry the last close.
        .withColumn("ttf_eur_mwh", _ffill("ttf_eur_mwh"))
        .withColumn("ttf_gbp_mwh", F.col("ttf_eur_mwh") * F.col("gbp_per_eur"))
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
    # No current_date() filter (it made this view recompute every run). Scheduled ingests
    # stop at yesterday, so a partial "today" only appears if a backfill ends on today, and
    # then flagging it as incomplete is accurate.
    return (
        g("dim_date")
        .join(loaded, "date", "inner")
        .filter(F.col("periods_loaded") != F.col("expected_settlement_periods"))
        .select("date", "expected_settlement_periods", "periods_loaded")
    )
