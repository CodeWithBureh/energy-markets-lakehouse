# Lakeflow Declarative Pipeline: SILVER layer
# ---------------------------------------------------------------------------------------
# Clean, typed, deduplicated tables, all with UTC timestamps.
# Because bronze is append-only, every silver table keeps only the *latest landed*
# version of each business key. That is how settlement revisions overwrite earlier values.
# Expectations log data-quality metrics on every update. "expect" warns,
# "expect_or_drop" removes the row.

import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F

BRONZE = spark.conf.get("energy.bronze_schema")  # noqa: F821
SILVER = spark.conf.get("energy.silver_schema")  # noqa: F821


def latest(df, keys, order_col="_file_modified_at"):
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def settlement_start_utc(date_col, period_col):
    """UTC start of a GB settlement period, derived from first principles:
    local midnight (Europe/London) converted to UTC, plus (period - 1) * 30 minutes.
    This handles 46/50-period clock-change days without any special cases."""
    return F.to_utc_timestamp(F.to_timestamp(date_col), "Europe/London") + F.make_dt_interval(
        F.lit(0), F.lit(0), (period_col - 1) * 30
    )


# --- GB (Elexon) ------------------------------------------------------------------------

@dlt.table(name=f"{SILVER}.gb_system_prices",
           comment="GB system (imbalance) price per settlement period, latest settlement run.")
@dlt.expect_or_drop("valid_period", "settlement_period BETWEEN 1 AND 50")
@dlt.expect_or_drop("price_present", "system_price_gbp_mwh IS NOT NULL")
@dlt.expect("plausible_price", "system_price_gbp_mwh BETWEEN -1000 AND 10000")
@dlt.expect("start_matches_api", "start_time_utc = api_start_time_utc")
def gb_system_prices():
    raw = spark.read.table(f"{BRONZE}.elexon_system_prices")  # noqa: F821
    rows = raw.select(F.explode("data").alias("r"), "_file_modified_at", "_source_file")
    df = rows.select(
        F.to_date("r.settlementDate").alias("settlement_date"),
        F.col("r.settlementPeriod").cast("int").alias("settlement_period"),
        F.col("r.startTime").cast("timestamp").alias("api_start_time_utc"),
        F.col("r.systemSellPrice").cast("double").alias("system_price_gbp_mwh"),
        F.col("r.systemBuyPrice").cast("double").alias("system_buy_price_gbp_mwh"),
        F.col("r.netImbalanceVolume").cast("double").alias("net_imbalance_volume_mwh"),
        "_file_modified_at",
        "_source_file",
    ).withColumn("start_time_utc", settlement_start_utc("settlement_date", F.col("settlement_period")))
    return latest(df, ["settlement_date", "settlement_period"])


@dlt.table(name=f"{SILVER}.gb_generation_by_fuel",
           comment="GB half-hourly generation outturn (MW) by fuel type, incl. interconnectors.")
@dlt.expect_or_drop("valid_period", "settlement_period BETWEEN 1 AND 50")
@dlt.expect("non_negative_generation", "generation_mw >= 0 OR fuel_type LIKE 'INT%'")
def gb_generation_by_fuel():
    raw = spark.read.table(f"{BRONZE}.elexon_generation_by_fuel")  # noqa: F821
    rows = raw.select(F.explode("data").alias("r"), "_file_modified_at")
    df = rows.select(
        F.to_date("r.settlementDate").alias("settlement_date"),
        F.col("r.settlementPeriod").cast("int").alias("settlement_period"),
        F.upper("r.fuelType").alias("fuel_type"),
        F.col("r.generation").cast("double").alias("generation_mw"),
        F.col("r.publishTime").cast("timestamp").alias("publish_time_utc"),
        "_file_modified_at",
    ).withColumn("start_time_utc", settlement_start_utc("settlement_date", F.col("settlement_period")))
    return latest(df, ["settlement_date", "settlement_period", "fuel_type"], "publish_time_utc")


# --- Europe (ENTSO-E) ------------------------------------------------------------------

@dlt.table(name=f"{SILVER}.eu_day_ahead_prices",
           comment="Day-ahead prices (EUR/MWh) for GB's interconnected neighbours, native resolution.")
@dlt.expect_or_drop("price_present", "price_eur_mwh IS NOT NULL")
@dlt.expect("currency_is_eur", "currency = 'EUR'")
@dlt.expect("plausible_price", "price_eur_mwh BETWEEN -500 AND 4000")
def eu_day_ahead_prices():
    raw = spark.read.table(f"{BRONZE}.entsoe_day_ahead")  # noqa: F821
    df = raw.select(
        "zone",
        "zone_eic",
        F.col("delivery_start_utc").cast("timestamp").alias("delivery_start_utc"),
        F.col("delivery_end_utc").cast("timestamp").alias("delivery_end_utc"),
        F.col("resolution_minutes").cast("int").alias("resolution_minutes"),
        F.col("price").cast("double").alias("price_eur_mwh"),
        "is_forward_filled",
        "currency",
        "curve_type",
        F.coalesce(F.col("classification_sequence").cast("int"), F.lit(1)).alias("classification_sequence"),
        "_file_modified_at",
    )
    return latest(df, ["zone", "delivery_start_utc", "resolution_minutes", "classification_sequence"])


# --- Macro (FRED) ----------------------------------------------------------------------

def _fred_series():
    raw = spark.read.table(f"{BRONZE}.fred_observations")  # noqa: F821
    rows = raw.select("series_id", F.explode("observations").alias("o"), "_file_modified_at")
    df = rows.select(
        "series_id",
        F.to_date("o.date").alias("obs_date"),
        # FRED uses "." for missing; try_cast turns it into NULL instead of failing.
        F.expr("try_cast(o.value AS DOUBLE)").alias("value"),
        "_file_modified_at",
    )
    return latest(df, ["series_id", "obs_date"])


@dlt.table(name=f"{SILVER}.fx_rates_daily",
           comment="Daily FX from FRED: USD per GBP, USD per EUR, and derived GBP per EUR.")
@dlt.expect("gbp_per_eur_plausible", "gbp_per_eur IS NULL OR gbp_per_eur BETWEEN 0.5 AND 1.5")
def fx_rates_daily():
    s = _fred_series().filter(F.col("series_id").isin("DEXUSUK", "DEXUSEU"))
    wide = s.groupBy("obs_date").pivot("series_id", ["DEXUSUK", "DEXUSEU"]).agg(F.first("value"))
    return wide.select(
        F.col("obs_date").alias("fx_date"),
        F.col("DEXUSUK").alias("usd_per_gbp"),
        F.col("DEXUSEU").alias("usd_per_eur"),
        (F.col("DEXUSEU") / F.col("DEXUSUK")).alias("gbp_per_eur"),
    )


@dlt.table(name=f"{SILVER}.gas_prices",
           comment="Natural gas benchmarks from FRED (USD/MMBtu). Henry Hub daily, EU monthly.")
@dlt.expect_or_drop("value_present", "price_usd_mmbtu IS NOT NULL")
def gas_prices():
    s = _fred_series().filter(F.col("series_id").isin("DHHNGSP", "PNGASEUUSDM"))
    return s.select(
        F.when(F.col("series_id") == "DHHNGSP", "henry_hub").otherwise("eu_gas").alias("benchmark"),
        F.when(F.col("series_id") == "DHHNGSP", "daily").otherwise("monthly").alias("frequency"),
        "obs_date",
        F.col("value").alias("price_usd_mmbtu"),
    )


# --- Equities --------------------------------------------------------------------------

@dlt.table(name=f"{SILVER}.stock_prices_daily",
           comment="Daily share prices in GBP (London quotes converted from pence).")
@dlt.expect_or_drop("close_present", "close_gbp IS NOT NULL")
@dlt.expect("positive_close", "close_gbp > 0")
def stock_prices_daily():
    raw = spark.read.table(f"{BRONZE}.stock_daily_prices")  # noqa: F821
    pence = F.col("quote_currency") == "GBp"

    def to_gbp(c):
        return F.when(pence, F.col(c) / 100.0).otherwise(F.col(c))

    df = raw.select(
        "ticker",
        F.to_date("trade_date").alias("trade_date"),
        to_gbp("open").alias("open_gbp"),
        to_gbp("high").alias("high_gbp"),
        to_gbp("low").alias("low_gbp"),
        to_gbp("close").alias("close_gbp"),
        to_gbp("adj_close").alias("adj_close_gbp"),
        F.col("volume").cast("long").alias("volume"),
        F.col("dividends").cast("double").alias("dividend"),
        "_file_modified_at",
    )
    return latest(df, ["ticker", "trade_date"])


# --- Weather (ERA5) --------------------------------------------------------------------

@dlt.table(name=f"{SILVER}.era5_gb_grid_hourly",
           comment="ERA5 hourly weather per 0.25-degree grid cell over GB, in friendly units.",
           cluster_by=["valid_time_utc"])
@dlt.expect_or_drop("in_gb_box", "latitude BETWEEN 49.5 AND 61 AND longitude BETWEEN -8 AND 2")
@dlt.expect("plausible_temp", "temp_c BETWEEN -40 AND 45")
def era5_gb_grid_hourly():
    raw = spark.read.table(f"{BRONZE}.era5_gb_hourly")  # noqa: F821
    df = raw.select(
        F.col("valid_time_utc").cast("timestamp").alias("valid_time_utc"),
        F.round("latitude", 2).alias("latitude"),
        F.round("longitude", 2).alias("longitude"),
        (F.col("t2m") - 273.15).alias("temp_c"),
        F.sqrt(F.col("u10") ** 2 + F.col("v10") ** 2).alias("wind_speed_10m_ms"),
        F.sqrt(F.col("u100") ** 2 + F.col("v100") ** 2).alias("wind_speed_100m_ms"),
        # ssrd: J/m^2 accumulated over the hour ending at valid_time -> mean W/m^2
        (F.col("ssrd") / 3600.0).alias("solar_irradiance_wm2"),
        "_file_modified_at",
    )
    return latest(df, ["valid_time_utc", "latitude", "longitude"])
