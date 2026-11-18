# Lakeflow Declarative Pipeline: SILVER layer
# ---------------------------------------------------------------------------------------
# Clean, typed, deduplicated tables, all with UTC timestamps.
#
# Incremental upserts with AUTO CDC (Lakeflow's managed MERGE):
#   bronze (append-only) --stream--> typed source view --AUTO CDC--> silver streaming table
# Each update reads only bronze rows that arrived since the last run, then merges them on
# the business key. A row is written only when its sequence value is newer than what silver
# already holds, so stale re-deliveries and out-of-order files are ignored, and settlement
# revisions overwrite earlier values. Re-landing an identical record leaves the values as
# they were.
#
# Expectations sit on the source views, so bad rows are dropped before they reach the merge.
# "expect" warns, "expect_or_drop" removes the row.

import dlt
from pyspark.sql import functions as F

BRONZE = spark.conf.get("energy.bronze_schema")  # noqa: F821
SILVER = spark.conf.get("energy.silver_schema")  # noqa: F821

# Newest landed file wins. The file name carries the run timestamp, so it breaks ties
# between files with the same modification time.
LANDED_ORDER = F.struct("_file_modified_at", "_source_file")


def bronze_stream(table):
    return spark.readStream.table(f"{BRONZE}.{table}")  # noqa: F821


def upsert(target, source, keys, sequence_by=LANDED_ORDER, comment=None, cluster_by=None):
    """Silver streaming table kept current (SCD type 1) from ``source`` by AUTO CDC."""
    dlt.create_streaming_table(name=f"{SILVER}.{target}", comment=comment, cluster_by=cluster_by)
    dlt.create_auto_cdc_flow(
        target=f"{SILVER}.{target}",
        source=source,
        keys=keys,
        sequence_by=sequence_by,
        stored_as_scd_type=1,
    )


def settlement_start_utc(date_col, period_col):
    """UTC start of a GB settlement period, derived from first principles:
    local midnight (Europe/London) converted to UTC, plus (period - 1) * 30 minutes.
    This handles 46/50-period clock-change days without any special cases."""
    return F.to_utc_timestamp(F.to_timestamp(date_col), "Europe/London") + F.make_dt_interval(
        F.lit(0), F.lit(0), (period_col - 1) * 30
    )


# --- GB (Elexon) ------------------------------------------------------------------------

@dlt.view(name="gb_system_prices_updates")
@dlt.expect_or_drop("valid_period", "settlement_period BETWEEN 1 AND 50")
@dlt.expect_or_drop("price_present", "system_price_gbp_mwh IS NOT NULL")
@dlt.expect("plausible_price", "system_price_gbp_mwh BETWEEN -1000 AND 10000")
@dlt.expect("start_matches_api", "start_time_utc = api_start_time_utc")
def gb_system_prices_updates():
    rows = bronze_stream("elexon_system_prices").select(
        F.explode("data").alias("r"), "_file_modified_at", "_source_file")
    return rows.select(
        F.to_date("r.settlementDate").alias("settlement_date"),
        F.col("r.settlementPeriod").cast("int").alias("settlement_period"),
        F.col("r.startTime").cast("timestamp").alias("api_start_time_utc"),
        F.col("r.systemSellPrice").cast("double").alias("system_price_gbp_mwh"),
        F.col("r.systemBuyPrice").cast("double").alias("system_buy_price_gbp_mwh"),
        F.col("r.netImbalanceVolume").cast("double").alias("net_imbalance_volume_mwh"),
        "_file_modified_at",
        "_source_file",
    ).withColumn("start_time_utc", settlement_start_utc("settlement_date", F.col("settlement_period")))


upsert("gb_system_prices", "gb_system_prices_updates",
       keys=["settlement_date", "settlement_period"],
       comment="GB system (imbalance) price per settlement period, latest settlement run.")


@dlt.view(name="gb_generation_by_fuel_updates")
@dlt.expect_or_drop("valid_period", "settlement_period BETWEEN 1 AND 50")
@dlt.expect("non_negative_generation", "generation_mw >= 0 OR fuel_type LIKE 'INT%'")
def gb_generation_by_fuel_updates():
    rows = bronze_stream("elexon_generation_by_fuel").select(
        F.explode("data").alias("r"), "_file_modified_at", "_source_file")
    return rows.select(
        F.to_date("r.settlementDate").alias("settlement_date"),
        F.col("r.settlementPeriod").cast("int").alias("settlement_period"),
        F.upper("r.fuelType").alias("fuel_type"),
        F.col("r.generation").cast("double").alias("generation_mw"),
        F.col("r.publishTime").cast("timestamp").alias("publish_time_utc"),
        "_file_modified_at",
        "_source_file",
    ).withColumn("start_time_utc", settlement_start_utc("settlement_date", F.col("settlement_period")))


# Elexon's publish time says which version is newest; landing order only breaks ties.
upsert("gb_generation_by_fuel", "gb_generation_by_fuel_updates",
       keys=["settlement_date", "settlement_period", "fuel_type"],
       sequence_by=F.struct("publish_time_utc", "_file_modified_at", "_source_file"),
       comment="GB half-hourly generation outturn (MW) by fuel type, incl. interconnectors.")


# --- Europe (ENTSO-E) ------------------------------------------------------------------

@dlt.view(name="eu_day_ahead_prices_updates")
@dlt.expect_or_drop("price_present", "price_eur_mwh IS NOT NULL")
@dlt.expect("currency_is_eur", "currency = 'EUR'")
@dlt.expect("plausible_price", "price_eur_mwh BETWEEN -500 AND 4000")
def eu_day_ahead_prices_updates():
    return bronze_stream("entsoe_day_ahead").select(
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
        "_source_file",
    )


upsert("eu_day_ahead_prices", "eu_day_ahead_prices_updates",
       keys=["zone", "delivery_start_utc", "resolution_minutes", "classification_sequence"],
       comment="Day-ahead prices (EUR/MWh) for GB's interconnected neighbours, native resolution.")


# --- Macro (FRED) ----------------------------------------------------------------------

@dlt.view(name="fred_observations_updates")
def fred_observations_updates():
    rows = bronze_stream("fred_observations").select(
        "series_id", F.explode("observations").alias("o"), "_file_modified_at", "_source_file")
    return rows.select(
        "series_id",
        F.to_date("o.date").alias("obs_date"),
        # FRED uses "." for missing; try_cast turns it into NULL instead of failing.
        F.expr("try_cast(o.value AS DOUBLE)").alias("value"),
        "_file_modified_at",
        "_source_file",
    )


upsert("fred_observations", "fred_observations_updates",
       keys=["series_id", "obs_date"],
       comment="Latest value of every FRED observation (gas prices and FX), one row per day.")


# Small reshapes of fred_observations. Materialized views, refreshed from the merged table.
def _fred_series():
    return spark.read.table(f"{SILVER}.fred_observations")  # noqa: F821


@dlt.table(name=f"{SILVER}.fx_rates_daily",
           comment="Daily FX from FRED: USD per GBP, USD per EUR, and derived GBP per EUR.")
@dlt.expect("gbp_per_eur_plausible", "gbp_per_eur IS NULL OR gbp_per_eur BETWEEN 0.5 AND 1.5")
def fx_rates_daily():
    s = _fred_series().filter(F.col("series_id").isin("DEXUSUK", "DEXUSEU"))
    # Conditional aggregation instead of pivot (unsupported in Lakeflow pipelines).
    wide = s.groupBy("obs_date").agg(*[
        F.first(F.when(F.col("series_id") == sid, F.col("value")), ignorenulls=True).alias(sid)
        for sid in ("DEXUSUK", "DEXUSEU")
    ])
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

@dlt.view(name="stock_prices_daily_updates")
@dlt.expect_or_drop("close_present", "close_gbp IS NOT NULL")
@dlt.expect("positive_close", "close_gbp > 0")
def stock_prices_daily_updates():
    raw = bronze_stream("stock_daily_prices")
    pence = F.col("quote_currency") == "GBp"

    def to_gbp(c):
        return F.when(pence, F.col(c) / 100.0).otherwise(F.col(c))

    return raw.select(
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
        "_source_file",
    )


upsert("stock_prices_daily", "stock_prices_daily_updates",
       keys=["ticker", "trade_date"],
       comment="Daily share prices in GBP (London quotes converted from pence).")


# --- Weather (ERA5) --------------------------------------------------------------------

@dlt.view(name="era5_gb_grid_hourly_updates")
@dlt.expect_or_drop("in_gb_box", "latitude BETWEEN 49.5 AND 61 AND longitude BETWEEN -8 AND 2")
@dlt.expect("plausible_temp", "temp_c BETWEEN -40 AND 45")
def era5_gb_grid_hourly_updates():
    return bronze_stream("era5_gb_hourly").select(
        F.col("valid_time_utc").cast("timestamp").alias("valid_time_utc"),
        F.round("latitude", 2).alias("latitude"),
        F.round("longitude", 2).alias("longitude"),
        (F.col("t2m") - 273.15).alias("temp_c"),
        F.sqrt(F.col("u10") ** 2 + F.col("v10") ** 2).alias("wind_speed_10m_ms"),
        F.sqrt(F.col("u100") ** 2 + F.col("v100") ** 2).alias("wind_speed_100m_ms"),
        # ssrd: J/m^2 accumulated over the hour ending at valid_time -> mean W/m^2
        (F.col("ssrd") / 3600.0).alias("solar_irradiance_wm2"),
        "_file_modified_at",
        "_source_file",
    )


upsert("era5_gb_grid_hourly", "era5_gb_grid_hourly_updates",
       keys=["valid_time_utc", "latitude", "longitude"],
       comment="ERA5 hourly weather per 0.25-degree grid cell over GB, in friendly units.",
       cluster_by=["valid_time_utc"])
