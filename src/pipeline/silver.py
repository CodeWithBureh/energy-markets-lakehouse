# Lakeflow Declarative Pipeline: SILVER layer
# ---------------------------------------------------------------------------------------
# Clean, typed, deduplicated tables, all with UTC timestamps.
#
# Incremental, change-only merges with AUTO CDC (Lakeflow's managed MERGE), SCD type 2:
#   bronze (append-only) --stream--> typed source view --AUTO CDC--> silver streaming table
# Each update reads only bronze rows that arrived since the last run and merges them on the
# business key, comparing the *data* columns only:
#   * same values re-landed in a newer file -> nothing is written
#   * any data value changed               -> the old version is closed (__END_AT set) and
#                                             the new one becomes current (__END_AT NULL)
#   * an older/out-of-order file           -> ignored
# Lineage columns (_file_modified_at, _source_file, Elexon publish time) only order the
# versions; they are not compared or stored. __START_AT keeps them for each version: it is
# the sequence struct of the delivery that first brought those values.
#
# Current values: WHERE __END_AT IS NULL (gold's s() helper does this). Full revision
# history, e.g. Elexon re-settlements, stays queryable for "as-of" analysis.
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
LINEAGE_COLUMNS = ["_file_modified_at", "_source_file"]


def bronze_stream(table):
    return spark.readStream.table(f"{BRONZE}.{table}")  # noqa: F821


def current(table):
    """Current version of every key in an SCD type 2 silver table."""
    return spark.read.table(f"{SILVER}.{table}").filter("__END_AT IS NULL")  # noqa: F821


def upsert(target, source, keys, sequence_by=LANDED_ORDER, metadata_columns=(),
           comment=None, cluster_by=None):
    """Silver SCD type 2 streaming table fed from ``source`` by AUTO CDC.

    A new version is written only when a data column changes. Lineage and any
    ``metadata_columns`` order the versions but are neither compared nor stored.
    """
    dlt.create_streaming_table(name=f"{SILVER}.{target}", comment=comment, cluster_by=cluster_by)
    dlt.create_auto_cdc_flow(
        target=f"{SILVER}.{target}",
        source=source,
        keys=keys,
        sequence_by=sequence_by,
        except_column_list=[*LINEAGE_COLUMNS, *metadata_columns],
        stored_as_scd_type=2,
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
# A re-publish with identical generation figures isn't a change, so publish time is
# metadata: it orders versions (and lives on in __START_AT) but isn't compared.
upsert("gb_generation_by_fuel", "gb_generation_by_fuel_updates",
       keys=["settlement_date", "settlement_period", "fuel_type"],
       sequence_by=F.struct("publish_time_utc", "_file_modified_at", "_source_file"),
       metadata_columns=["publish_time_utc"],
       comment="GB half-hourly generation outturn (MW) by fuel type, incl. interconnectors.")


def elexon_rows(table):
    return bronze_stream(table).select(
        F.explode("data").alias("r"), "_file_modified_at", "_source_file")


@dlt.view(name="gb_demand_outturn_updates")
@dlt.expect_or_drop("valid_period", "settlement_period BETWEEN 1 AND 50")
@dlt.expect_or_drop("demand_present", "demand_mw IS NOT NULL")
@dlt.expect("plausible_demand", "demand_mw BETWEEN 5000 AND 70000")
def gb_demand_outturn_updates():
    return elexon_rows("elexon_demand_outturn").select(
        F.to_date("r.settlementDate").alias("settlement_date"),
        F.col("r.settlementPeriod").cast("int").alias("settlement_period"),
        F.col("r.initialDemandOutturn").alias("demand_mw"),
        F.col("r.initialTransmissionSystemDemandOutturn").alias("transmission_demand_mw"),
        F.col("r.publishTime").cast("timestamp").alias("publish_time_utc"),
        "_file_modified_at",
        "_source_file",
    ).withColumn("start_time_utc", settlement_start_utc("settlement_date", F.col("settlement_period")))


upsert("gb_demand_outturn", "gb_demand_outturn_updates",
       keys=["settlement_date", "settlement_period"],
       sequence_by=F.struct("publish_time_utc", "_file_modified_at", "_source_file"),
       metadata_columns=["publish_time_utc"],
       comment="GB demand outturn (MW) per settlement period: national (INDO) and transmission "
               "system (ITSDO). Both are net of embedded wind and solar.")


@dlt.view(name="gb_market_index_prices_updates")
@dlt.expect_or_drop("valid_period", "settlement_period BETWEEN 1 AND 50")
@dlt.expect_or_drop("price_present", "price_gbp_mwh IS NOT NULL")
@dlt.expect("plausible_price", "price_gbp_mwh BETWEEN -1000 AND 10000")
def gb_market_index_prices_updates():
    return elexon_rows("elexon_market_index").select(
        F.to_date("r.settlementDate").alias("settlement_date"),
        F.col("r.settlementPeriod").cast("int").alias("settlement_period"),
        F.col("r.dataProvider").alias("data_provider"),
        F.col("r.price").alias("price_gbp_mwh"),
        F.col("r.volume").alias("volume_mwh"),
        "_file_modified_at",
        "_source_file",
    ).withColumn("start_time_utc", settlement_start_utc("settlement_date", F.col("settlement_period")))


upsert("gb_market_index_prices", "gb_market_index_prices_updates",
       keys=["settlement_date", "settlement_period", "data_provider"],
       comment="GB market index price (GBP/MWh) and traded volume per settlement period and "
               "provider. APXMIDP carries the trades; N2EXMIDP reports zero volume.")


# Forecasts keep every vintage: the publish time is part of the key, so a later forecast
# for the same period is a new row, not a revision. Gold picks the vintage it needs.

@dlt.view(name="gb_demand_forecast_updates")
@dlt.expect_or_drop("valid_period", "settlement_period BETWEEN 1 AND 50")
@dlt.expect_or_drop("demand_present", "demand_mw IS NOT NULL")
@dlt.expect("plausible_demand", "demand_mw BETWEEN 5000 AND 70000")
def gb_demand_forecast_updates():
    return elexon_rows("elexon_demand_forecast").select(
        F.col("r.publishTime").cast("timestamp").alias("publish_time_utc"),
        F.to_date("r.settlementDate").alias("settlement_date"),
        F.col("r.settlementPeriod").cast("int").alias("settlement_period"),
        F.col("r.startTime").cast("timestamp").alias("start_time_utc"),
        F.col("r.demand").alias("demand_mw"),
        F.col("r.boundary").alias("boundary"),
        "_file_modified_at",
        "_source_file",
    )


upsert("gb_demand_forecast", "gb_demand_forecast_updates",
       keys=["publish_time_utc", "settlement_date", "settlement_period"],
       comment="GB national demand forecast (NDF, MW), every published vintage.",
       cluster_by=["settlement_date"])


@dlt.view(name="gb_wind_forecast_updates")
@dlt.expect_or_drop("generation_present", "wind_mw IS NOT NULL")
@dlt.expect("plausible_wind", "wind_mw BETWEEN 0 AND 40000")
def gb_wind_forecast_updates():
    return elexon_rows("elexon_wind_forecast").select(
        F.col("r.publishTime").cast("timestamp").alias("publish_time_utc"),
        F.col("r.startTime").cast("timestamp").alias("start_time_utc"),
        F.col("r.generation").alias("wind_mw"),
        "_file_modified_at",
        "_source_file",
    )


upsert("gb_wind_forecast", "gb_wind_forecast_updates",
       keys=["publish_time_utc", "start_time_utc"],
       comment="Hourly wind generation forecast (WINDFOR, MW) for transmission-connected "
               "wind, every published vintage.",
       cluster_by=["start_time_utc"])


@dlt.view(name="gb_wind_solar_forecast_updates")
@dlt.expect_or_drop("valid_period", "settlement_period BETWEEN 1 AND 50")
@dlt.expect_or_drop("quantity_present", "forecast_mw IS NOT NULL")
@dlt.expect("non_negative", "forecast_mw >= 0")
def gb_wind_solar_forecast_updates():
    return elexon_rows("elexon_wind_solar_forecast").select(
        F.col("r.publishTime").cast("timestamp").alias("publish_time_utc"),
        F.col("r.processType").alias("process_type"),
        F.col("r.psrType").alias("psr_type"),
        F.to_date("r.settlementDate").alias("settlement_date"),
        F.col("r.settlementPeriod").cast("int").alias("settlement_period"),
        F.col("r.startTime").cast("timestamp").alias("start_time_utc"),
        F.col("r.quantity").alias("forecast_mw"),
        F.col("r.documentRevisionNumber").cast("int").alias("document_revision"),
        "_file_modified_at",
        "_source_file",
    )


upsert("gb_wind_solar_forecast", "gb_wind_solar_forecast_updates",
       keys=["publish_time_utc", "process_type", "psr_type", "settlement_date",
             "settlement_period"],
       comment="Half-hourly solar, onshore and offshore wind forecasts (DGWS, MW): day-ahead "
               "and intraday, every published vintage.",
       cluster_by=["settlement_date"])


@dlt.view(name="gb_demand_forecast_daily_updates")
@dlt.expect_or_drop("demand_present", "demand_peak_mw IS NOT NULL")
@dlt.expect("plausible_peak", "demand_peak_mw BETWEEN 15000 AND 70000")
def gb_demand_forecast_daily_updates():
    return elexon_rows("elexon_demand_forecast_daily").select(
        F.col("r.publishTime").cast("timestamp").alias("publish_time_utc"),
        F.to_date("r.forecastDate").alias("forecast_date"),
        F.col("r.demand").alias("demand_peak_mw"),
        "_file_modified_at",
        "_source_file",
    )


upsert("gb_demand_forecast_daily", "gb_demand_forecast_daily_updates",
       keys=["publish_time_utc", "forecast_date"],
       comment="GB national demand forecast 2-14 days ahead (NDFD): forecast daily PEAK "
               "demand (MW), every published vintage.")


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
    return current("fred_observations")


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


@dlt.view(name="gb_gas_sap_daily_updates")
@dlt.expect_or_drop("sap_present", "sap_p_kwh IS NOT NULL")
@dlt.expect("plausible_sap", "sap_p_kwh BETWEEN 0 AND 100")
def gb_gas_sap_daily_updates():
    return bronze_stream("gas_sap").select(
        F.to_date("gas_day").alias("gas_day"),
        "sap_p_kwh",
        # 1 p/kWh = 10 GBP/MWh
        (F.col("sap_p_kwh") * 10).alias("sap_gbp_mwh"),
        "_file_modified_at",
        "_source_file",
    )


upsert("gb_gas_sap_daily", "gb_gas_sap_daily_updates",
       keys=["gas_day"],
       comment="GB System Average Price (SAP) of gas per gas day (05:00-05:00 UK), in p/kWh and "
               "GBP/MWh. Source: National Gas Transmission.")


@dlt.view(name="ttf_gas_daily_updates")
@dlt.expect_or_drop("close_present", "close_eur_mwh IS NOT NULL")
@dlt.expect("plausible_ttf", "close_eur_mwh BETWEEN 0 AND 1000")
def ttf_gas_daily_updates():
    return bronze_stream("gas_ttf").select(
        F.to_date("trade_date").alias("trade_date"),
        F.col("close").alias("close_eur_mwh"),
        F.col("high").alias("high_eur_mwh"),
        F.col("low").alias("low_eur_mwh"),
        "_file_modified_at",
        "_source_file",
    )


upsert("ttf_gas_daily", "ttf_gas_daily_updates",
       keys=["trade_date"],
       comment="Dutch TTF front-month gas future, daily close (EUR/MWh), trading days only.")


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


@dlt.view(name="weather_forecast_gb_hourly_updates")
@dlt.expect_or_drop("wind_present", "wind_speed_100m_ms IS NOT NULL")
@dlt.expect("plausible_wind", "wind_speed_100m_ms BETWEEN 0 AND 80")
def weather_forecast_gb_hourly_updates():
    return bronze_stream("weather_forecast_gb_hourly").select(
        F.col("issued_utc").cast("timestamp").alias("issued_utc"),
        F.col("valid_time_utc").cast("timestamp").alias("valid_time_utc"),
        F.round("latitude", 2).alias("latitude"),
        F.round("longitude", 2).alias("longitude"),
        "wind_speed_100m_ms",
        "temp_c",
        "solar_irradiance_wm2",
        "_file_modified_at",
        "_source_file",
    )


upsert("weather_forecast_gb_hourly", "weather_forecast_gb_hourly_updates",
       keys=["issued_utc", "valid_time_utc", "latitude", "longitude"],
       comment="Open-Meteo hourly weather forecasts at 1-degree points over GB, every daily "
               "fetch (issued_utc = fetch time).",
       cluster_by=["issued_utc"])


upsert("era5_gb_grid_hourly", "era5_gb_grid_hourly_updates",
       keys=["valid_time_utc", "latitude", "longitude"],
       comment="ERA5 hourly weather per 0.25-degree grid cell over GB, in friendly units.",
       cluster_by=["valid_time_utc"])
