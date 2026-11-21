# Lakeflow Declarative Pipeline: BRONZE layer
# ---------------------------------------------------------------------------------------
# Append-only streaming tables fed by Auto Loader from the landing Volume. We keep payloads
# exactly as landed and add lineage columns. All cleaning happens in silver.
#
# Schema names come from pipeline configuration (set in resources/pipeline.yml), because
# bundle development mode prefixes schema names (e.g. dev_yourname_bronze).

import dlt
from pyspark.sql import functions as F

LANDING = spark.conf.get("energy.landing_root")  # noqa: F821 - `spark` is injected
BRONZE = spark.conf.get("energy.bronze_schema")  # noqa: F821


# Explicit schemas for the files *we* write (parsed ENTSO-E rows, stocks, ERA5 Parquet) and
# for FRED. With a schema, Auto Loader can start on an empty folder, so a source whose key
# isn't set up yet gives an empty table instead of failing the whole pipeline. Unexpected
# fields go to _rescued_data rather than being lost.
ENTSOE_SCHEMA = """
    zone STRING, zone_eic STRING, delivery_start_utc STRING, delivery_end_utc STRING,
    resolution_minutes BIGINT, position BIGINT, price DOUBLE, is_forward_filled BOOLEAN,
    currency STRING, price_unit STRING, curve_type STRING, classification_sequence BIGINT,
    series_mrid STRING, document_mrid STRING
"""
FRED_SCHEMA = """
    series_id STRING, realtime_start STRING, realtime_end STRING,
    observation_start STRING, observation_end STRING, units STRING, output_type BIGINT,
    file_type STRING, order_by STRING, sort_order STRING, count BIGINT, offset BIGINT,
    limit BIGINT,
    observations ARRAY<STRUCT<realtime_start: STRING, realtime_end: STRING,
                              date: STRING, value: STRING>>
"""
STOCKS_SCHEMA = """
    ticker STRING, trade_date STRING, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
    adj_close DOUBLE, volume DOUBLE, dividends DOUBLE, stock_splits DOUBLE,
    quote_currency STRING
"""
# Elexon responses are {"data": [...]}. The first two Elexon tables predate explicit schemas
# and infer theirs; these name the fields we use (numbers as DOUBLE, times as STRING, typed
# in silver). Other top-level keys, e.g. demand outturn's "metadata", go to _rescued_data.
ELEXON_DEMAND_OUTTURN_SCHEMA = """
    data ARRAY<STRUCT<publishTime: STRING, startTime: STRING, settlementDate: STRING,
                      settlementPeriod: BIGINT, initialDemandOutturn: DOUBLE,
                      initialTransmissionSystemDemandOutturn: DOUBLE>>
"""
ELEXON_MARKET_INDEX_SCHEMA = """
    data ARRAY<STRUCT<dataset: STRING, startTime: STRING, dataProvider: STRING,
                      settlementDate: STRING, settlementPeriod: BIGINT, price: DOUBLE,
                      volume: DOUBLE>>
"""
ELEXON_DEMAND_FORECAST_SCHEMA = """
    data ARRAY<STRUCT<dataset: STRING, publishTime: STRING, startTime: STRING,
                      settlementDate: STRING, settlementPeriod: BIGINT, boundary: STRING,
                      demand: DOUBLE>>
"""
ELEXON_WIND_FORECAST_SCHEMA = """
    data ARRAY<STRUCT<dataset: STRING, publishTime: STRING, startTime: STRING,
                      generation: DOUBLE>>
"""
ELEXON_WIND_SOLAR_FORECAST_SCHEMA = """
    data ARRAY<STRUCT<dataset: STRING, documentId: STRING, documentRevisionNumber: BIGINT,
                      publishTime: STRING, processType: STRING, businessType: STRING,
                      psrType: STRING, quantity: DOUBLE, startTime: STRING,
                      settlementDate: STRING, settlementPeriod: BIGINT>>
"""
# flatten_era5 writes every numeric column as float64, so DOUBLE always matches.
ERA5_SCHEMA = """
    valid_time_utc TIMESTAMP, latitude DOUBLE, longitude DOUBLE, t2m DOUBLE, u10 DOUBLE,
    v10 DOUBLE, u100 DOUBLE, v100 DOUBLE, ssrd DOUBLE
"""


def autoload(subpath: str, fmt: str, schema: str | None = None, **options):
    reader = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", fmt)
        .option("recursiveFileLookup", "true")
    )
    if schema:
        reader = reader.schema(schema).option("cloudFiles.schemaEvolutionMode", "rescue")
    else:
        reader = (
            reader.option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        )
    for k, v in options.items():
        reader = reader.option(k, v)
    return (
        reader.load(f"{LANDING}/{subpath}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dlt.table(
    name=f"{BRONZE}.elexon_system_prices",
    comment="Raw Elexon system price responses, one row per landed file ({data: [...]}).",
)
def elexon_system_prices():
    return autoload("elexon/system_prices", "json", multiLine="true",
                    pathGlobFilter="*.json")


@dlt.table(
    name=f"{BRONZE}.elexon_generation_by_fuel",
    comment="Raw Elexon FUELHH responses, one row per landed file.",
)
def elexon_generation_by_fuel():
    return autoload("elexon/generation_by_fuel", "json", multiLine="true",
                    pathGlobFilter="*.json")


def _elexon_table(dataset, schema, comment):
    @dlt.table(name=f"{BRONZE}.elexon_{dataset}", comment=comment)
    def _table():
        return autoload(f"elexon/{dataset}", "json", schema, multiLine="true",
                        pathGlobFilter="*.json")


_elexon_table("demand_outturn", ELEXON_DEMAND_OUTTURN_SCHEMA,
              "Raw Elexon demand outturn (INDO/ITSDO) responses, one row per landed file.")
_elexon_table("market_index", ELEXON_MARKET_INDEX_SCHEMA,
              "Raw Elexon market index data (MID) responses, one row per landed file.")
_elexon_table("demand_forecast", ELEXON_DEMAND_FORECAST_SCHEMA,
              "Raw Elexon national demand forecasts (NDF), one file per publish day.")
_elexon_table("wind_forecast", ELEXON_WIND_FORECAST_SCHEMA,
              "Raw Elexon wind generation forecasts (WINDFOR), one file per publish day.")
_elexon_table("wind_solar_forecast", ELEXON_WIND_SOLAR_FORECAST_SCHEMA,
              "Raw Elexon wind and solar forecasts (DGWS), one file per publish day.")


@dlt.table(
    name=f"{BRONZE}.entsoe_day_ahead",
    comment="ENTSO-E day-ahead prices, parsed from A44 XML at landing (one row per slot). "
            "Untouched XML is archived under landing/entsoe/day_ahead_xml.",
)
def entsoe_day_ahead():
    return autoload("entsoe/day_ahead", "json", ENTSOE_SCHEMA, pathGlobFilter="*.jsonl")


@dlt.table(
    name=f"{BRONZE}.fred_observations",
    comment="Raw FRED observation responses, one row per series per landed file.",
)
def fred_observations():
    return autoload("fred/observations", "json", FRED_SCHEMA, multiLine="true",
                    pathGlobFilter="*.json")


@dlt.table(
    name=f"{BRONZE}.stock_daily_prices",
    comment="Daily OHLCV for London-listed energy companies (yfinance), as landed.",
)
def stock_daily_prices():
    return autoload("stocks/daily_prices", "json", STOCKS_SCHEMA, pathGlobFilter="*.jsonl")


@dlt.table(
    name=f"{BRONZE}.era5_gb_hourly",
    comment="ERA5 hourly reanalysis over GB, flattened from NetCDF to Parquet at landing.",
)
def era5_gb_hourly():
    return autoload("era5/gb_hourly", "parquet", ERA5_SCHEMA, pathGlobFilter="*.parquet")
