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


def autoload(subpath: str, fmt: str, **options):
    reader = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("recursiveFileLookup", "true")
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


@dlt.table(
    name=f"{BRONZE}.entsoe_day_ahead",
    comment="ENTSO-E day-ahead prices, parsed from A44 XML at landing (one row per slot). "
            "Untouched XML is archived under landing/entsoe/day_ahead_xml.",
)
def entsoe_day_ahead():
    return autoload("entsoe/day_ahead", "json", pathGlobFilter="*.jsonl")


@dlt.table(
    name=f"{BRONZE}.fred_observations",
    comment="Raw FRED observation responses, one row per series per landed file.",
)
def fred_observations():
    return autoload("fred/observations", "json", multiLine="true", pathGlobFilter="*.json")


@dlt.table(
    name=f"{BRONZE}.stock_daily_prices",
    comment="Daily OHLCV for London-listed energy companies (yfinance), as landed.",
)
def stock_daily_prices():
    return autoload("stocks/daily_prices", "json", pathGlobFilter="*.jsonl")


@dlt.table(
    name=f"{BRONZE}.era5_gb_hourly",
    comment="ERA5 hourly reanalysis over GB, flattened from NetCDF to Parquet at landing.",
)
def era5_gb_hourly():
    return autoload("era5/gb_hourly", "parquet", pathGlobFilter="*.parquet")
