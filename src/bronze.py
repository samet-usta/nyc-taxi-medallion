"""Bronze layer: load raw TLC trip files as-is and add ingestion metadata."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from config import (
    BRONZE_TRIPS_DIR,
    BRONZE_ZONES_DIR,
    MONTHS,
    RAW_DIR,
    TAXI_TYPE,
    YEAR,
    ZONE_LOOKUP_FILE,
)
from spark_session import get_spark


# Explicit schema for the zone lookup CSV (no inferSchema)
ZONE_SCHEMA = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True),
])

def build_raw_trip_paths() -> list[str]:
    """Return paths of the raw monthly trip files listed in config."""
    raw_trip_paths = []
    for month in MONTHS:
        file_name = f"{TAXI_TYPE}_tripdata_{YEAR}-{month:02d}.parquet"
        file_path = RAW_DIR / file_name
        raw_trip_paths.append(str(file_path))
    return raw_trip_paths


def check_schemas_match(spark: SparkSession, paths: list[str]) -> None:
    """Fail fast if any monthly file has a different schema than the first one."""
    reference_path = paths[0]
    schema_reference = spark.read.parquet(reference_path).schema

    for path in paths[1:]:
        schema_current = spark.read.parquet(path).schema
        if schema_current != schema_reference:
            raise ValueError(f"Schema mismatch: {path} differs from {reference_path}")

    print(f"[ok] schema check passed for {len(paths)} files")


def read_raw_trips(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Read all raw monthly trip files into a single DataFrame."""
    df_raw_trips = spark.read.parquet(*paths)
    return df_raw_trips


def add_ingestion_metadata(df_raw: DataFrame) -> DataFrame:
    """Add ingestion timestamp and source file name to every row."""
    df_with_metadata = (
        df_raw
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )
    return df_with_metadata


def read_raw_zones(spark: SparkSession) -> DataFrame:
    """Read the taxi zone lookup CSV with an explicit schema."""
    df_raw_zones = (
        spark.read
        .option("header", True)
        .schema(ZONE_SCHEMA)
        .csv(str(ZONE_LOOKUP_FILE))
    )
    return df_raw_zones


def write_parquet(df: DataFrame, target_dir: Path) -> None:
    """Write a DataFrame as Parquet, replacing any previous output."""
    df.write.mode("overwrite").parquet(str(target_dir))
    print(f"[ok] written: {target_dir}")


def check_row_count(
    spark: SparkSession,
    df_source: DataFrame,
    target_dir: Path,
    table_name: str,
) -> None:
    """Fail if the written Parquet has a different row count than its source."""
    source_count = df_source.count()
    written_count = spark.read.parquet(str(target_dir)).count()

    if source_count != written_count:
        raise ValueError(
            f"Row count mismatch for {table_name}: "
            f"source={source_count:,}, bronze={written_count:,}"
        )

    print(f"[ok] {table_name}: {written_count:,} rows (source = bronze)")



def main():
    spark = get_spark("nyc-taxi-bronze")

    # Trips
    raw_trip_paths = build_raw_trip_paths()
    check_schemas_match(spark, raw_trip_paths)
    df_raw_trips = read_raw_trips(spark, raw_trip_paths)
    df_bronze_trips = add_ingestion_metadata(df_raw_trips)
    write_parquet(df_bronze_trips, BRONZE_TRIPS_DIR)

    # Zone lookup (small table, written as a single file)
    df_raw_zones = read_raw_zones(spark)
    df_bronze_zones = add_ingestion_metadata(df_raw_zones)
    write_parquet(df_bronze_zones.coalesce(1), BRONZE_ZONES_DIR)

    # Validation: written row counts must match the sources
    check_row_count(spark, df_raw_trips, BRONZE_TRIPS_DIR, "yellow_trips")
    check_row_count(spark, df_raw_zones, BRONZE_ZONES_DIR, "taxi_zones")

    spark.stop()



if __name__ == "__main__":
    main()