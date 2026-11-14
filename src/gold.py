"""Gold layer: build business-level aggregate tables from silver trips."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import GOLD_DIR, SILVER_TRIPS_DIR
from spark_session import get_spark

DAILY_ZONE_REVENUE_TABLE = "daily_zone_revenue"
HOURLY_DEMAND_TABLE = "hourly_demand"

def read_silver_trips(spark: SparkSession) -> DataFrame:
    """Read the silver trip table."""
    df_silver_trips = spark.read.parquet(str(SILVER_TRIPS_DIR))
    return df_silver_trips


def build_daily_zone_revenue(df_silver: DataFrame) -> DataFrame:
    """Trips and revenue (total_amount) per pickup date and pickup zone."""
    # Grouped by ID: some zone names repeat in the lookup; borough and zone follow from the ID
    df_daily_zone_revenue = (
        df_silver
        .groupBy("pickup_date", "pickup_location_id", "pickup_borough", "pickup_zone")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_revenue_per_trip"),
        )
        .orderBy("pickup_date", "pickup_location_id")
    )
    return df_daily_zone_revenue


def build_hourly_demand(df_silver: DataFrame) -> DataFrame:
    """Trips per day of week and pickup hour, with the average number of trips per day."""
    # weekday() returns 0 = Monday; +1 gives ISO numbering (1 = Monday ... 7 = Sunday)
    day_of_week = F.weekday("pickup_date") + 1
    day_name = F.date_format("pickup_date", "EEEE")

    df_with_day = (
        df_silver
        .withColumn("pickup_day_of_week", day_of_week)
        .withColumn("pickup_day_name", day_name)
    )

    # Day name follows from the day number, so it does not change the number of groups
    df_counts = (
        df_with_day
        .groupBy("pickup_day_of_week", "pickup_day_name", "pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            # Weekdays can occur a different number of times in a period; this keeps averages fair
            F.countDistinct("pickup_date").alias("day_count"),
        )
    )

    avg_trips_per_day = F.round(F.col("trip_count") / F.col("day_count"), 2)
    df_hourly_demand = (
        df_counts
        .withColumn("avg_trips_per_day", avg_trips_per_day)
        .orderBy("pickup_day_of_week", "pickup_hour")
    )
    return df_hourly_demand


def write_gold_table(df: DataFrame, table_name: str) -> None:
    """Write a gold table as Parquet under GOLD_DIR/<table_name>; overwrite keeps reruns idempotent."""
    target_dir = GOLD_DIR / table_name
    # Gold tables are small aggregates: a single file is enough
    df.coalesce(1).write.mode("overwrite").parquet(str(target_dir))
    print(f"[ok] gold written: {target_dir}")


def verify_gold_table(spark: SparkSession, df_expected: DataFrame, table_name: str) -> DataFrame:
    """Read a gold table back; row count and schema (name + type) must match the in-memory DataFrame."""
    target_dir = GOLD_DIR / table_name
    df_written = spark.read.parquet(str(target_dir))

    written_count = df_written.count()
    expected_count = df_expected.count()
    if written_count != expected_count:
        raise ValueError(f"{table_name} count mismatch: written={written_count:,}, expected={expected_count:,}")

    # Compare name + type only: Parquet read-back marks every column nullable
    written_fields = [(f.name, f.dataType) for f in df_written.schema.fields]
    expected_fields = [(f.name, f.dataType) for f in df_expected.schema.fields]
    if written_fields != expected_fields:
        raise ValueError(f"{table_name} schema mismatch between written and expected")

    print(f"[ok] {table_name} read-back: {written_count:,} rows, {len(written_fields)} columns")
    return df_written


def check_trip_total(df_gold: DataFrame, silver_count: int, table_name: str) -> None:
    """Fail if trip_count does not add up to the silver row count (no trips lost or duplicated)."""
    row_totals = df_gold.agg(F.sum("trip_count").alias("total_trips")).first()
    total_trips = row_totals["total_trips"]
    if total_trips != silver_count:
        raise ValueError(f"{table_name} trip total {total_trips:,} != silver {silver_count:,}")
    print(f"[ok] {table_name}: trip_count total {total_trips:,} = silver")


def main():
    spark = get_spark("nyc-taxi-gold")

    df_silver = read_silver_trips(spark)
    silver_count = df_silver.count()
    column_count = len(df_silver.columns)
    print(f"[ok] silver trips: {silver_count:,} rows, {column_count} columns")

    df_daily_zone_revenue = build_daily_zone_revenue(df_silver)
    write_gold_table(df_daily_zone_revenue, DAILY_ZONE_REVENUE_TABLE)
    df_written_daily = verify_gold_table(spark, df_daily_zone_revenue, DAILY_ZONE_REVENUE_TABLE)
    check_trip_total(df_written_daily, silver_count, DAILY_ZONE_REVENUE_TABLE)

    df_hourly_demand = build_hourly_demand(df_silver)
    write_gold_table(df_hourly_demand, HOURLY_DEMAND_TABLE)
    df_written_hourly = verify_gold_table(spark, df_hourly_demand, HOURLY_DEMAND_TABLE)
    check_trip_total(df_written_hourly, silver_count, HOURLY_DEMAND_TABLE)

    spark.stop()


if __name__ == "__main__":
    main()