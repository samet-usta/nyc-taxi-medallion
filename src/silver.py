"""Silver layer: clean bronze trips and report data quality per rule."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import BRONZE_TRIPS_DIR
from spark_session import get_spark

MILES_TO_KM = 1.609344
MAX_DURATION_MIN = 180
MAX_DISTANCE_KM = 150
MAX_SPEED_KMH = 120

# Captures year (group 1) and month (group 2) from names like yellow_tripdata_2024-01.parquet
SOURCE_FILE_PATTERN = r"_(\d{4})-(\d{2})\.parquet"

def read_bronze_trips(spark: SparkSession) -> DataFrame:
    """Read the bronze trip table."""
    df_bronze_trips = spark.read.parquet(str(BRONZE_TRIPS_DIR))
    return df_bronze_trips


def add_rule_columns(df_trips: DataFrame) -> DataFrame:
    """Add the columns needed by the cleaning rules (km, minutes, km/h, months)."""
    duration_min = (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
    distance_km = F.col("trip_distance") * MILES_TO_KM
    
    # try_divide returns null instead of failing on zero duration (ANSI mode)
    speed_kmh = F.try_divide(distance_km, duration_min / 60)
    
    file_year = F.regexp_extract("_source_file", SOURCE_FILE_PATTERN, 1).cast("int")
    file_month = F.regexp_extract("_source_file", SOURCE_FILE_PATTERN, 2).cast("int")

    df_with_rule_columns = (
        df_trips
        .withColumn("trip_distance_km", distance_km)
        .withColumn("trip_duration_min", duration_min)
        .withColumn("avg_speed_kmh", speed_kmh)
        .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
        .withColumn("file_year", file_year)
        .withColumn("file_month", file_month)
    )
    return df_with_rule_columns


def build_rules() -> dict:
    """Return the cleaning rules as {rule name: condition that marks a row to drop}."""
    pickup_outside_file_month = (F.col("pickup_year") != F.col("file_year")) | (F.col("pickup_month") != F.col("file_month"))

    rules = {
        "zero_or_negative_fare_amount": F.col("fare_amount") <= 0,
        "zero_or_negative_total_amount": F.col("total_amount") <= 0,
        "dropoff_before_pickup": F.col("trip_duration_min") < 0,
        "zero_duration": F.col("trip_duration_min") == 0,
        "pickup_outside_file_month": pickup_outside_file_month,
        "too_long_duration": F.col("trip_duration_min") > MAX_DURATION_MIN,
        "too_long_distance": F.col("trip_distance_km") > MAX_DISTANCE_KM,
        "too_fast": F.col("avg_speed_kmh") > MAX_SPEED_KMH,
    }
    return rules


def build_drop_condition(rules: dict):
    """Combine all rules with OR: a row is dropped if it matches at least one rule."""
    # Starting from False does not change the result: False | x = x
    drop_condition = F.lit(False)
    for condition in rules.values():
        drop_condition = drop_condition | condition
    return drop_condition


def report_rule_counts(df: DataFrame, rules: dict, drop_condition) -> int:
    """Print how many rows each rule matches, in a single scan of the data."""
    count_expressions = [F.count("*").alias("total_rows")]
    for rule_name, condition in rules.items():
        count_expressions.append(F.sum(condition.cast("int")).alias(rule_name))
    count_expressions.append(F.sum(drop_condition.cast("int")).alias("any_rule"))

    row_counts = df.select(count_expressions).first()
    total_rows = row_counts["total_rows"]

    print("[data quality] rows matched per rule:")
    for column_name in row_counts.asDict():
        rule_count = row_counts[column_name]
        rule_pct = rule_count / total_rows * 100
        print(f"  {column_name}: {rule_count:,} ({rule_pct:.2f}%)")
    return row_counts["any_rule"]

def apply_rules(df: DataFrame, drop_condition) -> DataFrame:
    """Keep only the rows that match none of the rules."""
    # A rule that evaluates to null counts as "not matched", same as in the report
    drop_condition_without_null = F.coalesce(drop_condition, F.lit(False))
    df_kept = df.filter(~drop_condition_without_null)
    return df_kept


def get_source_columns(df: DataFrame) -> list[str]:
    """Return the columns that come from the source (not starting with '_')."""
    source_columns = []
    for column_name in df.columns:
        if not column_name.startswith("_"):
            source_columns.append(column_name)
    return source_columns


def remove_duplicates(df: DataFrame, source_columns: list[str]) -> DataFrame:
    """Drop rows whose source columns are all identical to another row."""
    df_unique = df.dropDuplicates(source_columns)
    return df_unique


def main():
    spark = get_spark("nyc-taxi-silver")

    df_bronze_trips = read_bronze_trips(spark)
    bronze_count = df_bronze_trips.count()
    print(f"[ok] bronze trips: {bronze_count:,} rows")

    df_with_rule_columns = add_rule_columns(df_bronze_trips)

    rules = build_rules()
    drop_condition = build_drop_condition(rules)
    
    rules_dropped_count = report_rule_counts(df_with_rule_columns, rules, drop_condition)

    df_kept = apply_rules(df_with_rule_columns, drop_condition)
    kept_count = df_kept.count()
    filtered_count = bronze_count - kept_count
    if filtered_count != rules_dropped_count:
        raise ValueError(
            f"Filter mismatch: report={rules_dropped_count:,}, filtered={filtered_count:,}"
        )
    print(f"[ok] dropped by rules: {filtered_count:,} rows (report = filter)")

    source_columns = get_source_columns(df_bronze_trips)
    df_unique = remove_duplicates(df_kept, source_columns)
    df_silver = df_unique.drop("file_year", "file_month")

    silver_count = df_silver.count()
    duplicates_count = kept_count - silver_count
    print(f"[data quality] duplicates removed: {duplicates_count:,}")
    print(f"[ok] silver trips: {silver_count:,} rows")
    

    spark.stop()


if __name__ == "__main__":
    main()