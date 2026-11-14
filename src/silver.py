"""Silver layer: clean bronze trips and report data quality per rule."""

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from config import BRONZE_TRIPS_DIR, BRONZE_ZONES_DIR , SILVER_TRIPS_DIR 
from spark_session import get_spark


MILES_TO_KM = 1.609344
MAX_DURATION_MIN = 180
MAX_DISTANCE_KM = 150
MAX_SPEED_KMH = 120

# Captures year (group 1) and month (group 2) from names like yellow_tripdata_2024-01.parquet
SOURCE_FILE_PATTERN = r"_(\d{4})-(\d{2})\.parquet"

# Source column name -> silver column name (snake_case); unlisted columns keep their names
COLUMN_RENAMES = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "trip_distance": "trip_distance_miles",
    "RatecodeID": "ratecode_id",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "Airport_fee": "airport_fee",
}

# Code -> description, from the TLC Yellow Taxi data dictionary (March 2025)
PAYMENT_TYPE_DESCRIPTIONS = {
    0: "Flex Fare",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}

RATECODE_DESCRIPTIONS = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride",
    99: "Unknown",
}

# Codes missing from the dictionaries get this value, so new codes are easy to spot
UNMAPPED_DESCRIPTION = "Unmapped"

def read_bronze_trips(spark: SparkSession) -> DataFrame:
    """Read the bronze trip table."""
    df_bronze_trips = spark.read.parquet(str(BRONZE_TRIPS_DIR))
    return df_bronze_trips


def read_bronze_zones(spark: SparkSession) -> DataFrame:
    """Read the bronze taxi zone lookup table."""
    df_zones = spark.read.parquet(str(BRONZE_ZONES_DIR))
    return df_zones


def standardize_column_names(df: DataFrame) -> DataFrame:
    """Rename source columns to snake_case using COLUMN_RENAMES."""
    df_renamed = df.withColumnsRenamed(COLUMN_RENAMES)
    return df_renamed


def add_rule_columns(df_trips: DataFrame) -> DataFrame:
    """Add the columns needed by the cleaning rules (km, minutes, km/h, months)."""
    duration_min = (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60
    distance_km = F.col("trip_distance_miles") * MILES_TO_KM

    # try_divide returns null instead of failing on zero duration (ANSI mode)
    speed_kmh = F.try_divide(distance_km, duration_min / 60)

    file_year = F.regexp_extract("_source_file", SOURCE_FILE_PATTERN, 1).cast("int")
    file_month = F.regexp_extract("_source_file", SOURCE_FILE_PATTERN, 2).cast("int")

    df_with_rule_columns = (
        df_trips
        .withColumn("trip_distance_km", distance_km)
        .withColumn("trip_duration_min", duration_min)
        .withColumn("avg_speed_kmh", speed_kmh)
        .withColumn("pickup_year", F.year("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
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


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Add pickup date and hour for analysis and gold aggregations."""
    df_with_derived_columns = (
        df
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
    )
    return df_with_derived_columns


def map_code_to_description(code_column: str, descriptions: dict) -> Column:
    """Build a CASE WHEN expression that turns codes into descriptions."""
    code = F.col(code_column)
    # Null must be caught first: null == 1 is null, so it would fall through to "Unmapped"
    description = F.when(code.isNull(), F.lit(None).cast("string"))
    for code_value, text in descriptions.items():
        description = description.when(code == code_value, text)
    description = description.otherwise(UNMAPPED_DESCRIPTION)
    return description


def add_code_descriptions(df: DataFrame) -> DataFrame:
    """Add readable descriptions for payment_type and ratecode_id."""
    payment_type_desc = map_code_to_description("payment_type", PAYMENT_TYPE_DESCRIPTIONS)
    ratecode_desc = map_code_to_description("ratecode_id", RATECODE_DESCRIPTIONS)
    df_with_descriptions = (
        df
        .withColumn("payment_type_desc", payment_type_desc)
        .withColumn("ratecode_desc", ratecode_desc)
    )
    return df_with_descriptions


def check_zone_ids_unique(df_zones: DataFrame) -> None:
    """Fail fast if LocationID repeats: a repeated key would multiply trip rows in the join."""
    zone_count = df_zones.count()
    distinct_id_count = df_zones.select("LocationID").distinct().count()
    if zone_count != distinct_id_count:
        raise ValueError(
            f"Duplicate LocationID in zone lookup: rows={zone_count}, distinct={distinct_id_count}"
        )
    print(f"[ok] zone lookup: {zone_count} rows, LocationID is unique")


def add_zone_columns(df: DataFrame, df_zones: DataFrame, location_prefix: str) -> DataFrame:
    """Add borough and zone names for one location column (pickup or dropoff)."""
    location_column = f"{location_prefix}_location_id"

    # Renamed before the join, so pickup and dropoff columns never clash
    df_prefixed_zones = df_zones.select(
        F.col("LocationID").alias("zone_location_id"),
        F.col("Borough").alias(f"{location_prefix}_borough"),
        F.col("Zone").alias(f"{location_prefix}_zone"),
    )

    join_condition = F.col(location_column) == F.col("zone_location_id")
    # Left join keeps every trip; broadcast sends the small lookup to every executor (no shuffle)
    df_joined = df.join(F.broadcast(df_prefixed_zones), join_condition, "left")
    df_with_zones = df_joined.drop("zone_location_id")
    return df_with_zones


def write_silver_trips(df):
    """Write silver trips as Parquet; overwrite makes reruns idempotent."""
    df.write.mode("overwrite").parquet(str(SILVER_TRIPS_DIR))
    print(f"[ok] silver written: {SILVER_TRIPS_DIR}")


def verify_silver_trips(spark, df_expected):
    """Read silver back from disk; count and schema must match the in-memory DataFrame."""
    df_written = spark.read.parquet(str(SILVER_TRIPS_DIR))

    written_count = df_written.count()
    expected_count = df_expected.count()
    if written_count != expected_count:
        raise ValueError(f"Count mismatch: written={written_count:,}, expected={expected_count:,}")

    # Compare name + type only: Parquet read-back marks every column nullable
    written_fields = [(f.name, f.dataType) for f in df_written.schema.fields]
    expected_fields = [(f.name, f.dataType) for f in df_expected.schema.fields]
    if written_fields != expected_fields:
        raise ValueError("Schema mismatch between written and expected silver")

    print(f"[ok] silver read-back: {written_count:,} rows, {len(written_fields)} columns")
    return written_count


def check_reconciliation(bronze_count, silver_count, rules_dropped_count, duplicates_count):
    """Every bronze row must be accounted for: kept in silver or dropped for a known reason."""
    accounted = silver_count + rules_dropped_count + duplicates_count
    if bronze_count != accounted:
        raise ValueError(
            f"Reconciliation failed: bronze={bronze_count:,}, accounted={accounted:,}"
        )
    print(
        f"[ok] reconciliation: bronze {bronze_count:,} = silver {silver_count:,}"
        f" + rules {rules_dropped_count:,} + duplicates {duplicates_count:,}"
    )


def main():
    spark = get_spark("nyc-taxi-silver")

    df_bronze_trips = read_bronze_trips(spark)
    bronze_count = df_bronze_trips.count()
    print(f"[ok] bronze trips: {bronze_count:,} rows")

    df_trips = standardize_column_names(df_bronze_trips)
    # Taken before the rule columns are added, so only the 19 source columns are listed
    source_columns = get_source_columns(df_trips)

    df_with_rule_columns = add_rule_columns(df_trips)

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

    df_unique = remove_duplicates(df_kept, source_columns)
    df_without_file_columns = df_unique.drop("file_year", "file_month")

    df_with_derived_columns = add_derived_columns(df_without_file_columns)

    df_with_descriptions = add_code_descriptions(df_with_derived_columns)

    df_bronze_zones = read_bronze_zones(spark)
    check_zone_ids_unique(df_bronze_zones)
    df_with_pickup_zones = add_zone_columns(df_with_descriptions, df_bronze_zones, "pickup")
    df_silver = add_zone_columns(df_with_pickup_zones, df_bronze_zones, "dropoff")

    silver_count = df_silver.count()
    duplicates_count = kept_count - silver_count
    print(f"[data quality] duplicates removed: {duplicates_count:,}")
    print(f"[ok] silver trips: {silver_count:,} rows")

    write_silver_trips(df_silver)
    written_count = verify_silver_trips(spark, df_silver)

    check_reconciliation(bronze_count, written_count, filtered_count, duplicates_count)

    spark.stop()


if __name__ == "__main__":
    main()