"""Shared SparkSession factory for all pipeline jobs."""

from pyspark.sql import SparkSession

def get_spark(app_name: str) -> SparkSession:
    """Create (or reuse) a local SparkSession with project defaults."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

if __name__ == "__main__":
    print(get_spark("test").version)