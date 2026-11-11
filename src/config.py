"""Central configuration for the NYC Taxi medallion pipeline.

All paths and source settings live here so other scripts import them
instead of hard-coding values.
"""

from pathlib import Path

# Repo root: one level above src/, independent of the working directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Data layers (data/ is git-ignored)
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
BRONZE_TRIPS_DIR = BRONZE_DIR / "yellow_trips"
BRONZE_ZONES_DIR = BRONZE_DIR / "taxi_zones"
ZONE_LOOKUP_FILE = RAW_DIR / "taxi_zone_lookup.csv"


# Source: NYC TLC Trip Record Data
BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
TAXI_TYPE = "yellow"
YEAR = 2024
MONTHS = [1, 2, 3]  # dev subset; use list(range(1, 13)) for the full year