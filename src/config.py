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

# Source: NYC TLC Trip Record Data
BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
TAXI_TYPE = "yellow"
YEAR = 2024
MONTHS = [1, 2, 3]  # dev subset; use list(range(1, 13)) for the full year