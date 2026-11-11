"""Download NYC TLC trip data and the taxi zone lookup into data/raw/.

The script is idempotent: files that already exist are skipped.
"""

import urllib.request
from pathlib import Path

from config import BASE_URL, MONTHS, RAW_DIR, TAXI_TYPE, YEAR


def build_trip_url(year: int, month: int) -> str:
    """Return the download URL for one month of trip data."""
    return f"{BASE_URL}/trip-data/{TAXI_TYPE}_tripdata_{year}-{month:02d}.parquet"


def download_file(url: str, target: Path) -> None:
    """Download a file to target, skipping it if it already exists.

    Data is written to a temporary .part file and renamed on success,
    so an interrupted download never looks like a complete file.
    """
    if target.exists():
        print(f"[skip] {target.name} already exists")
        return

    print(f"[download] {target.name} ...")
    tmp = target.with_suffix(target.suffix + ".part")
    urllib.request.urlretrieve(url, tmp)
    tmp.rename(target)  # reached only if the download completed
    size_mb = target.stat().st_size / 1024 / 1024
    print(f"[ok] {target.name} ({size_mb:.1f} MB)")


def main() -> None:
    """Download all configured months plus the zone lookup CSV."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    for month in MONTHS:
        url = build_trip_url(YEAR, month)
        target = RAW_DIR / f"{TAXI_TYPE}_tripdata_{YEAR}-{month:02d}.parquet"
        download_file(url, target)

    # Zone lookup (LocationID -> Borough, Zone), joined in the silver layer
    zone_url = f"{BASE_URL}/misc/taxi_zone_lookup.csv"
    download_file(zone_url, RAW_DIR / "taxi_zone_lookup.csv")


if __name__ == "__main__":
    main()