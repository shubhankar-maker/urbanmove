
import os
from pathlib import Path

import pandas as pd
import requests


TAXI_ZONES_URL = (
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
)

REQUIRED_COLUMNS = {
    "LocationID",
    "Borough",
    "Zone",
    "service_zone",
}


def validate_taxi_zones(file_path: Path) -> dict:
    """Validate the taxi-zone CSV and report data-quality findings."""

    df = pd.read_csv(file_path)

    missing_columns = REQUIRED_COLUMNS - set(df.columns)
    if missing_columns:
        raise ValueError(
            f"Taxi-zone schema is missing columns: {sorted(missing_columns)}"
        )

    if df["LocationID"].isna().any():
        raise ValueError("LocationID contains missing values.")

    if df["LocationID"].duplicated().any():
        raise ValueError("LocationID contains duplicate identifiers.")

    report = {
        "row_count": len(df),
        "columns": list(df.columns),
        "missing_values": df.isna().sum().to_dict(),
        "duplicate_rows": int(df.duplicated().sum()),
    }

    return report


def download_taxi_zones() -> Path:
    """Download taxi zones once and validate the resulting CSV."""

    data_dir = Path(os.getenv("URBANMOVE_DATA_DIR", "data"))
    raw_dir = data_dir / "raw" / "taxi_zones"
    raw_dir.mkdir(parents=True, exist_ok=True)

    output_file = raw_dir / "taxi_zone_lookup.csv"

    if not (output_file.exists() and output_file.stat().st_size > 0):
        response = requests.get(TAXI_ZONES_URL, timeout=30)
        response.raise_for_status()

        if not response.content:
            raise ValueError("Downloaded taxi-zone file is empty.")

        temporary_file = output_file.with_suffix(".csv.tmp")

        try:
            temporary_file.write_bytes(response.content)
            validate_taxi_zones(temporary_file)
            temporary_file.replace(output_file)
        finally:
            temporary_file.unlink(missing_ok=True)

        print(f"Downloaded taxi zones data to {output_file}")
    else:
        print(f"Using existing taxi-zone file: {output_file}")

    report = validate_taxi_zones(output_file)

    print(f"Rows: {report['row_count']}")
    print(f"Duplicate rows: {report['duplicate_rows']}")
    print(f"Missing values: {report['missing_values']}")

    return output_file


if __name__ == "__main__":
    download_taxi_zones()
