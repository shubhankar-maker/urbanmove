import os
from datetime import date
from pathlib import Path

import pandas as pd
import requests


NYC_311_API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

REQUIRED_COLUMNS = {
    "unique_key",
    "created_date",
    "complaint_type",
    "descriptor",
    "borough",
    "incident_zip",
    "status",
}

PAGE_SIZE = 1000

def validate_date_range(start_date: str, end_date: str) -> None:
    """Validate the requested NYC 311 date range."""

    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
    except ValueError:
        raise ValueError("Dates must use YYYY-MM-DD format.")

    if start.isoformat() != start_date or end.isoformat() != end_date:
        raise ValueError("Dates must use YYYY-MM-DD format.")

    if start >= end:
        raise ValueError("start_date must be before end_date.")


def download_311_data(start_date: str, end_date: str) -> Path:
    """Download NYC 311 requests for a date range using pagination."""

    validate_date_range(start_date, end_date)

    data_dir = Path(os.getenv("URBANMOVE_DATA_DIR", "data"))


def validate_311_data(file_path: Path) -> dict:
    """Validate the saved NYC 311 CSV."""
    df = pd.read_csv(file_path)

    missing_columns = REQUIRED_COLUMNS - set(df.columns)
    if missing_columns:
        raise ValueError(
            f"NYC 311 schema is missing columns: {sorted(missing_columns)}"
        )

    if df.empty:
        raise ValueError("NYC 311 dataset is empty.")

    if df["unique_key"].isna().any():
        raise ValueError("NYC 311 unique_key contains missing values.")

    if df["unique_key"].duplicated().any():
        raise ValueError("NYC 311 unique_key contains duplicates.")

    return {
        "row_count": len(df),
        "columns": list(df.columns),
        "missing_values": df.isna().sum().to_dict(),
        "duplicate_keys": int(df["unique_key"].duplicated().sum()),
    }


def download_311_data(start_date: str, end_date: str) -> Path:
    """Download NYC 311 requests for a date range using pagination."""

    data_dir = Path(os.getenv("URBANMOVE_DATA_DIR", "data"))
    raw_dir = data_dir / "raw" / "311"
    raw_dir.mkdir(parents=True, exist_ok=True)

    output_file = raw_dir / f"nyc_311_{start_date}_{end_date}.csv"

    if output_file.exists() and output_file.stat().st_size > 0:
        print(f"Using existing NYC 311 file: {output_file}")
        report = validate_311_data(output_file)
    else:
        all_records = []
        offset = 0

        while True:
            params = {
                "$select": ",".join(sorted(REQUIRED_COLUMNS)),
                "$where": (
                    f"created_date >= '{start_date}T00:00:00' "
                    f"AND created_date < '{end_date}T00:00:00'"
                ),
                "$limit": PAGE_SIZE,
                "$offset": offset,
                "$order": "unique_key",
            }

            response = requests.get(
                NYC_311_API_URL,
                params=params,
                timeout=30,
            )
            response.raise_for_status()

            records = response.json()

            if not isinstance(records, list):
                raise ValueError("NYC 311 API returned an unexpected response.")

            if not records:
                break

            all_records.extend(records)
            print(f"Fetched {len(all_records)} records so far.")

            if len(records) < PAGE_SIZE:
                break

            offset += PAGE_SIZE

        if not all_records:
            raise ValueError("NYC 311 API returned no records for this date range.")

        df = pd.DataFrame(all_records)

        missing_columns = REQUIRED_COLUMNS - set(df.columns)
        if missing_columns:
            raise ValueError(
                f"NYC 311 API schema is missing columns: "
                f"{sorted(missing_columns)}"
            )

        temporary_file = output_file.with_suffix(".csv.tmp")

        try:
            df.to_csv(temporary_file, index=False)
            validate_311_data(temporary_file)
            temporary_file.replace(output_file)
        finally:
            temporary_file.unlink(missing_ok=True)

        print(f"Downloaded NYC 311 data to: {output_file}")
        report = validate_311_data(output_file)

    print(f"Rows: {report['row_count']}")
    print(f"Duplicate keys: {report['duplicate_keys']}")
    print(f"Missing values: {report['missing_values']}")

    return output_file


if __name__ == "__main__":
    download_311_data("2025-01-01", "2025-01-02")
