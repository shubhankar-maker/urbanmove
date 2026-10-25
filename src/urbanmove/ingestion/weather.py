
import os
from pathlib import Path

import pandas as pd
import requests
from datetime import date



WEATHER_API_URL = "https://archive-api.open-meteo.com/v1/archive"

REQUIRED_COLUMNS = {
    "time",
    "temperature_2m",
    "precipitation",
    "rain",
    "wind_speed_10m",
}



def validate_date_range(start_date: str, end_date: str) -> None:
    """Validate the requested weather date range."""

    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
    except ValueError:
        raise ValueError("Dates must use YYYY-MM-DD format.")

    if start.isoformat() != start_date or end.isoformat() != end_date:
        raise ValueError("Dates must use YYYY-MM-DD format.")

    if start > end:
        raise ValueError("start_date must be on or before end_date.")


def validate_weather(file_path: Path) -> dict:
    """Validate the saved hourly weather CSV."""

    df = pd.read_csv(file_path)

    missing_columns = REQUIRED_COLUMNS - set(df.columns)
    if missing_columns:
        raise ValueError(
            f"Weather schema is missing columns: {sorted(missing_columns)}"
        )

    if df.empty:
        raise ValueError("Weather dataset is empty.")

    if df["time"].isna().any():
        raise ValueError("Weather timestamps contain missing values.")

    if df["time"].duplicated().any():
        raise ValueError("Weather timestamps contain duplicates.")

    return {
        "row_count": len(df),
        "columns": list(df.columns),
        "missing_values": df.isna().sum().to_dict(),
        "duplicate_timestamps": int(df["time"].duplicated().sum()),
    }


def download_weather(start_date: str, end_date: str) -> Path:
    """Download hourly NYC weather for a date range."""
    validate_date_range(start_date, end_date)

    data_dir = Path(os.getenv("URBANMOVE_DATA_DIR", "data"))
    raw_dir = data_dir / "raw" / "weather"
    raw_dir.mkdir(parents=True, exist_ok=True)

    output_file = raw_dir / f"nyc_weather_{start_date}_{end_date}.csv"

    if output_file.exists() and output_file.stat().st_size > 0:
        print(f"Using existing weather file: {output_file}")
        report = validate_weather(output_file)
    else:
        params = {
            "latitude": 40.7128,
            "longitude": -74.0060,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": (
                "temperature_2m,precipitation,rain,wind_speed_10m"
            ),
            "timezone": "America/New_York",
        }

        response = requests.get(
            WEATHER_API_URL,
            params=params,
            timeout=30,
        )
        response.raise_for_status()

        payload = response.json()
        hourly = payload.get("hourly")

        if not hourly or "time" not in hourly:
            raise ValueError("Weather API returned no hourly timestamp data.")

        df = pd.DataFrame(hourly)

        missing_columns = REQUIRED_COLUMNS - set(df.columns)
        if missing_columns:
            raise ValueError(
                f"Weather API schema is missing columns: "
                f"{sorted(missing_columns)}"
            )

        if df.empty:
            raise ValueError("Weather API returned no weather records.")

        temporary_file = output_file.with_suffix(".csv.tmp")

        try:
            df.to_csv(temporary_file, index=False)
            validate_weather(temporary_file)
            temporary_file.replace(output_file)
        finally:
            temporary_file.unlink(missing_ok=True)

        print(f"Downloaded weather data to: {output_file}")
        report = validate_weather(output_file)

    print(f"Rows: {report['row_count']}")
    print(f"Duplicate timestamps: {report['duplicate_timestamps']}")
    print(f"Missing values: {report['missing_values']}")

    return output_file


if __name__ == "__main__":
    download_weather("2025-01-01", "2025-01-02")
