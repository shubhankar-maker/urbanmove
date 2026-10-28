
from pathlib import Path

import pyarrow.parquet as pq
import requests


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

REQUIRED_COLUMNS = {
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
    "total_amount",
}



def validate_taxi_trips_file(file_path: Path) -> dict:
    """Validate Parquet readability, row count, and required schema."""
    parquet_file = pq.ParquetFile(file_path)

    metadata_row_count = parquet_file.metadata.num_rows

    if metadata_row_count == 0:
        raise ValueError("Downloaded Parquet file is empty")

    actual_columns = set(parquet_file.schema_arrow.names)
    missing_columns = REQUIRED_COLUMNS - actual_columns

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    # Read the file in batches to check that its data pages are readable.
    readable_row_count = 0

    for batch in parquet_file.iter_batches(batch_size=65_536):
        readable_row_count += batch.num_rows

    if readable_row_count != metadata_row_count:
        raise ValueError(
            "Parquet row count mismatch: "
            f"metadata={metadata_row_count}, "
            f"readable={readable_row_count}"
        )

    return {
        "row_count": readable_row_count,
        "columns": sorted(actual_columns),
    }



def download_taxi_trips(year: int, month: int) -> Path:
    """Download and validate one monthly NYC Yellow Taxi Parquet file."""
    if not 1 <= month <= 12:
        raise ValueError("month must be between 1 and 12")

    if year < 2009:
        raise ValueError("year must be 2009 or later")

    output_dir = Path("data/raw/taxi_trips")
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    output_path = output_dir / filename
    temp_path = output_dir / f"{filename}.tmp"

    # Reuse an existing file only if it passes validation.
    if output_path.exists():
        try:
            report = validate_taxi_trips_file(output_path)
            print(f"Reusing validated file: {output_path}")
            print(f"Rows: {report['row_count']}")
            return output_path
        except Exception:
            # An invalid existing file must be downloaded again.
            pass

    url = f"{BASE_URL}/{filename}"

    try:
        with requests.get(
            url, stream=True, timeout=(10, 120)
        ) as response:
            response.raise_for_status()

            with temp_path.open("wb") as file:
                for chunk in response.iter_content(
                    chunk_size=1024 * 1024
                ):
                    if chunk:
                        file.write(chunk)

        report = validate_taxi_trips_file(temp_path)

        # Replace the destination only after validation succeeds.
        temp_path.replace(output_path)

        print(f"Downloaded and validated: {output_path}")
        print(f"Rows: {report['row_count']}")

        return output_path

    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
