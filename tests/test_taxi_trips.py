
import pandas as pd
import pyarrow.parquet as pq
import pytest
from unittest.mock import MagicMock, patch

from src.urbanmove.ingestion.taxi_trips import (
    REQUIRED_COLUMNS,
    download_taxi_trips,
    validate_taxi_trips_file,
)

def test_invalid_month_fails_before_download():
    with pytest.raises(ValueError, match="month must be between 1 and 12"):
        download_taxi_trips(2025, 13)


def test_zero_month_fails_before_download():
    with pytest.raises(ValueError, match="month must be between 1 and 12"):
        download_taxi_trips(2025, 0)


def test_year_before_2009_fails_before_download():
    with pytest.raises(ValueError, match="year must be 2009 or later"):
        download_taxi_trips(2008, 1)


def test_parquet_missing_required_column_is_rejected(tmp_path):
    columns = sorted(REQUIRED_COLUMNS)
    columns.remove("PULocationID")

    df = pd.DataFrame({column: [] for column in columns})
    file_path = tmp_path / "incomplete.parquet"
    df.to_parquet(file_path, index=False)

    parquet_file = pq.ParquetFile(file_path)
    actual_columns = set(parquet_file.schema_arrow.names)
    missing_columns = REQUIRED_COLUMNS - actual_columns

    assert missing_columns == {"PULocationID"}


def test_empty_parquet_file_is_rejected(tmp_path):
    import pandas as pd

    file_path = tmp_path / "empty.parquet"
    pd.DataFrame(columns=sorted(REQUIRED_COLUMNS)).to_parquet(
        file_path,
        index=False,
    )

    with pytest.raises(
        ValueError, match="Downloaded Parquet file is empty"
    ):
        validate_taxi_trips_file(file_path)


def test_parquet_missing_required_column_is_rejected(tmp_path):
    columns = sorted(REQUIRED_COLUMNS - {"PULocationID"})
    df = pd.DataFrame({column: [1] for column in columns})

    file_path = tmp_path / "incomplete.parquet"
    df.to_parquet(file_path, index=False)

    with pytest.raises(ValueError, match="Missing required columns"):
        validate_taxi_trips_file(file_path)



def test_successful_taxi_trip_download(tmp_path, monkeypatch):
    import src.urbanmove.ingestion.taxi_trips as taxi_trips

    # Create a small, valid Parquet file for the mock response.
    source_file = tmp_path / "source.parquet"
    df = pd.DataFrame(
        {
            column: [1]
            for column in REQUIRED_COLUMNS
        }
    )
    df["tpep_pickup_datetime"] = pd.to_datetime(
        ["2025-01-01 10:00:00"]
    )
    df["tpep_dropoff_datetime"] = pd.to_datetime(
        ["2025-01-01 10:15:00"]
    )
    df.to_parquet(source_file, index=False)
    parquet_bytes = source_file.read_bytes()

    # Redirect output to a temporary test directory.
    monkeypatch.chdir(tmp_path)

    mock_response = MagicMock()
    mock_response.__enter__.return_value = mock_response
    mock_response.iter_content.return_value = [parquet_bytes]

    with patch.object(
        taxi_trips.requests, "get", return_value=mock_response
    ) as mock_get:
        output_path = taxi_trips.download_taxi_trips(2025, 1)

    assert output_path.exists()
    assert output_path.name == "yellow_tripdata_2025-01.parquet"
    assert taxi_trips.validate_taxi_trips_file(output_path)["row_count"] == 1
    mock_get.assert_called_once()




def test_http_error_cleans_up_temporary_file(tmp_path, monkeypatch):
    import requests
    from unittest.mock import MagicMock, patch

    import src.urbanmove.ingestion.taxi_trips as taxi_trips

    monkeypatch.chdir(tmp_path)

    mock_response = MagicMock()
    mock_response.__enter__.return_value = mock_response
    mock_response.raise_for_status.side_effect = requests.HTTPError(
        "404 Not Found"
    )

    with patch.object(
        taxi_trips.requests, "get", return_value=mock_response
    ):
        with pytest.raises(requests.HTTPError, match="404 Not Found"):
            taxi_trips.download_taxi_trips(2025, 1)

    output_dir = tmp_path / "data/raw/taxi_trips"

    assert not (
        output_dir / "yellow_tripdata_2025-01.parquet.tmp"
    ).exists()
    assert not (
        output_dir / "yellow_tripdata_2025-01.parquet"
    ).exists()



def test_existing_valid_file_is_reused(tmp_path, monkeypatch):
    import src.urbanmove.ingestion.taxi_trips as taxi_trips
    from unittest.mock import patch

    monkeypatch.chdir(tmp_path)

    output_dir = tmp_path / "data/raw/taxi_trips"
    output_dir.mkdir(parents=True)

    output_path = output_dir / "yellow_tripdata_2025-01.parquet"

    df = pd.DataFrame(
        {
            column: [1]
            for column in REQUIRED_COLUMNS
        }
    )
    df["tpep_pickup_datetime"] = pd.to_datetime(
        ["2025-01-01 10:00:00"]
    )
    df["tpep_dropoff_datetime"] = pd.to_datetime(
        ["2025-01-01 10:15:00"]
    )
    df.to_parquet(output_path, index=False)

    with patch.object(taxi_trips.requests, "get") as mock_get:
        result = taxi_trips.download_taxi_trips(2025, 1)

    assert result.resolve() == output_path.resolve()
    assert result.exists()
    mock_get.assert_not_called()




def test_failed_replacement_preserves_existing_file(
    tmp_path, monkeypatch
):
    from unittest.mock import MagicMock, patch

    import requests
    import src.urbanmove.ingestion.taxi_trips as taxi_trips

    monkeypatch.chdir(tmp_path)

    output_dir = tmp_path / "data/raw/taxi_trips"
    output_dir.mkdir(parents=True)

    output_path = output_dir / "yellow_tripdata_2025-01.parquet"

    df = pd.DataFrame(
        {column: [1] for column in REQUIRED_COLUMNS}
    )
    df["tpep_pickup_datetime"] = pd.to_datetime(
        ["2025-01-01 10:00:00"]
    )
    df["tpep_dropoff_datetime"] = pd.to_datetime(
        ["2025-01-01 10:15:00"]
    )
    df.to_parquet(output_path, index=False)

    original_bytes = output_path.read_bytes()

    # Force the existing file to be treated as invalid.
    with patch.object(
        taxi_trips,
        "validate_taxi_trips_file",
        side_effect=[
            ValueError("Simulated validation failure"),
            requests.HTTPError("Simulated download failure"),
        ],
    ):
        mock_response = MagicMock()
        mock_response.__enter__.return_value = mock_response
        mock_response.raise_for_status.side_effect = requests.HTTPError(
            "Simulated download failure"
        )

        with patch.object(
            taxi_trips.requests,
            "get",
            return_value=mock_response,
        ):
            with pytest.raises(requests.HTTPError):
                taxi_trips.download_taxi_trips(2025, 1)

    assert output_path.exists()
    assert output_path.read_bytes() == original_bytes
    assert not output_path.with_suffix(
        ".parquet.tmp"
    ).exists()
