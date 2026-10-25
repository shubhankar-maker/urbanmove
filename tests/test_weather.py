
import pandas as pd
import pytest
from datetime import date
import requests
from src.urbanmove.ingestion.weather import validate_date_range

from src.urbanmove.ingestion.weather import validate_weather

from unittest.mock import Mock, patch

from src.urbanmove.ingestion.weather import (
    WEATHER_API_URL,
    download_weather,
    validate_date_range,
    validate_weather,
)

def test_invalid_date_format_fails():
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        validate_date_range("2025/01/01", "2025-01-02")


def test_start_date_after_end_date_fails():
    with pytest.raises(ValueError, match="on or before"):
        validate_date_range("2025-01-03", "2025-01-01")


def test_valid_weather_file(tmp_path):
    df = pd.DataFrame(
        {
            "time": ["2025-01-01T00:00", "2025-01-01T01:00"],
            "temperature_2m": [2.5, 2.1],
            "precipitation": [0.0, 0.2],
            "rain": [0.0, 0.2],
            "wind_speed_10m": [10.0, 12.0],
        }
    )

    file_path = tmp_path / "weather.csv"
    df.to_csv(file_path, index=False)

    report = validate_weather(file_path)

    assert report["row_count"] == 2
    assert report["duplicate_timestamps"] == 0


def test_missing_required_column_fails(tmp_path):
    df = pd.DataFrame(
        {
            "time": ["2025-01-01T00:00"],
            "temperature_2m": [2.5],
            "precipitation": [0.0],
            "rain": [0.0],
        }
    )

    file_path = tmp_path / "weather.csv"
    df.to_csv(file_path, index=False)

    with pytest.raises(ValueError, match="missing columns"):
        validate_weather(file_path)


def test_duplicate_timestamps_fail(tmp_path):
    df = pd.DataFrame(
        {
            "time": ["2025-01-01T00:00", "2025-01-01T00:00"],
            "temperature_2m": [2.5, 2.1],
            "precipitation": [0.0, 0.2],
            "rain": [0.0, 0.2],
            "wind_speed_10m": [10.0, 12.0],
        }
    )

    file_path = tmp_path / "weather.csv"
    df.to_csv(file_path, index=False)

    with pytest.raises(ValueError, match="timestamps contain duplicates"):
        validate_weather(file_path)


def test_empty_weather_file_fails(tmp_path):
    df = pd.DataFrame(
        columns=[
            "time",
            "temperature_2m",
            "precipitation",
            "rain",
            "wind_speed_10m",
        ]
    )

    file_path = tmp_path / "weather.csv"
    df.to_csv(file_path, index=False)

    with pytest.raises(ValueError, match="empty"):
        validate_weather(file_path)

def test_weather_api_timeout_fails(tmp_path, monkeypatch):
    monkeypatch.setenv("URBANMOVE_DATA_DIR", str(tmp_path))

    with patch(
        "src.urbanmove.ingestion.weather.requests.get",
        side_effect=requests.Timeout("Request timed out"),
    ) as mock_get:
        with pytest.raises(requests.Timeout, match="timed out"):
            download_weather("2025-01-03", "2025-01-04")

    mock_get.assert_called_once()
    assert mock_get.call_args.kwargs["timeout"] == 30

def test_weather_api_http_error_fails(tmp_path, monkeypatch):
    monkeypatch.setenv("URBANMOVE_DATA_DIR", str(tmp_path))

    response = Mock()
    response.raise_for_status.side_effect = requests.HTTPError(
        "503 Service Unavailable"
    )

    with patch(
        "src.urbanmove.ingestion.weather.requests.get",
        return_value=response,
    ) as mock_get:
        with pytest.raises(requests.HTTPError, match="503"):
            download_weather("2025-01-03", "2025-01-04")

    mock_get.assert_called_once()