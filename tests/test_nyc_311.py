

from unittest.mock import Mock, patch

import pandas as pd
import pytest

from src.urbanmove.ingestion.nyc_311 import (
    PAGE_SIZE,
    download_311_data,
    validate_311_data,
    validate_date_range,
)

def make_311_dataframe():
    return pd.DataFrame(
        {
            "unique_key": ["1001", "1002"],
            "created_date": [
                "2025-01-01T10:00:00",
                "2025-01-01T11:00:00",
            ],
            "complaint_type": ["Street Condition", "Noise"],
            "descriptor": ["Pothole", "Loud Music"],
            "borough": ["BROOKLYN", "MANHATTAN"],
            "incident_zip": ["11209", "10001"],
            "status": ["Closed", "Open"],
        }
    )


def test_valid_311_file(tmp_path):
    df = make_311_dataframe()
    file_path = tmp_path / "nyc_311.csv"
    df.to_csv(file_path, index=False)

    report = validate_311_data(file_path)

    assert report["row_count"] == 2
    assert report["duplicate_keys"] == 0
    assert report["missing_values"]["borough"] == 0


def test_missing_required_column_fails(tmp_path):
    df = make_311_dataframe().drop(columns=["borough"])
    file_path = tmp_path / "nyc_311.csv"
    df.to_csv(file_path, index=False)

    with pytest.raises(ValueError, match="schema is missing columns"):
        validate_311_data(file_path)


def test_duplicate_unique_keys_fail(tmp_path):
    df = make_311_dataframe()
    df["unique_key"] = ["1001", "1001"]
    file_path = tmp_path / "nyc_311.csv"
    df.to_csv(file_path, index=False)

    with pytest.raises(ValueError, match="unique_key contains duplicates"):
        validate_311_data(file_path)


def test_missing_unique_key_fails(tmp_path):
    df = make_311_dataframe()
    df["unique_key"] = ["1001", None]
    file_path = tmp_path / "nyc_311.csv"
    df.to_csv(file_path, index=False)

    with pytest.raises(ValueError, match="unique_key contains missing values"):
        validate_311_data(file_path)


def test_empty_311_file_fails(tmp_path):
    df = make_311_dataframe().iloc[0:0]
    file_path = tmp_path / "nyc_311.csv"
    df.to_csv(file_path, index=False)

    with pytest.raises(ValueError, match="dataset is empty"):
        validate_311_data(file_path)

def test_311_pagination_fetches_multiple_pages(tmp_path, monkeypatch):
    monkeypatch.setenv("URBANMOVE_DATA_DIR", str(tmp_path))

    first_page = [
        {
            "unique_key": str(i),
            "created_date": "2025-01-01T10:00:00",
            "complaint_type": "Street Condition",
            "descriptor": "Pothole",
            "borough": "BROOKLYN",
            "incident_zip": "11209",
            "status": "Open",
        }
        for i in range(PAGE_SIZE)
    ]

    second_page = [
        {
            "unique_key": "second-page-record",
            "created_date": "2025-01-01T11:00:00",
            "complaint_type": "Noise",
            "descriptor": "Loud Music",
            "borough": "MANHATTAN",
            "incident_zip": "10001",
            "status": "Closed",
        }
    ]

    first_response = Mock()
    first_response.json.return_value = first_page
    first_response.raise_for_status.return_value = None

    second_response = Mock()
    second_response.json.return_value = second_page
    second_response.raise_for_status.return_value = None

    with patch(
        "src.urbanmove.ingestion.nyc_311.requests.get",
        side_effect=[first_response, second_response],
    ) as mock_get:
        output_file = download_311_data("2025-01-01", "2025-01-02")

    assert output_file.exists()

    saved_df = pd.read_csv(output_file)
    assert len(saved_df) == PAGE_SIZE + 1
    assert saved_df["unique_key"].nunique() == PAGE_SIZE + 1

    assert mock_get.call_count == 2
    assert mock_get.call_args_list[0].kwargs["params"]["$offset"] == 0
    assert mock_get.call_args_list[1].kwargs["params"]["$offset"] == PAGE_SIZE



def test_311_api_http_error_fails(tmp_path, monkeypatch):
    import requests

    monkeypatch.setenv("URBANMOVE_DATA_DIR", str(tmp_path))

    with patch(
        "src.urbanmove.ingestion.nyc_311.requests.get",
        side_effect=requests.HTTPError("503 Service Unavailable"),
    ):
        with pytest.raises(requests.HTTPError, match="503"):
            download_311_data("2025-01-01", "2025-01-02")

    output_file = (
        tmp_path
        / "raw"
        / "311"
        / "nyc_311_2025-01-01_2025-01-02.csv"
    )
    assert not output_file.exists()

def test_311_invalid_date_format_fails():
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        validate_date_range("2025/01/01", "2025-01-02")


def test_311_start_date_after_end_date_fails():
    with pytest.raises(ValueError, match="before end_date"):
        validate_date_range("2025-01-03", "2025-01-02")


def test_311_no_records_fails_without_creating_csv(tmp_path, monkeypatch):
    monkeypatch.setenv("URBANMOVE_DATA_DIR", str(tmp_path))

    response = Mock()
    response.json.return_value = []
    response.raise_for_status.return_value = None

    with patch(
        "src.urbanmove.ingestion.nyc_311.requests.get",
        return_value=response,
    ):
        with pytest.raises(ValueError, match="returned no records"):
            download_311_data("2025-01-01", "2025-01-02")

    output_file = (
        tmp_path
        / "raw"
        / "311"
        / "nyc_311_2025-01-01_2025-01-02.csv"
    )
    assert not output_file.exists()
