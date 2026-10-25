
import pandas as pd
import pytest

from src.urbanmove.ingestion.taxi_zones import validate_taxi_zones


def test_valid_taxi_zones_file(tmp_path):
    df = pd.DataFrame(
        {
            "LocationID": [1, 2, 264],
            "Borough": ["Manhattan", "Queens", "Unknown"],
            "Zone": ["Alphabet City", "Jamaica Bay", None],
            "service_zone": ["Yellow Zone", "Boro Zone", None],
        }
    )

    file_path = tmp_path / "taxi_zones.csv"
    df.to_csv(file_path, index=False)

    report = validate_taxi_zones(file_path)

    assert report["row_count"] == 3
    assert report["duplicate_rows"] == 0
    assert report["missing_values"]["Zone"] == 1


def test_missing_required_column_fails(tmp_path):
    df = pd.DataFrame(
        {
            "LocationID": [1],
            "Borough": ["Manhattan"],
            "Zone": ["Alphabet City"],
        }
    )

    file_path = tmp_path / "taxi_zones.csv"
    df.to_csv(file_path, index=False)

    with pytest.raises(ValueError, match="missing columns"):
        validate_taxi_zones(file_path)


def test_duplicate_location_ids_fail(tmp_path):
    df = pd.DataFrame(
        {
            "LocationID": [1, 1],
            "Borough": ["Manhattan", "Queens"],
            "Zone": ["Zone A", "Zone B"],
            "service_zone": ["Yellow Zone", "Boro Zone"],
        }
    )

    file_path = tmp_path / "taxi_zones.csv"
    df.to_csv(file_path, index=False)

    with pytest.raises(ValueError, match="duplicate identifiers"):
        validate_taxi_zones(file_path)


def test_missing_location_id_fails(tmp_path):
    df = pd.DataFrame(
        {
            "LocationID": [1, None],
            "Borough": ["Manhattan", "Queens"],
            "Zone": ["Zone A", "Zone B"],
            "service_zone": ["Yellow Zone", "Boro Zone"],
        }
    )

    file_path = tmp_path / "taxi_zones.csv"
    df.to_csv(file_path, index=False)

    with pytest.raises(ValueError, match="missing values"):
        validate_taxi_zones(file_path)
