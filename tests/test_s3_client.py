from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from urbanmove.storage.s3_client import S3Storage


@pytest.fixture
def mock_s3(monkeypatch):
    mock_client = MagicMock()
    mock_session = MagicMock()
    mock_session.client.return_value = mock_client

    mock_boto_session = MagicMock(return_value=mock_session)

    monkeypatch.setattr(
        "urbanmove.storage.s3_client.boto3.Session",
        mock_boto_session,
    )

    storage = S3Storage(
        bucket_name="urbanmove-test-bucket",
        profile_name="test-profile",
        region_name="ap-south-1",
    )

    return storage, mock_client


def test_upload_file_success(mock_s3, tmp_path):
    storage, mock_client = mock_s3

    local_file = tmp_path / "test.txt"
    local_file.write_text("UrbanMove test")

    result = storage.upload_file(
        str(local_file),
        "raw/test.txt",
    )

    mock_client.upload_file.assert_called_once_with(
        str(local_file),
        "urbanmove-test-bucket",
        "raw/test.txt",
    )

    assert result == "s3://urbanmove-test-bucket/raw/test.txt"


def test_upload_file_missing_file(mock_s3):
    storage, _ = mock_s3

    with pytest.raises(FileNotFoundError):
        storage.upload_file(
            "/tmp/nonexistent_urbanmove_file.txt",
            "raw/test.txt",
        )


def test_upload_file_empty_key(mock_s3, tmp_path):
    storage, _ = mock_s3

    local_file = tmp_path / "test.txt"
    local_file.write_text("test")

    with pytest.raises(ValueError):
        storage.upload_file(str(local_file), "")


def test_download_file_success(mock_s3, tmp_path):
    storage, mock_client = mock_s3

    destination = tmp_path / "downloads" / "test.txt"

    result = storage.download_file(
        "raw/test.txt",
        str(destination),
    )

    mock_client.download_file.assert_called_once_with(
        "urbanmove-test-bucket",
        "raw/test.txt",
        str(destination),
    )

    assert result == str(destination)


def test_object_exists_true(mock_s3):
    storage, mock_client = mock_s3

    mock_client.head_object.return_value = {"ContentLength": 100}

    assert storage.object_exists("raw/test.txt") is True


def test_object_exists_false(mock_s3):
    storage, mock_client = mock_s3

    mock_client.head_object.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}},
        "HeadObject",
    )

    assert storage.object_exists("raw/missing.txt") is False
