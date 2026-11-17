import logging
import os
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger(__name__)


class S3Storage:
    """Reusable S3 operations for UrbanMove."""

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        profile_name: Optional[str] = None,
        region_name: Optional[str] = None,
    ):
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET_NAME")
        profile_name = profile_name or os.getenv("AWS_PROFILE")
        region_name = region_name or os.getenv("AWS_REGION", "ap-south-1")

        if not self.bucket_name:
            raise ValueError("S3_BUCKET_NAME is not configured")

        session = boto3.Session(
            profile_name=profile_name,
            region_name=region_name,
        )
        self.client = session.client("s3")

    def upload_file(self, local_path: str, s3_key: str) -> str:
        """Upload a local file to S3."""

        file_path = Path(local_path)

        if not file_path.is_file():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        if not s3_key:
            raise ValueError("S3 object key cannot be empty")

        try:
            self.client.upload_file(
                str(file_path),
                self.bucket_name,
                s3_key,
            )
            logger.info("Uploaded %s to S3 key %s", file_path, s3_key)

        except ClientError:
            logger.exception("Failed to upload %s", file_path)
            raise

        return f"s3://{self.bucket_name}/{s3_key}"

    def download_file(self, s3_key: str, local_path: str) -> str:
        """Download an S3 object to a local path."""

        if not s3_key:
            raise ValueError("S3 object key cannot be empty")

        destination = Path(local_path)
        destination.parent.mkdir(parents=True, exist_ok=True)

        try:
            self.client.download_file(
                self.bucket_name,
                s3_key,
                str(destination),
            )
            logger.info("Downloaded S3 key %s to %s", s3_key, destination)

        except ClientError:
            logger.exception("Failed to download S3 key %s", s3_key)
            raise

        return str(destination)

    def object_exists(self, s3_key: str) -> bool:
        """Check whether an S3 object exists."""

        if not s3_key:
            raise ValueError("S3 object key cannot be empty")

        try:
            self.client.head_object(
                Bucket=self.bucket_name,
                Key=s3_key,
            )
            return True

        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code")

            if error_code in ("404", "NoSuchKey", "NotFound"):
                return False

            logger.exception("Failed to check S3 key %s", s3_key)
            raise