from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from urbanmove.ingestion.nyc_311 import (
    download_311_data,
    validate_311_data,
)
from urbanmove.storage.s3_client import S3Storage

START_DATE = "2025-01-01"
END_DATE = "2025-01-02"

default_args = {
    "owner": "urbanmove",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="urbanmove_nyc_311",
    default_args=default_args,
    description="Ingest and validate NYC 311 complaint data",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["urbanmove", "ingestion", "311"],
) as dag:

    def ingest_311_data():
        file_path = download_311_data(START_DATE, END_DATE)
        return str(file_path)

    def validate_311_task(ti):
        file_path = ti.xcom_pull(task_ids="ingest_311_data")
        report = validate_311_data(Path(file_path))
        print(f"NYC 311 validation report: {report}")
        return report

    def upload_311_to_s3(ti):
        file_path = ti.xcom_pull(task_ids="ingest_311_data")

        storage = S3Storage()

        s3_key = f"raw/nyc_311/{Path(file_path).name}"

        s3_uri = storage.upload_file(
            local_path=file_path,
            s3_key=s3_key,
        )

        print(f"Uploaded NYC 311 data to: {s3_uri}")

        return s3_uri

    ingest_task = PythonOperator(
        task_id="ingest_311_data",
        python_callable=ingest_311_data,
        execution_timeout=timedelta(minutes=30),
    )

    validate_task = PythonOperator(
        task_id="validate_311_data",
        python_callable=validate_311_task,
        execution_timeout=timedelta(minutes=10),
    )

    upload_task = PythonOperator(
        task_id="upload_311_to_s3",
        python_callable=upload_311_to_s3,
        execution_timeout=timedelta(minutes=10),
    )

    ingest_task >> validate_task >> upload_task