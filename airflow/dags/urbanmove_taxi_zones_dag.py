
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from urbanmove.ingestion.taxi_zones import (
    download_taxi_zones,
    validate_taxi_zones,
)

from urbanmove.storage.s3_client import S3Storage


def ingest_taxi_zones():
    output_path = download_taxi_zones()

    print(f"Ingestion completed: {output_path}")

    return str(output_path)


def validate_taxi_zones_task(ti):
    file_path = ti.xcom_pull(task_ids="ingest_taxi_zones")

    report = validate_taxi_zones(Path(file_path))

    print(f"Validation report: {report}")


def upload_taxi_zones_to_s3(ti):
    file_path = ti.xcom_pull(task_ids="ingest_taxi_zones")

    storage = S3Storage()

    s3_key = f"raw/taxi_zones/{Path(file_path).name}"

    s3_uri = storage.upload_file(
        local_path=file_path,
        s3_key=s3_key,
    )

    print(f"Uploaded taxi zones data to: {s3_uri}")

    return s3_uri


default_args = {
    "owner": "urbanmove",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="urbanmove_taxi_zones",
    description="Ingest, validate and upload NYC taxi-zone reference data",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["urbanmove", "ingestion", "aws"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_taxi_zones",
        python_callable=ingest_taxi_zones,
        execution_timeout=timedelta(minutes=10),
    )

    validate_task = PythonOperator(
        task_id="validate_taxi_zones",
        python_callable=validate_taxi_zones_task,
        execution_timeout=timedelta(minutes=10),
    )

    upload_task = PythonOperator(
        task_id="upload_taxi_zones_to_s3",
        python_callable=upload_taxi_zones_to_s3,
        execution_timeout=timedelta(minutes=10),
    )

    ingest_task >> validate_task >> upload_task
