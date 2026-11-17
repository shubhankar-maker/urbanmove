from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from urbanmove.storage.s3_client import S3Storage
from urbanmove.ingestion.taxi_trips import (
    download_taxi_trips,
    validate_taxi_trips_file,
)


def ingest_taxi_trips():
    file_path = download_taxi_trips(year=2025, month=1)
    print(f"Taxi trips ingestion completed: {file_path}")
    return str(file_path)


def validate_taxi_trips_task(ti):
    file_path = ti.xcom_pull(task_ids="ingest_taxi_trips")

    report = validate_taxi_trips_file(Path(file_path))

    print(f"Taxi trips validation report: {report}")

def upload_taxi_trips_to_s3(ti):
    file_path = ti.xcom_pull(task_ids="ingest_taxi_trips")

    storage = S3Storage()

    s3_key = f"raw/taxi_trips/{Path(file_path).name}"

    s3_uri = storage.upload_file(
        local_path=file_path,
        s3_key=s3_key,
    )

    print(f"Uploaded taxi trips data to: {s3_uri}")

    return s3_uri

default_args = {
    "owner": "urbanmove",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="urbanmove_taxi_trips",
    description="Ingest and validate NYC Yellow Taxi trip data",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["urbanmove", "ingestion"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_taxi_trips",
        python_callable=ingest_taxi_trips,
        execution_timeout=timedelta(minutes=30),
    )

    validate_task = PythonOperator(
        task_id="validate_taxi_trips",
        python_callable=validate_taxi_trips_task,
        execution_timeout=timedelta(minutes=30),
    )

    upload_task = PythonOperator(
        task_id="upload_taxi_trips_to_s3",
        python_callable=upload_taxi_trips_to_s3,
        execution_timeout=timedelta(minutes=30),
    )

    ingest_task >> validate_task >> upload_task