from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from urbanmove.ingestion.taxi_zones import (
    download_taxi_zones,
    validate_taxi_zones,
)


def ingest_taxi_zones():
    output_path = download_taxi_zones()

    print(f"Ingestion completed: {output_path}")

    return str(output_path)


def validate_taxi_zones_task(ti):
    file_path = ti.xcom_pull(task_ids="ingest_taxi_zones")

    report = validate_taxi_zones(Path(file_path))

    print(f"Validation report: {report}")


default_args = {
    "owner": "urbanmove",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="urbanmove_taxi_zones",
    description="Ingest and validate NYC taxi-zone reference data",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["urbanmove", "ingestion"],
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

    ingest_task >> validate_task
