from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from urbanmove.ingestion.weather import (
    validate_date_range,
    download_weather,
    validate_weather,
)


def ingest_weather():
    start_date = "2025-01-01"
    end_date = "2025-01-02"

    validate_date_range(start_date, end_date)

    file_path = download_weather(start_date, end_date)

    print(f"Weather ingestion completed: {file_path}")
    return str(file_path)


def validate_weather_task(ti):
    file_path = ti.xcom_pull(task_ids="ingest_weather")

    report = validate_weather(Path(file_path))

    print(f"Weather validation report: {report}")


default_args = {
    "owner": "urbanmove",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="urbanmove_weather",
    description="Ingest and validate historical weather data",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["urbanmove", "ingestion", "weather"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_weather",
        python_callable=ingest_weather,
        execution_timeout=timedelta(minutes=15),
    )

    validate_task = PythonOperator(
        task_id="validate_weather",
        python_callable=validate_weather_task,
        execution_timeout=timedelta(minutes=10),
    )

    ingest_task >> validate_task