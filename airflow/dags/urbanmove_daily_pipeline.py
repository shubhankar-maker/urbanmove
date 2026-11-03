from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)

default_args = {
    "owner": "urbanmove",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="urbanmove_daily_pipeline",
    default_args=default_args,
    description="Master pipeline coordinating all UrbanMove ingestion DAGs",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["urbanmove", "master", "orchestration"],
) as dag:

    taxi_zones = TriggerDagRunOperator(
        task_id="trigger_taxi_zones",
        trigger_dag_id="urbanmove_taxi_zones",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    taxi_trips = TriggerDagRunOperator(
        task_id="trigger_taxi_trips",
        trigger_dag_id="urbanmove_taxi_trips",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    weather = TriggerDagRunOperator(
        task_id="trigger_weather",
        trigger_dag_id="urbanmove_weather",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    nyc_311 = TriggerDagRunOperator(
        task_id="trigger_nyc_311",
        trigger_dag_id="urbanmove_nyc_311",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    [
        taxi_zones,
        taxi_trips,
        weather,
        nyc_311,
    ]