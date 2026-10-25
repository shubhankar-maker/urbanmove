
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path


PROJECT_DIR = Path("/opt/airflow/dags")


def validate_project():
    required_dirs = [
        PROJECT_DIR,
    ]

    for directory in required_dirs:
        if not directory.exists():
            raise FileNotFoundError(
                f"Required directory does not exist: {directory}"
            )

    print("UrbanMove project folder validation passed.")


def report_success():
    print("UrbanMove first Airflow workflow completed successfully.")


with DAG(
    dag_id="urbanmove_first_dag",
    start_date=datetime(2026, 10, 25),
    schedule=None,
    catchup=False,
    tags=["urbanmove", "learning"],
) as dag:

    validate_task = PythonOperator(
        task_id="validate_project_folders",
        python_callable=validate_project,
    )

    success_task = PythonOperator(
        task_id="report_success",
        python_callable=report_success,
    )

    validate_task >> success_task
