from airflow import DAG
from datetime import timedelta
import pendulum

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from transformation.app import (
    extract_data,
    clean_data,
    load_data
)

default_args = {
    "owner": "mohamed saifi",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="orders_etl_dag",
    description="This DAG orchestrates 3 tasks to extract, clean and load data",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 11, 8, tz="Africa/Casablanca"),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["pyspark", "etl", "data-engineering"]
) as dag:

    start = EmptyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=clean_data
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    start >> extract >> transform >> load