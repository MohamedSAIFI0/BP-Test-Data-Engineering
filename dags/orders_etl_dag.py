from airflow import DAG
from datetime import datetime, timedelta
import pendulum

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from transformation import (
    extract_data,
    clean_data,
    load_data
)

default_args = {
    "owner":"mohamed saifi",
    "depends_on_past": False,
    "retries":2,
    "retry_delay":timedelta(minutes=2)
}


with DAG(
    dag_id="orders_etl_dag",
    description="this dag orchestrats 3 tasks to extract data from a folder, clean it, and load cleaned data to folder",
    default_args=default_args,
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["pyspark", "etl", "data-engineering"]

) as dag:
    start = EmptyOperator(
        task_id="start"
    )

    extract = PythonOperator(
        task_id = "extract_data",
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id = "transform_data",
        python_callable=clean_data
    )

    load = PythonOperator(
        task_id = "load_data",
        python_callable=load_data
    )


    (
        start
        >> extract 
        >> transform
        >> load
    )

    