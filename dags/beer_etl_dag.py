from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from include.etl.extract import extract_beer_data
from include.etl.transform import transform_beer_data
from include.etl.load import load_to_duckdb

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('beer_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_beer_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_beer_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_to_duckdb
    )

    extract_task >> transform_task >> load_task