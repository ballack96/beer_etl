from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import duckdb

from include.etl.extract import fetch_hops_data


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Path to DuckDB file
DUCKDB_PATH = os.path.join(os.path.dirname(__file__), "../data/hops.duckdb")
TABLE_NAME = "hops"

def extract_transform_load():
    print("🔄 Extracting hop data...")
    df = fetch_hops_data()

    if df.empty:
        raise ValueError("❌ DataFrame is empty. Extraction failed or site changed.")

    print(f"✅ Extracted {len(df)} rows.")

    # Optional: Replace empty strings with None
    df.replace("", None, inplace=True)

    # Optional: Convert column names to snake_case
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    # Load into DuckDB
    con = duckdb.connect(DB_PATH)
    con.execute(f"CREATE OR REPLACE TABLE {TABLE_NAME} AS SELECT * FROM df")
    con.close()

    print(f"✅ Loaded data into DuckDB table: {TABLE_NAME}")


with DAG(
    dag_id="hop_etl_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["hops", "etl", "duckdb"],
) as dag:

    etl_task = PythonOperator(
        task_id="extract_transform_load_hops",
        python_callable=extract_transform_load,
    )

    etl_task