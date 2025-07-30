import json
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime
import os

def load_to_duckdb():
    """Load transformed beer data to DuckDB"""
    # Fixed path - removed "beer_etl_project/" prefix
    processed_path = Path("include/data/processed")
    json_files = sorted(processed_path.glob("beer_styles_transformed_*.json"), reverse=True)

    if not json_files:
        print("No transformed data files found.")
        return

    latest_file = json_files[0]
    with open(latest_file, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # Save as Parquet - fixed path
    parquet_file = processed_path / latest_file.with_suffix(".parquet").name
    df.to_parquet(parquet_file, index=False)

    # Register in DuckDB - fixed path
    db_path = processed_path / "beer_styles.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(f"""
        CREATE OR REPLACE TABLE beer_styles AS
        SELECT * FROM read_parquet('{parquet_file}')
    """)
    con.close()

    print(f"Loaded {len(df)} records into DuckDB and saved Parquet at {parquet_file}")


# Keep original function from your current file for compatibility
def load_to_duckdb_original():
    processed_path = Path("beer_etl_project/include/data/processed")
    json_files = sorted(processed_path.glob("beer_styles_transformed_*.json"), reverse=True)

    if not json_files:
        print("No transformed data files found.")
        return

    latest_file = json_files[0]
    with open(latest_file, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # Save as Parquet
    parquet_file = processed_path / latest_file.with_suffix(".parquet").name
    df.to_parquet(parquet_file, index=False)

    # Register in DuckDB
    db_path = processed_path / "beer_styles.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(f"""
        CREATE OR REPLACE TABLE beer_styles AS
        SELECT * FROM read_parquet('{parquet_file}')
    """)
    con.close()

    print(f"Loaded {len(df)} records into DuckDB and saved Parquet at {parquet_file}")


######################################################
## Load hop data from https://beermaverick.com      ##
######################################################
def load_hops_to_duckdb(df, table_name="hops"):
    """
    Loads DataFrame into DuckDB after dropping existing table.
    """
    db_path = os.path.abspath(os.path.join(os.getcwd(), "include/data/ingredients.duckdb"))

    # Ensure target directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    con = duckdb.connect(db_path)

    # Drop the table if it exists
    con.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Recreate and load data
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

    con.close()
    print(f"✅ Loaded {len(df)} rows into fresh '{table_name}' table at {db_path}")


######################################################
## Load hop data from https://beermaverick.com      ##
######################################################
def load_fermentables_to_duckdb(df, table_name="fermentables"):
    """
    Loads DataFrame into DuckDB after dropping existing table.
    """
    db_path = os.path.abspath(os.path.join(os.getcwd(), "include/data/ingredients.duckdb"))

    # Ensure target directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    con = duckdb.connect(db_path)

    # Drop the table if it exists
    con.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Recreate and load data
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

    con.close()
    print(f"✅ Loaded {len(df)} rows into fresh '{table_name}' table at {db_path}")

#########################################################
## Load yeasts data from https://beermaverick.com      ##
#########################################################
def load_yeasts_to_duckdb(df, table_name="yeasts"):
    """
    Loads DataFrame into DuckDB after dropping existing table.
    """
    db_path = os.path.abspath(os.path.join(os.getcwd(), "include/data/ingredients.duckdb"))

    # Ensure target directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    con = duckdb.connect(db_path)

    # Drop the table if it exists
    con.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Recreate and load data
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

    con.close()
    print(f"✅ Loaded {len(df)} rows into fresh '{table_name}' table at {db_path}")