import json
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime

def load_to_duckdb():
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
