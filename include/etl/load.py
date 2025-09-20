import json
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime
import os
from airflow.models import Variable

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

##################################################################
## Load recipe list from  https://www.brewersfriend.com         ##
##################################################################
def load_clone_recipes_to_duckdb(df, table_name="clone_recipes"):
    """
    Pushes the cleaned clone-recipes DataFrame into DuckDB Cloud (MotherDuck).
    """
    # 1) Grab your MotherDuck token from the Airflow UI
    token = Variable.get("MOTHERDUCK_TOKEN")
    os.environ["MOTHERDUCK_TOKEN"] = token

    # 2) Connect via the md: alias and write
    con = duckdb.connect("md:beer_etl")
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.register("df", df)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    con.close()

    print(f"✅ Loaded {len(df)} rows into '{table_name}' on MotherDuck")

######################################################
## Load styles data from BJCP                       ##
######################################################
def load_styles_to_duckdb(df, table_name="beer_styles"):
    """
    Loads the transformed styles DataFrame into DuckDB Cloud via MotherDuck.
    Uses the MOTHERDUCK_TOKEN stored in Airflow Variables.
    """
    # fetch token from Airflow UI
    token = Variable.get("MOTHERDUCK_TOKEN")
    os.environ["MOTHERDUCK_TOKEN"] = token

    # connect via the md: alias
    con = duckdb.connect("md:beer_etl")

    # drop & recreate
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.register("df_styles", df)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_styles")
    con.close()
    print(f"✅ Loaded {len(df)} rows into '{table_name}' on MotherDuck")

######################################################
## Load hop data from https://beermaverick.com      ##
######################################################

def load_hops_to_duckdb(df, table_name="hops"):
    """
    Loads DataFrame into DuckDB after dropping existing table.
    """
    # Inject token from Airflow variable into env
    token = Variable.get("MOTHERDUCK_TOKEN")
    os.environ["MOTHERDUCK_TOKEN"] = token

    # Use DuckDB cloud URI
    con = duckdb.connect("md:beer_etl")  # or any other db name
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.register("df", df)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    con.close()

    print(f"✅ Loaded {len(df)} rows into fresh '{table_name}' table at {con}")



###############################################################
## Load fermentables data from https://beermaverick.com      ##
###############################################################
def load_fermentables_to_duckdb(df, table_name="fermentables"):
    """
    Loads DataFrame into DuckDB after dropping existing table.
    """
    # Inject token from Airflow variable into env
    token = Variable.get("MOTHERDUCK_TOKEN")
    os.environ["MOTHERDUCK_TOKEN"] = token

    # Use DuckDB cloud URI
    con = duckdb.connect("md:beer_etl")  # or any other db name
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.register("df", df)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    con.close()
    
    print(f"✅ Loaded {len(df)} rows into fresh '{table_name}' table at {con}")

#########################################################
## Load yeasts data from https://beermaverick.com      ##
#########################################################
def load_yeasts_to_duckdb(df, table_name="yeasts"):
    """
    Loads DataFrame into DuckDB after dropping existing table.
    """
    # Inject token from Airflow variable into env
    token = Variable.get("MOTHERDUCK_TOKEN")
    os.environ["MOTHERDUCK_TOKEN"] = token

    # Use DuckDB cloud URI
    con = duckdb.connect("md:beer_etl")  # or any other db name
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.register("df", df)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    con.close()
    
    print(f"✅ Loaded {len(df)} rows into fresh '{table_name}' table at {con}")


#########################################################
## Load BeerXML recipe data to DuckDB Cloud           ##
#########################################################
def load_beerxml_to_duckdb(df, table_name="beerxml_recipes"):
    """
    Loads the transformed BeerXML recipe DataFrame into DuckDB Cloud via MotherDuck.
    Uses the MOTHERDUCK_TOKEN stored in Airflow Variables.
    
    Args:
        df: Transformed BeerXML recipe DataFrame
        table_name: Name of the table to create in DuckDB
    """
    # Inject token from Airflow variable into env
    token = Variable.get("MOTHERDUCK_TOKEN")
    os.environ["MOTHERDUCK_TOKEN"] = token

    # Use DuckDB cloud URI
    con = duckdb.connect("md:beer_etl")
    
    # Drop existing table if it exists
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    # Register DataFrame and create table
    con.register("df_beerxml", df)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_beerxml")
    
    # Create indexes for better query performance
    try:
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_recipe_name ON {table_name}(recipe_name)")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_brewer ON {table_name}(brewer)")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_style_name ON {table_name}(style_name)")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_recipe_type ON {table_name}(recipe_type)")
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_created_year ON {table_name}(created_year)")
        print(f"✅ Created indexes for {table_name}")
    except Exception as e:
        print(f"⚠️ Could not create indexes for {table_name}: {e}")
    
    con.close()
    
    print(f"✅ Loaded {len(df)} BeerXML recipe rows into fresh '{table_name}' table on MotherDuck")
