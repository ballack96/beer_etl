from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import numpy as np

# Saved modules
from include.etl.extract import fetch_beerxml_from_s3
from include.etl.flatten import flatten_beerxml_to_json
from include.etl.load_mongodb import load_beerxml_to_mongodb


def clean_data_for_xcom(data):
    """
    Clean data to ensure it's JSON serializable for XCom.
    
    Args:
        data: DataFrame or list of dictionaries
    
    Returns:
        Cleaned data that can be serialized to JSON
    """
    if isinstance(data, pd.DataFrame):
        # Convert DataFrame to list of dictionaries
        data_dict = data.to_dict(orient='records')
    else:
        data_dict = data
    
    cleaned_data = []
    for item in data_dict:
        cleaned_item = {}
        for key, value in item.items():
            # Handle different data types
            if isinstance(value, (list, tuple)):
                # Handle lists and tuples
                cleaned_item[key] = clean_list_for_xcom(value)
            elif isinstance(value, dict):
                # Handle nested dictionaries
                cleaned_item[key] = clean_dict_for_xcom(value)
            elif isinstance(value, (np.ndarray, list)) and len(value) == 0:
                cleaned_item[key] = []
            elif isinstance(value, (np.integer, np.floating)):
                cleaned_item[key] = value.item()  # Convert numpy types to Python types
            elif isinstance(value, pd.Timestamp):
                cleaned_item[key] = value.isoformat() if pd.notna(value) else None
            elif isinstance(value, str):
                cleaned_item[key] = value
            elif value is None:
                cleaned_item[key] = None
            else:
                # Try to handle pandas NaN/NaT values
                try:
                    if pd.isna(value) or value is pd.NaT:
                        cleaned_item[key] = None
                    else:
                        cleaned_item[key] = value
                except (ValueError, TypeError):
                    # If pd.isna fails (e.g., with arrays), just use the value as-is
                    cleaned_item[key] = value
        cleaned_data.append(cleaned_item)
    
    return cleaned_data


def clean_list_for_xcom(lst):
    """Clean a list for JSON serialization."""
    cleaned_list = []
    for item in lst:
        if isinstance(item, dict):
            cleaned_list.append(clean_dict_for_xcom(item))
        elif isinstance(item, (np.integer, np.floating)):
            cleaned_list.append(item.item())
        elif isinstance(item, pd.Timestamp):
            cleaned_list.append(item.isoformat() if pd.notna(item) else None)
        elif isinstance(item, str):
            cleaned_list.append(item)
        elif item is None:
            cleaned_list.append(None)
        else:
            try:
                if pd.isna(item) or item is pd.NaT:
                    cleaned_list.append(None)
                else:
                    cleaned_list.append(item)
            except (ValueError, TypeError):
                cleaned_list.append(item)
    return cleaned_list


def clean_dict_for_xcom(dct):
    """Clean a dictionary for JSON serialization."""
    cleaned_dict = {}
    for key, value in dct.items():
        if isinstance(value, dict):
            cleaned_dict[key] = clean_dict_for_xcom(value)
        elif isinstance(value, (list, tuple)):
            cleaned_dict[key] = clean_list_for_xcom(value)
        elif isinstance(value, (np.integer, np.floating)):
            cleaned_dict[key] = value.item()
        elif isinstance(value, pd.Timestamp):
            cleaned_dict[key] = value.isoformat() if pd.notna(value) else None
        elif isinstance(value, str):
            cleaned_dict[key] = value
        elif value is None:
            cleaned_dict[key] = None
        else:
            try:
                if pd.isna(value) or value is pd.NaT:
                    cleaned_dict[key] = None
                else:
                    cleaned_dict[key] = value
            except (ValueError, TypeError):
                cleaned_dict[key] = value
    return cleaned_dict

def extract_beerxml(**context):
    """
    Extract BeerXML recipe data from S3 bucket.
    """
    # Get S3 configuration from Airflow Variables
    bucket_name = Variable.get("BEERXML_S3_BUCKET", default_var="beer-etl")
    prefix = Variable.get("BEERXML_S3_PREFIX", default_var="raw-clone-recipes/clone-recipes/")
    connection_id = Variable.get("BEERXML_AWS_CONNECTION_ID", default_var="aws-s3-user")
    
    print(f"🔄 Extracting BeerXML recipes from S3 bucket: {bucket_name}, prefix: {prefix}")
    print(f"🔗 Using AWS connection: {connection_id}")
    
    try:
        # Fetch BeerXML data from S3
        recipes_data = fetch_beerxml_from_s3(
            bucket_name=bucket_name, 
            prefix=prefix, 
            connection_id=connection_id
        )
        
        if not recipes_data:
            raise ValueError("❌ No BeerXML recipe data extracted from S3 bucket")
        
        print(f"✅ Extracted {len(recipes_data)} BeerXML recipes from S3")
        
        # Clean the raw data for JSON serialization
        cleaned_recipes_data = clean_data_for_xcom(recipes_data)
        context['ti'].xcom_push(key='raw_beerxml_recipes', value=cleaned_recipes_data)
        
    except Exception as e:
        print(f"❌ Error extracting BeerXML data: {str(e)}")
        raise


def flatten_beerxml(**context):
    """
    Flatten raw BeerXML recipe data into JSON format for MongoDB.
    """
    print("🧪 Flattening BeerXML recipe data to JSON format...")
    
    try:
        # Pull raw data from previous task
        raw_recipes = context['ti'].xcom_pull(task_ids='extract_beerxml', key='raw_beerxml_recipes')
        
        if not raw_recipes:
            raise ValueError("❌ No raw BeerXML data found in XCom")
        
        print(f"📊 Processing {len(raw_recipes)} raw BeerXML recipes")
        
        # Get organization ID from Airflow Variables
        org_id = Variable.get("BREWLYTIX_ORG_ID", default_var="brewlytix")
        
        # Flatten each recipe to JSON format
        flattened_recipes = []
        for recipe in raw_recipes:
            try:
                # Extract XML content and file key from the recipe data
                xml_content = recipe.get('xml_content', '')
                file_key = recipe.get('file_key', '')
                
                if xml_content and file_key:
                    # Flatten the BeerXML to JSON format
                    flattened_recipe = flatten_beerxml_to_json(xml_content, file_key, org_id)
                    if flattened_recipe:
                        flattened_recipes.extend(flattened_recipe)
                else:
                    print(f"⚠️ Skipping recipe with missing XML content or file key")
            except Exception as e:
                print(f"⚠️ Error flattening recipe {recipe.get('file_key', 'unknown')}: {str(e)}")
                continue
        
        print(f"✅ Flattened {len(flattened_recipes)} BeerXML recipes to JSON format")
        print(f"📈 Flattening summary:")
        print(f"   - Total recipes processed: {len(flattened_recipes)}")
        
        # Count recipe types
        recipe_types = {}
        for recipe in flattened_recipes:
            recipe_type = recipe.get('type', 'Unknown')
            recipe_types[recipe_type] = recipe_types.get(recipe_type, 0) + 1
        
        print(f"   - Recipe types: {recipe_types}")
        
        # Clean the flattened data for JSON serialization
        cleaned_flattened_data = clean_data_for_xcom(flattened_recipes)
        
        # Push flattened data to XCom
        context['ti'].xcom_push(key='flattened_beerxml_recipes', value=cleaned_flattened_data)
        
    except Exception as e:
        print(f"❌ Error flattening BeerXML data: {str(e)}")
        raise


def load_beerxml(**context):
    """
    Load flattened BeerXML recipe data to MongoDB.
    """
    print("💾 Loading BeerXML recipes to MongoDB...")
    
    try:
        # Pull flattened data from previous task
        flattened_recipes = context['ti'].xcom_pull(task_ids='flatten_beerxml', key='flattened_beerxml_recipes')
        
        if not flattened_recipes:
            raise ValueError("❌ No flattened BeerXML data found in XCom")
        
        # Get MongoDB configuration from Airflow Variables
        database_name = Variable.get("BREWLYTIX_DB_NAME", default_var="brewlytix")
        collection_name = Variable.get("BREWLYTIX_COLLECTION_NAME", default_var="recipes")
        connection_id = Variable.get("BREWLYTIX_MONGODB_CONNECTION_ID", default_var="brewlytix-mongodb")
        
        # Load to MongoDB
        load_beerxml_to_mongodb(
            recipes_data=flattened_recipes,
            database_name=database_name,
            collection_name=collection_name,
            connection_id=connection_id
        )
        
        print("✅ Successfully loaded BeerXML recipes to MongoDB")
        
        # Print summary statistics
        print(f"📊 Load Summary:")
        print(f"   - Total recipes loaded: {len(flattened_recipes)}")
        print(f"   - Database: {database_name}")
        print(f"   - Collection: {collection_name}")
        
        # Count unique values
        unique_brewers = len(set(recipe.get('brewer', '') for recipe in flattened_recipes if recipe.get('brewer')))
        unique_styles = len(set(recipe.get('style', {}).get('name', '') for recipe in flattened_recipes if recipe.get('style', {}).get('name')))
        
        print(f"   - Unique brewers: {unique_brewers}")
        print(f"   - Unique styles: {unique_styles}")
        
        # Count recipe types
        recipe_types = {}
        for recipe in flattened_recipes:
            recipe_type = recipe.get('type', 'Unknown')
            recipe_types[recipe_type] = recipe_types.get(recipe_type, 0) + 1
        
        print(f"   - Recipe types: {recipe_types}")
        
    except Exception as e:
        print(f"❌ Error loading BeerXML data: {str(e)}")
        raise


# Define the DAG
with DAG(
    dag_id="beerxml_etl_dag",
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 1, 1),
        'retries': 1,
        'retry_delay': 300,  # 5 minutes
    },
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["beerxml", "etl", "s3", "motherduck", "recipes"],
    description="ETL pipeline for BeerXML recipe files stored in S3",
) as dag:

    # Extract task
    extract_task = PythonOperator(
        task_id="extract_beerxml",
        python_callable=extract_beerxml,
        provide_context=True,
    )

    # Flatten task
    flatten_task = PythonOperator(
        task_id="flatten_beerxml",
        python_callable=flatten_beerxml,
        provide_context=True,
    )

    # Load task
    load_task = PythonOperator(
        task_id="load_beerxml",
        python_callable=load_beerxml,
        provide_context=True,
    )

    # Define task dependencies
    extract_task >> flatten_task >> load_task
