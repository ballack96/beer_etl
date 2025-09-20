from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd

# Saved modules
from include.etl.extract import fetch_beerxml_from_s3
from include.etl.transform import transform_beerxml_data
from include.etl.load import load_beerxml_to_duckdb

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
        context['ti'].xcom_push(key='raw_beerxml_recipes', value=recipes_data)
        
    except Exception as e:
        print(f"❌ Error extracting BeerXML data: {str(e)}")
        raise


def transform_beerxml(**context):
    """
    Transform raw BeerXML recipe data into clean, structured format.
    """
    print("🧪 Transforming BeerXML recipe data...")
    
    try:
        # Pull raw data from previous task
        raw_recipes = context['ti'].xcom_pull(task_ids='extract_beerxml', key='raw_beerxml_recipes')
        
        if not raw_recipes:
            raise ValueError("❌ No raw BeerXML data found in XCom")
        
        # Convert to DataFrame
        df = pd.DataFrame(raw_recipes)
        print(f"📊 Loaded {len(df)} raw BeerXML recipes into DataFrame")
        
        # Transform the data
        df_transformed = transform_beerxml_data(df)
        
        print(f"✅ Transformed {len(df_transformed)} BeerXML recipes")
        print(f"📈 Transformation summary:")
        print(f"   - Recipe types: {df_transformed['recipe_type_category'].value_counts().to_dict()}")
        print(f"   - Style compliance score (avg): {df_transformed['style_compliance_score'].mean():.2f}")
        print(f"   - Complexity score (avg): {df_transformed['complexity_score'].mean():.2f}")
        
        # Push transformed data to XCom
        context['ti'].xcom_push(key='transformed_beerxml_recipes', value=df_transformed.to_dict(orient='records'))
        
    except Exception as e:
        print(f"❌ Error transforming BeerXML data: {str(e)}")
        raise


def load_beerxml(**context):
    """
    Load transformed BeerXML recipe data to DuckDB Cloud.
    """
    print("💾 Loading BeerXML recipes to DuckDB...")
    
    try:
        # Pull transformed data from previous task
        transformed_recipes = context['ti'].xcom_pull(task_ids='transform_beerxml', key='transformed_beerxml_recipes')
        
        if not transformed_recipes:
            raise ValueError("❌ No transformed BeerXML data found in XCom")
        
        # Convert to DataFrame
        df = pd.DataFrame(transformed_recipes)
        
        # Load to DuckDB
        load_beerxml_to_duckdb(df, table_name="beerxml_recipes")
        
        print("✅ Successfully loaded BeerXML recipes to DuckDB Cloud")
        
        # Print summary statistics
        print(f"📊 Load Summary:")
        print(f"   - Total recipes loaded: {len(df)}")
        print(f"   - Unique brewers: {df['brewer'].nunique()}")
        print(f"   - Unique styles: {df['style_name'].nunique()}")
        print(f"   - Recipe types: {df['recipe_type_category'].value_counts().to_dict()}")
        
        # Show top brewers by recipe count
        top_brewers = df['brewer'].value_counts().head(5)
        print(f"   - Top brewers by recipe count: {top_brewers.to_dict()}")
        
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

    # Transform task
    transform_task = PythonOperator(
        task_id="transform_beerxml",
        python_callable=transform_beerxml,
        provide_context=True,
    )

    # Load task
    load_task = PythonOperator(
        task_id="load_beerxml",
        python_callable=load_beerxml,
        provide_context=True,
    )

    # Define task dependencies
    extract_task >> transform_task >> load_task
