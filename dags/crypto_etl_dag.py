from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import pandas as pd
from requests import Session
from dotenv import load_dotenv

# --- CONFIGURATION ---
env_path = '/opt/airflow/dags/.env' 
load_dotenv(env_path)
API_KEY = os.getenv('CMC_API_KEY') 

if not API_KEY:
    raise ValueError("❌ CMC_API_KEY is empty! Airflow cannot find the .env file or the key inside it.")

# --- ETL FUNCTIONS ---

def run_crypto_etl():
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': API_KEY}
    params = {'start':'1', 'limit':'50', 'convert':'USD'}
    
    session = Session()
    session.headers.update(headers)
    response = session.get(url, params=params)
    data = response.json()
    
    if 'data' in data:
        df = pd.json_normalize(data['data'])
        
        # TRANSFORMATION
        df.columns = [c.replace('quote.USD.', '') for c in df.columns]
        cols_to_keep = ['name', 'symbol', 'price', 'market_cap', 'percent_change_24h', 'last_updated']
        final_df = df[cols_to_keep]
        
        # GENERATE UNIQUE FILENAME WITH TIMESTAMP
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"temp_crypto_{timestamp}.csv"
        temp_csv_path = os.path.join('/opt/airflow/dags/', filename)
        
        final_df.to_csv(temp_csv_path, index=False)
        
        # Returning this allows 'ti.xcom_pull' to see the exact filename
        return temp_csv_path
    else:
        raise ValueError(f"API Error: {data.get('status', {}).get('error_message')}")

def load_to_minio(ti):
    # Pull the UNIQUE file path from the previous task
    file_path = ti.xcom_pull(task_ids='extract_and_transform')
    
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    
    # We use the filename only for the S3 Key
    s3_key = f"raw/{os.path.basename(file_path)}"
    
    s3_hook.load_file(
        filename=file_path,
        key=s3_key,
        bucket_name='crypto',
        replace=True
    )

def load_to_postgres(ti):
    # Pull the SAME unique file path
    file_path = ti.xcom_pull(task_ids='extract_and_transform')
    df = pd.read_csv(file_path)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # 'append' ensures your data grows every hour!
    df.to_sql('market_data', engine, if_exists='append', index=False)
    
    # Optional: Delete the local temp file after success to keep the folder clean
    # os.remove(file_path)

# --- DAG DEFINITION ---

default_args = {
    'owner': 'Segun',
    'start_date': datetime(2026, 3, 30), # Updated to current date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'coinmarketcap_full_stack_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_and_transform',
        python_callable=run_crypto_etl
    )

    minio_task = PythonOperator(
        task_id='load_to_minio',
        python_callable=load_to_minio
    )

    postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    # Parallel loading to Lake (MinIO) and Warehouse (Postgres)
    extract_task >> [minio_task, postgres_task]
