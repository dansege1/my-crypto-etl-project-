from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
from requests import Session
from dotenv import load_dotenv

# 1. Load Environment Variables (same logic as before)
script_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(script_dir, '.env')
load_dotenv(env_path)
API_KEY = os.getenv('CMC_API_KEY')

# 2. Define the ETL Function
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
        # Transformation
        df.columns = [c.replace('quote.USD.', '') for c in df.columns]
        cols_to_keep = ['name', 'symbol', 'price', 'market_cap', 'percent_change_24h', 'last_updated']
        final_df = df[cols_to_keep]
        
        # Save to a timestamped file to show versioning
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        output_path = os.path.join(os.path.dirname(script_dir), f'crypto_data_{timestamp}.csv')
        final_df.to_csv(output_path, index=False)
        return f"Successfully saved to {output_path}"
    else:
        raise ValueError(f"API Error: {data.get('status', {}).get('error_message')}")

# 3. Define the DAG
default_args = {
    'owner': 'Segun',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'coinmarketcap_etl_pipeline',
    default_args=default_args,
    description='Automated Crypto ETL from CoinMarketCap API',
    schedule_interval=timedelta(days=1), # Runs once a day
    catchup=False
) as dag:

    etl_task = PythonOperator(
        task_id='extract_transform_load',
        python_callable=run_crypto_etl,
    )

    etl_task
