import os
import pandas as pd
from requests import Session
from dotenv import load_dotenv

# 1. Setup Paths
script_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(script_dir, '.env')

# 2. Load Environment Variables
load_dotenv(env_path)
API_KEY = os.getenv('CMC_API_KEY')

def extract_and_transform():
    print("--- Starting Pipeline ---")
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': API_KEY}
    params = {'start':'1', 'limit':'50', 'convert':'USD'}
    
    session = Session()
    session.headers.update(headers)
    
    try:
        response = session.get(url, params=params)
        data = response.json()
        
        if 'data' in data:
            # EXTRACTION
            df = pd.json_normalize(data['data'])
            
            # TRANSFORMATION: Rename the nested 'quote.USD' columns
            # This makes the data much easier to work with!
            df.columns = [c.replace('quote.USD.', '') for c in df.columns]
            
            # Select only the most important columns for the final CSV
            cols_to_keep = ['name', 'symbol', 'price', 'market_cap', 'percent_change_24h', 'last_updated']
            final_df = df[cols_to_keep]
            
            print(f"✅ Success! Extracted and Cleaned {len(final_df)} rows.")
            return final_df
        else:
            print(f"❌ API Error: {data.get('status', {}).get('error_message')}")
            return None
            
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return None

if __name__ == "__main__":
    df_clean = extract_and_transform()
    if df_clean is not None:
        # Save to the main folder
        output_path = os.path.join(os.path.dirname(script_dir), 'crypto_market_data.csv')
        df_clean.to_csv(output_path, index=False)
        print(f"🚀 File Saved to: {output_path}")
        
