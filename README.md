Crypto ETL Pipeline with Apache Airflow
This project implements an automated Extract, Transform, Load (ETL) pipeline that pulls real-time cryptocurrency data from the CoinMarketCap API, processes it for analysis, and orchestrates the workflow using Apache Airflow.

🚀 Project Overview
The pipeline is designed to track market data for top cryptocurrencies, ensuring data consistency and providing a clean dataset for downstream analysis or visualization.

📂 Repository Structure
crypto_etl_dag.py: The Airflow DAG that schedules and monitors the ETL tasks.

scripts/Coin_market_api.py: Contains the extraction logic and API interaction.

scripts/coin_market_analysis.py: Handles data cleaning, transformation, and basic analysis.

docker-compose.yaml: (Optional) If you are running Airflow in a containerized environment.

🛠️ Tech Stack
Orchestration: Apache Airflow

Language: Python 3.x

Libraries: Pandas, Requests

Environment: Docker / PostgreSQL

⚙️ Setup & Installation
Clone the Repository:

Bash
git clone https://github.com/your-username/crypto-etl-pipeline.git
Configure API Keys:

Obtain an API Key from CoinMarketCap.

Set it as an environment variable or an Airflow Connection (e.g., CMC_API_KEY).

Run with Airflow:
Place the scripts in your dags/ and scripts/ folders and trigger the crypto_etl_dag.

📊 Pipeline Workflow
Extract: Fetch the latest listings and prices from the CoinMarketCap API.

Transform: Normalize JSON data into a structured format, handle missing values, and calculate price changes.

Load: Store the processed data into a local CSV or a PostgreSQL database.
