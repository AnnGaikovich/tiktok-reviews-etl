from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
import pandas as pd
from pymongo import MongoClient
from airflow.hooks.base import BaseHook
import os

# Import configuration
from config.config import (
    FINAL_PATH, DATE_COLUMN, MONGODB_CONN_ID,
    MONGODB_DATABASE, MONGODB_COLLECTION, default_args
)

def check_file_exists(**context):
    """Check if the processed CSV file exists."""
    return os.path.exists(FINAL_PATH)

def load_csv_to_mongodb(**context):
    """Read processed CSV and load data into MongoDB."""
    # Read CSV
    df = pd.read_csv(FINAL_PATH)
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN])
    records = df.to_dict('records')

    # Get MongoDB connection from Airflow connection
    conn = BaseHook.get_connection(MONGODB_CONN_ID)
    client = MongoClient(host=conn.host, port=conn.port)
    db = client[MONGODB_DATABASE]
    collection = db[MONGODB_COLLECTION]

    # Replace existing data (idempotent run)
    collection.delete_many({})
    if records:
        collection.insert_many(records)
    print(f"Loaded {len(records)} records into MongoDB collection '{MONGODB_COLLECTION}'")
    client.close()

# DAG 
with DAG(
    dag_id='load_to_mongodb',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['tiktok', 'mongodb'],
) as dag:

    wait_for_file = PythonSensor(
        task_id='wait_for_file',
        python_callable=check_file_exists,
        poke_interval=10,
        timeout=600,
        mode='poke',
    )

    load_data = PythonOperator(
        task_id='load_processed_data',
        python_callable=load_csv_to_mongodb,
    )

    wait_for_file >> load_data