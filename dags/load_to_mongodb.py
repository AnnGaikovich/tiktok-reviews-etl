from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd
import os

# Import configuration
from config.config import (
    FINAL_PATH, DATE_COLUMN, MONGODB_CONN_ID,
    MONGODB_DATABASE, MONGODB_COLLECTION, default_args
)

def check_file_exists(**context):
    return os.path.exists(FINAL_PATH)

def load_csv_to_mongodb(**context):
    # Read CSV
    df = pd.read_csv(FINAL_PATH)
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN])
    records = df.to_dict('records')

    # Use MongoHook to get collection
    hook = MongoHook(mongo_conn_id=MONGODB_CONN_ID)
    collection = hook.get_collection(MONGODB_COLLECTION, MONGODB_DATABASE)

    # Replace existing data (idempotent run)
    collection.delete_many({})
    if records:
        collection.insert_many(records)
    print(f"Loaded {len(records)} records into MongoDB collection '{MONGODB_COLLECTION}'")

# DAG definition
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