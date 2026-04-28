from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd
import os

# Import configuration
from config.config import (
    FINAL_PATH, DATE_COLUMN, MONGODB_CONN_ID,
    MONGODB_DATABASE, MONGODB_COLLECTION, default_args
)

@dag(
    dag_id='load_to_mongodb',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['tiktok', 'mongodb'],
)
def load_to_mongodb():

    @task.sensor(poke_interval=10, timeout=600, mode='poke')
    def wait_for_file():
        """Sensor that waits for the processed CSV file to exist."""
        return os.path.exists(FINAL_PATH)

    @task
    def load_processed_data():
        """Read CSV and load into MongoDB using MongoHook."""
        df = pd.read_csv(FINAL_PATH)
        df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN])
        records = df.to_dict('records')

        hook = MongoHook(mongo_conn_id=MONGODB_CONN_ID)
        collection = hook.get_collection(MONGODB_COLLECTION, MONGODB_DATABASE)

        collection.delete_many({})
        if records:
            collection.insert_many(records)
        print(f"Loaded {len(records)} records into MongoDB collection '{MONGODB_COLLECTION}'")

    # Dependencies
    wait_for_file() >> load_processed_data()

# Instantiate the DAG
dag = load_to_mongodb()