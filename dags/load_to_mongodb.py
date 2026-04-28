from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd
import os

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
    params={
        'input_file': FINAL_PATH,
    }
)
def load_to_mongodb():

    @task.sensor(poke_interval=10, timeout=600, mode='poke')
    def wait_for_file(input_path: str) -> bool:
        """Wait for the processed CSV file."""
        return os.path.exists(input_path)

    @task
    def load_processed_data(input_path: str) -> None:
        """Load CSV into MongoDB."""
        df = pd.read_csv(input_path)
        df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN])
        records = df.to_dict('records')

        hook = MongoHook(mongo_conn_id=MONGODB_CONN_ID)
        collection = hook.get_collection(MONGODB_COLLECTION, MONGODB_DATABASE)

        collection.delete_many({})
        if records:
            collection.insert_many(records)
        print(f"Loaded {len(records)} records into MongoDB collection '{MONGODB_COLLECTION}'")

    input_path = '{{ params.input_file }}'
    wait_for_file(input_path) >> load_processed_data(input_path)

dag = load_to_mongodb()