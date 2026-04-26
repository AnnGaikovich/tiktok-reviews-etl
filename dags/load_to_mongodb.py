from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from airflow.hooks.base import BaseHook
import os

PROCESSED_CSV_PATH = '/opt/airflow/data/processed_tiktok.csv'
DATE_COLUMN = 'at'      # должна совпадать с именем в первом DAG

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

def check_file_exists(**context):
    return os.path.exists(PROCESSED_CSV_PATH)

def load_csv_to_mongodb(**context):
    df = pd.read_csv(PROCESSED_CSV_PATH)
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN])
    records = df.to_dict('records')

    # Получаем соединение, настроенное в Airflow UI
    conn = BaseHook.get_connection('mongodb_default')
    client = MongoClient(host=conn.host, port=conn.port)
    db = client[conn.schema]          # имя базы данных из Connection
    collection = db['reviews']

    collection.delete_many({})        # очищаем перед загрузкой
    if records:
        collection.insert_many(records)
    print(f"Загружено {len(records)} записей в MongoDB")
    client.close()

with DAG(
    dag_id='load_to_mongodb',
    default_args=default_args,
    schedule=None,          # больше не ждёт Dataset, а запускается вручную или по триггеру
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