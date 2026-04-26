from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import pandas as pd
import re
import os

# ---------- Константы ----------
FILE_PATH = '/opt/airflow/data/tiktok_google_play_reviews.csv'
TEMP_ORIGINAL = '/opt/airflow/data/temp_original.csv'
TEMP_FILLED = '/opt/airflow/data/temp_filled.csv'
TEMP_SORTED = '/opt/airflow/data/temp_sorted.csv'
FINAL_PATH = '/opt/airflow/data/processed_tiktok.csv'
DATE_COLUMN = 'at'          # название колонки с датой

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

# ---------- Функции для сенсора и ветвления ----------
def check_file_exists(**context):
    return os.path.exists(FILE_PATH)

def check_file_empty(**context):
    file_path = context['templates_dict']['file_path']
    if os.path.getsize(file_path) == 0:
        return 'log_empty_file'
    try:
        df_sample = pd.read_csv(file_path, nrows=1)
        if df_sample.empty:
            return 'log_empty_file'
    except:
        return 'log_empty_file'
    return 'process_data_group.read_csv'

# ---------- Функции обработки ----------
def read_csv(**context):
    df = pd.read_csv(FILE_PATH)
    df.to_csv(TEMP_ORIGINAL, index=False)
    context['ti'].xcom_push(key='original_path', value=TEMP_ORIGINAL)

def replace_null_values(**context):
    df = pd.read_csv(TEMP_ORIGINAL)
    # Заменяем null на '-' во всех колонках, КРОМЕ колонки с датой
    cols_to_fill = [col for col in df.columns if col != DATE_COLUMN]
    df[cols_to_fill] = df[cols_to_fill].fillna('-')
    # Удаляем строки, где дата отсутствует или равна прочерку (если замена затронула)
    df = df.dropna(subset=[DATE_COLUMN])
    df = df[df[DATE_COLUMN] != '-']
    df.to_csv(TEMP_FILLED, index=False)
    context['ti'].xcom_push(key='filled_path', value=TEMP_FILLED)

def sort_by_created_date(**context):
    df = pd.read_csv(TEMP_FILLED)
    # Парсим дату, невалидные станут NaT
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], errors='coerce')
    df = df.dropna(subset=[DATE_COLUMN])
    df = df.sort_values(DATE_COLUMN)
    df.to_csv(TEMP_SORTED, index=False)
    context['ti'].xcom_push(key='sorted_path', value=TEMP_SORTED)

def clean_content_column(**context):
    df = pd.read_csv(TEMP_SORTED)
    def clean_text(text):
        if not isinstance(text, str):
            return text
        # Оставляем буквы, цифры, пробелы и базовую пунктуацию
        cleaned = re.sub(r'[^\w\s\.\,\!\?\:\;\-\'\"\(\)]', '', text)
        return cleaned
    df['content'] = df['content'].apply(clean_text)
    df.to_csv(FINAL_PATH, index=False)
    # Удаляем временные файлы
    for f in [TEMP_ORIGINAL, TEMP_FILLED, TEMP_SORTED]:
        if os.path.exists(f):
            os.remove(f)
    context['ti'].xcom_push(key='final_path', value=FINAL_PATH)

# ---------- DAG ----------
with DAG(
    dag_id='process_tiktok_data',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['tiktok', 'processing'],
) as dag:

    wait_for_file = PythonSensor(
        task_id='wait_for_file',
        python_callable=check_file_exists,
        poke_interval=10,
        timeout=600,
        mode='poke',
    )

    branch_task = BranchPythonOperator(
        task_id='check_file_empty',
        python_callable=check_file_empty,
        templates_dict={'file_path': FILE_PATH},
    )

    log_empty = BashOperator(
        task_id='log_empty_file',
        bash_command='echo "Файл пуст. Обработка остановлена."',
    )

    with TaskGroup(group_id='process_data_group') as process_group:
        read_csv_task = PythonOperator(
            task_id='read_csv',
            python_callable=read_csv,
        )
        replace_nulls = PythonOperator(
            task_id='replace_null_values',
            python_callable=replace_null_values,
        )
        sort_data = PythonOperator(
            task_id='sort_by_created_date',
            python_callable=sort_by_created_date,
        )
        clean_content = PythonOperator(
            task_id='clean_content_column',
            python_callable=clean_content_column,
        )
        read_csv_task >> replace_nulls >> sort_data >> clean_content

    mark_dataset_ready = BashOperator(
        task_id='mark_dataset_ready',
        bash_command='echo "Processed" > /opt/airflow/data/dataset_ready.flag',
    )

    wait_for_file >> branch_task
    branch_task >> log_empty
    branch_task >> process_group >> mark_dataset_ready