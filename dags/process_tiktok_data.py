from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import re
import os

# Import configuration
from config.config import (
    FILE_PATH, TEMP_ORIGINAL, TEMP_FILLED, TEMP_SORTED, FINAL_PATH,
    DATE_COLUMN, default_args, FLAG_PATH
)

@dag(
    dag_id='process_tiktok_data',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['tiktok', 'processing'],
)
def process_tiktok_data():

    @task.sensor(poke_interval=10, timeout=600, mode='poke')
    def wait_for_file():
        """Sensor that waits for the input CSV file to exist."""
        return os.path.exists(FILE_PATH)

    @task.branch
    def check_file_empty():
        """Branch: if file empty -> log_empty, else proceed to processing group."""
        if os.path.getsize(FILE_PATH) == 0:
            return 'log_empty_file'
        try:
            df_sample = pd.read_csv(FILE_PATH, nrows=1)
            if df_sample.empty:
                return 'log_empty_file'
        except:
            return 'log_empty_file'
        return 'process_data_group.read_csv'

    log_empty = BashOperator(
        task_id='log_empty_file',
        bash_command='echo "File is empty. Processing stopped."',
    )

    @task_group
    def process_data_group():
        @task
        def read_csv():
            df = pd.read_csv(FILE_PATH)
            df.to_csv(TEMP_ORIGINAL, index=False)
            return TEMP_ORIGINAL

        @task
        def replace_null_values(original_path: str):
            df = pd.read_csv(original_path)
            cols_to_fill = [col for col in df.columns if col != DATE_COLUMN]
            df[cols_to_fill] = df[cols_to_fill].fillna('-')
            df = df.dropna(subset=[DATE_COLUMN])
            df = df[df[DATE_COLUMN] != '-']
            df.to_csv(TEMP_FILLED, index=False)
            return TEMP_FILLED

        @task
        def sort_by_created_date(filled_path: str):
            df = pd.read_csv(filled_path)
            df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], errors='coerce')
            df = df.dropna(subset=[DATE_COLUMN])
            df = df.sort_values(DATE_COLUMN)
            df.to_csv(TEMP_SORTED, index=False)
            return TEMP_SORTED

        @task
        def clean_content(sorted_path: str):
            df = pd.read_csv(sorted_path)
            def clean_text(text):
                if not isinstance(text, str):
                    return text
                cleaned = re.sub(r'[^\w\s\.\,\!\?\:\;\-\'\"\(\)]', '', text)
                return cleaned
            df['content'] = df['content'].apply(clean_text)
            df.to_csv(FINAL_PATH, index=False)
         
            for f in [TEMP_ORIGINAL, TEMP_FILLED, TEMP_SORTED]:
                if os.path.exists(f):
                    os.remove(f)
            return FINAL_PATH

        original = read_csv()
        filled = replace_null_values(original)
        sorted_df = sort_by_created_date(filled)
        clean_content(sorted_df)

    mark_dataset_ready = BashOperator(
        task_id='mark_dataset_ready',
        bash_command=f'echo "Processed" > {FLAG_PATH}',
    )

    # Dependencies
    wait_sensor = wait_for_file()
    branch = check_file_empty()
    wait_sensor >> branch
    branch >> log_empty
    branch >> process_data_group() >> mark_dataset_ready

# Instantiate the DAG
dag = process_tiktok_data()