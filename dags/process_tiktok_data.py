from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
import pandas as pd
import re
import os

# Import configuration (used as defaults)
from config import (
    FILE_PATH, TEMP_ORIGINAL, TEMP_FILLED, TEMP_SORTED, FINAL_PATH,
    DATE_COLUMN, default_args, FLAG_PATH
)

@dag(
    dag_id='process_tiktok_data',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['tiktok', 'processing'],
    params={
        'input_file': FILE_PATH,
        'output_file': FINAL_PATH,
    }
)
def process_tiktok_data():

    @task.sensor(poke_interval=10, timeout=600, mode='poke')
    def wait_for_file(input_path: str) -> bool:
        """Sensor that waits for the input CSV file to exist."""
        return os.path.exists(input_path)

    @task.branch
    def check_file_empty(input_path: str) -> str:
        """Branch: if file empty -> log_empty, else proceed to processing group."""
        if os.path.getsize(input_path) == 0:
            return 'log_empty_file'
        try:
            df_sample = pd.read_csv(input_path, nrows=1)
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
    def process_data_group(input_file: str, output_file: str):
        @task
        def read_csv(input_path: str) -> str:
            """Read original CSV and save to temporary file."""
            df = pd.read_csv(input_path)
            temp_original = TEMP_ORIGINAL  # could be parameterized too
            df.to_csv(temp_original, index=False)
            return temp_original

        @task
        def replace_null_values(original_path: str) -> str:
            """Replace nulls with dash, drop invalid dates."""
            df = pd.read_csv(original_path)
            cols_to_fill = [col for col in df.columns if col != DATE_COLUMN]
            df[cols_to_fill] = df[cols_to_fill].fillna('-')
            df = df.dropna(subset=[DATE_COLUMN])
            df = df[df[DATE_COLUMN] != '-']
            temp_filled = TEMP_FILLED
            df.to_csv(temp_filled, index=False)
            return temp_filled

        @task
        def sort_by_created_date(filled_path: str) -> str:
            """Sort by date column."""
            df = pd.read_csv(filled_path)
            df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], errors='coerce')
            df = df.dropna(subset=[DATE_COLUMN])
            df = df.sort_values(DATE_COLUMN)
            temp_sorted = TEMP_SORTED
            df.to_csv(temp_sorted, index=False)
            return temp_sorted

        @task
        def clean_content(sorted_path: str, final_output: str) -> str:
            """Clean content column and save final CSV."""
            df = pd.read_csv(sorted_path)
            def clean_text(text):
                if not isinstance(text, str):
                    return text
                cleaned = re.sub(r'[^\w\s\.\,\!\?\:\;\-\'\"\(\)]', '', text)
                return cleaned
            df['content'] = df['content'].apply(clean_text)
            df.to_csv(final_output, index=False)

            for f in [TEMP_ORIGINAL, TEMP_FILLED, TEMP_SORTED]:
                if os.path.exists(f):
                    os.remove(f)
            return final_output

        # Pass paths explicitly
        original = read_csv(input_file)
        filled = replace_null_values(original)
        sorted_df = sort_by_created_date(filled)
        clean_content(sorted_df, output_file)

    mark_dataset_ready = BashOperator(
        task_id='mark_dataset_ready',
        bash_command=f'echo "Processed" > {FLAG_PATH}',
    )

    # Dependencies with parameter passing
    input_path = '{{ params.input_file }}'
    output_path = '{{ params.output_file }}'

    wait = wait_for_file(input_path)
    branch = check_file_empty(input_path)
    wait >> branch
    branch >> log_empty
    branch >> process_data_group(input_path, output_path) >> mark_dataset_ready

# Instantiate the DAG
dag = process_tiktok_data()