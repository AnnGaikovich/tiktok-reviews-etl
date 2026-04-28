from datetime import datetime

BASE_DIR = '/opt/airflow/data'
FILE_PATH = f'{BASE_DIR}/tiktok_google_play_reviews.csv'
TEMP_ORIGINAL = f'{BASE_DIR}/temp_original.csv'
TEMP_FILLED = f'{BASE_DIR}/temp_filled.csv'
TEMP_SORTED = f'{BASE_DIR}/temp_sorted.csv'
FINAL_PATH = f'{BASE_DIR}/processed_tiktok.csv'
FLAG_PATH = f'{BASE_DIR}/dataset_ready.flag'

DATE_COLUMN = 'at'

MONGODB_CONN_ID = 'mongodb_default'
MONGODB_DATABASE = 'tiktok'
MONGODB_COLLECTION = 'reviews'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}