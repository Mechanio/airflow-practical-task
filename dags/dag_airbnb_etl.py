# NOT FULLY DONE
# relative path error
from datetime import datetime, timedelta
import os
from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    'owner': 'mechanio',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}
RAW_PATH = "../raw/AB_NYC_2019.csv"
TRANSFORMED_PATH = "../transformed/transformed_data.csv"


def get_data():
    if os.path.exists(RAW_PATH):
        df = pd.read_csv(RAW_PATH)
    else:
        raise FileNotFoundError(f"{RAW_PATH} does not exist.")


def transform_data():
    df = pd.read_csv(RAW_PATH)
    df = df[df['price'] > 0]
    df['last_review'] = pd.to_datetime(df['last_review'])
    df['last_review'].fillna(df['last_review'].min(), inplace=True)
    df.fillna(value={'name': 'Unknown', 'host_name': 'Unknown', 'reviews_per_month': 0}, inplace=True)  # Fill missing values
    df.dropna(subset=['latitude', 'longitude'], inplace=True)
    df.to_csv(TRANSFORMED_PATH, index=False)


def data_quality_check():
    transformed_df = pd.read_csv(TRANSFORMED_PATH)
    expected_count = len(transformed_df)

    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    records = hook.get_records("SELECT COUNT(*) FROM airbnb_listings;")
    record_count = records[0][0]

    if record_count != expected_count:
        raise ValueError(f"Data quality check failed: Expected {expected_count} records, but found {record_count} records.")
    else:
        print(f"Data quality check passed: Found {record_count} records.")


with DAG(
    dag_id='nyc_airbnb_etl',
    default_args=default_args,
    start_date=datetime(2024, 9, 1),
    schedule_interval='@daily'
) as dag:
    extract_data = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
    )
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )
    load_data_to_postgres = PostgresOperator(
        task_id='load_data_to_postgres',
        postgres_conn_id='postgres_localhost',
        sql="""
        COPY airbnb_listings(name, host_id, host_name, neighbourhood_group, neighbourhood, latitude, longitude, room_type, price, minimum_nights, number_of_reviews, last_review, reviews_per_month, calculated_host_listings_count, availability_365)
        FROM '{{ params.transformed_file_path }}'
        DELIMITER ','
        CSV HEADER;
        """,
        params={'transformed_file_path': TRANSFORMED_PATH}
    )
    quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check
    )

    extract_data >> transform_data >> load_data_to_postgres >> quality_check