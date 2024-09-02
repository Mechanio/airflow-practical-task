from datetime import datetime, timedelta
import os
from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator

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
        df.to_csv(TRANSFORMED_PATH, index=False)
    else:
        raise FileNotFoundError(f"{RAW_PATH} does not exist.")


def transform_data():
    df = pd.read_csv(TRANSFORMED_PATH)
    df = df[df['price'] > 0]
    df['last_review'] = pd.to_datetime(df['last_review'])
    df['last_review'].fillna(df['last_review'].min(), inplace=True)
    df.fillna(value={'name': 'Unknown', 'host_name': 'Unknown', 'reviews_per_month': 0}, inplace=True)  # Fill missing values
    df.dropna(subset=['latitude', 'longitude'], inplace=True)
    df.to_csv(TRANSFORMED_PATH, index=False)


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

    extract_data >> transform_data