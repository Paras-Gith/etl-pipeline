from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.ingestion import scan_input_folder
from src.error_handler import process_with_retry
from src.repoter import generate_report

default_args = {
    'owner': 'etl_team',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['alerts@yourcompany.com']
}

def run_ingestion(**context):
    files = scan_input_folder()
    context['ti'].xcom_push(key='files', value=files)

def run_etl(**context):
    files = context['ti'].xcom_pull(key='files')
    for f in files:
        process_with_retry(f, table='orders')

def run_reports(**context):
    generate_report()

with DAG(
    dag_id='csv_to_db_etl',
    default_args=default_args,
    schedule='0 6 * * *',  # ✅ fixed param name
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'csv', 'database']
) as dag:

    ingest = PythonOperator(task_id='ingest_files', python_callable=run_ingestion)
    etl = PythonOperator(task_id='run_etl', python_callable=run_etl)
    report = PythonOperator(task_id='gen_reports', python_callable=run_reports)
