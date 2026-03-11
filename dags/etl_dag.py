
import sys
import os
from datetime import datetime, timedelta
import logging
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_DIR)


from etl.transform import transform
from etl.load import load


TMP_DIR = "/tmp/etl_pipeline"
RAW_BACKUP_DIR = "/opt/airflow/backups/raw"

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(RAW_BACKUP_DIR, exist_ok=True)

API_URL = "http://host.docker.internal:5000/sales/csv"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def timestamped_file(prefix, folder=TMP_DIR):
    """Generate a timestamped CSV filename"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(folder, f"{prefix}_{ts}.csv")


def extract_task(**kwargs):
    logging.info("🔹 Starting Extract Phase")
    
    # Read CSV from API
    df = pd.read_csv(API_URL)
    
    # Save permanent raw backup
    raw_file = timestamped_file("raw", RAW_BACKUP_DIR)
    df.to_csv(raw_file, index=False)
    logging.info(f"✅ Raw backup saved: {raw_file}")
    
    # Save tmp CSV for downstream tasks
    extract_file = timestamped_file("extracted")
    df.to_csv(extract_file, index=False)
    logging.info(f"✅ Extract Phase completed. Rows: {len(df)}. Saved to {extract_file}")
    
    kwargs['ti'].xcom_push(key='extract_file', value=extract_file)
    return extract_file

def transform_task(**kwargs):
    logging.info("🔹 Starting Transform Phase")
    ti = kwargs['ti']
    extract_file = ti.xcom_pull(key='extract_file', task_ids='extract_data')
    
    df = pd.read_csv(extract_file)
    df_transformed = transform(df)
    
    transform_file = timestamped_file("transformed")
    df_transformed.to_csv(transform_file, index=False)
    logging.info(f"✅ Transform Phase completed. Rows: {len(df_transformed)}. Saved to {transform_file}")
    
    ti.xcom_push(key='transform_file', value=transform_file)
    return transform_file

def load_task(**kwargs):
    logging.info("🔹 Starting Load Phase")
    ti = kwargs['ti']
    transform_file = ti.xcom_pull(key='transform_file', task_ids='transform_data')
    
    df = pd.read_csv(transform_file)
    load(df)
    logging.info("✅ Load Phase completed. Database updated.")


with DAG(
    'sales_etl_pipeline_full',
    default_args=default_args,
    description='ETL pipeline with separate phases, raw backup, and timestamped CSVs',
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_task,
        provide_context=True
    )

    t2_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_task,
        provide_context=True
    )

    t3_load = PythonOperator(
        task_id='load_data',
        python_callable=load_task,
        provide_context=True
    )

    # Task dependencies
    t1_extract >> t2_transform >> t3_load