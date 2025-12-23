from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime, timedelta
import requests
import json
import os
import logging

BUCKET_NAME = "openair-nature"

# Großer Feldberg route
LAT = "50.23" 
LON = "8.45"

def extract_air_quality_data():
    API_KEY = os.getenv('OPENWEATHER_API_KEY')

    logging.info(f"DEBUG: API_KEY value -> {API_KEY}")
    
    if not API_KEY:
        logging.error("API_KEY COULD NOT FOUND!")
        raise Exception("API_KEY is not defined in the system.")

    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={LAT}&lon={LON}&appid={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API error: {response.status_code}")

def upload_to_gcs(ti):
    data = ti.xcom_pull(task_ids='extract_data')
    
    hook = GCSHook(gcp_conn_id='google_cloud_default')
    
    file_name = f"raw/air_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=file_name,
        data=json.dumps(data),
        mime_type='application/json'
    )
    print(f"Data successfully loaded: {file_name}")

def check_air_quality(ti):
    data = ti.xcom_pull(task_ids='extract_data')
    aqi = data['list'][0]['main']['aqi']
    return aqi >= 3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'openair_nature_extract_to_gcs',
    default_args=default_args,
    description='Loads air quality data to GCS bucket',
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_air_quality_data
    )

    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id='load_to_bq',
        bucket=BUCKET_NAME,
        source_objects=['raw/*.json'],
        destination_project_dataset_table='openair-nature-pipeline.openair_nature_data.taunus_air_quality',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_APPEND',
        autodetect=True,
        ignore_unknown_values=True,
        max_bad_records=10,
        dag=dag,
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            'dbt run --project-dir /opt/airflow/dbt_project '
            '--profiles-dir /opt/airflow/dbt_project'
        ),
        dag=dag,
    )

    check_aqi_threshold = ShortCircuitOperator(
        task_id='check_aqi_threshold',
        python_callable=check_air_quality,
        dag=dag,
    )

    send_alert_email = EmailOperator(
        task_id='send_alert_email',
        to='eylemgokdemir05@example.com',
        subject='WARNING: Taunus Mountains air quality warning!',
        html_content="""
            <h3>Air quality at critical level!</h3>
            <p>The latest AQI value measured in the Taunus region exceeded the threshold limit.</p>
            <p>You can check the latest data from the Looker Studio dashboard.</p>
        """,
        dag=dag,
    )

    extract_task >> upload_task >> load_to_bq >> dbt_run >> check_aqi_threshold >> send_alert_email