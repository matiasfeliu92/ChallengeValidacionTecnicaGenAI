from datetime import datetime, timedelta
import os
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from src.scripts import ExtractData, LoadData

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_and_load():
    "Extract json data from file in data/raw and load it into the PostgreSQL database"
    extract_data = ExtractData()
    data_extracted = extract_data.extract()
    load_data = LoadData()
    load_data.load(data_extracted)

with DAG(
    'ChallengeValidacionTecnicaGenAI',
    default_args=default_args,
    description='This process was created, for extract, load and transform data of json logs created by an application of documents',
    schedule_interval=timedelta(days=1),
    start_date=datetime.now(),
    tags=['Challenge'],
) as dag:
    extract_and_load_data = PythonOperator(
        task_id="extract_data", 
        python_callable=extract_and_load
    )
    # transform_with_DBT = BashOperator(
    #     task_id="transform_with_DBT",
    #     bash_command="cd /opt/airflow/european_leagues_DBT && dbt run --select stg_football_data int_team_match_performance --profiles-dir /home/airflow/.dbt",
    #     dag=dag,
    # )

    extract_and_load_data ##>> transform_with_DBT