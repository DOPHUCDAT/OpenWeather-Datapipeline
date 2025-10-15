import sys
sys.path.append('/opt/airflow/api-request')
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
from insert_record import main

default_args = {
    'description': 'Orchestrator DAG for Weather API',
    'start_date': datetime(2025, 8, 4),
    'catchup': False,
}

dag = DAG(
    dag_id='weather_api_dbt_orchestrator',
    default_args=default_args,
    schedule=timedelta(minutes=5),
)

with dag:
    task_1 = PythonOperator(
        task_id='ingest_data_task',
        python_callable=main,
    )
    
    task_2 = DockerOperator(
        task_id='transform_data_task',
        image = 'ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command='run',
        working_dir='/usr/app',
        mounts=[
            Mount(
                source='/home/datdp/repos/Weather_API/dbt/Weatherstack',
                target='/usr/app',
                type='bind'
            ),
            Mount(
                source= '/home/datdp/repos/Weather_API/dbt/profiles.yml',
                target= '/root/.dbt/profiles.yml',
                type= 'bind'
            )
        ],
        network_mode='weather_api_weather_network',
        docker_url='unix://var/run/docker.sock',
        auto_remove='success'
    )
    task_1 >> task_2