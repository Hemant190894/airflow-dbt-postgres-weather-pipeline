import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount

sys.path.append('/opt/airflow/api_request')

try:
    from insert_data import final_main
except ImportError as e:
    print(f"Failed to import insert_data_to_db: {e}")

default_args = {
    'description': 'A DAG to orchestrate weather data insertion into the database',
    'start_date': datetime(2025, 8, 28),
    'catchup': False,
}

# The Python callable is now correctly defined to use the imported function
def insert_weather_data_callable():
    final_main()

dag = DAG(
    dag_id="weather_data_orchestrator",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1440),
    tags=["weather_data", "api_request"],
    max_active_runs=1,
    catchup=False,
)

with dag:
    task_insert_data = PythonOperator(
        task_id="insert_weather_data_orchestrator",
        python_callable=insert_weather_data_callable,
    )

    # Run dbt build (staging then mart) using DockerOperator
    # Host path to the dbt project on your machine
    HOST_DBT_PATH = "/Users/hemantdayma/Desktop/Automated_Pipeline_Weather_API/weather_data/dbt"

    task_dbt_build = DockerOperator(
        task_id="dbt_build_orchestrator",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
        api_version="auto",
        auto_remove=True,
        command="build --full-refresh --select stg_weather_data weather_analytics",
        docker_url="unix://var/run/docker.sock",
        network_mode="weather_data_default",
        mount_tmp_dir=False,
        mounts=[
            Mount(target="/opt/dbt", source=HOST_DBT_PATH, type="bind", read_only=False)
        ],
        # Provide profiles dir and DB credentials required by profiles.yml
        environment={
            "DBT_PROFILES_DIR": "/opt/dbt/profiles",
            "DB_USER": os.environ.get("DB_USER", ""),
            "DB_PASSWORD": os.environ.get("DB_PASSWORD", ""),
            "DB_NAME": os.environ.get("DB_NAME", ""),
        },
        working_dir="/opt/dbt",
        tty=True,
        dag=dag,
    )

    task_insert_data >> task_dbt_build

