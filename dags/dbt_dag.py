from airflow import DAG
from datetime import datetime, timedelta
from custom_operator.dbt_operator import DbtCoreOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_pipeline',
    default_args=default_args,
    description='DBT pipeline for trino/iceberg',
    schedule_interval='@daily',
    start_date=datetime(2025, 10, 6),
    catchup=False,
    tags=['dbt'],
) as dag:

    DBT_PROJECT_DIR = '/opt/airflow/dags/dbt_trino'
    DBT_PROFILES_DIR = DBT_PROJECT_DIR

    dbt_run = DbtCoreOperator(
        task_id="dbt_run",
        dbt_command="run",
        dbt_project_dir="/opt/airflow/dags/dbt_trino",
        dbt_profiles_dir="/opt/airflow/dags/dbt_trino",
        target="dev",
       # select="staging",
        full_refresh=True,
    )

    dbt_test = DbtCoreOperator(
        task_id='dbt_test',
        dbt_command='test',
        dbt_project_dir=DBT_PROJECT_DIR,
        dbt_profiles_dir=DBT_PROFILES_DIR,
        target="dev",
    )

    dbt_run >> dbt_test

