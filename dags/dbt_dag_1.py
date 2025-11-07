from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

from module.utils.airbyte import trigger_sync, wait_for_job
from module.utils.iceberg import create_iceberg_table_if_not_exists
from module.config_loader import load_env
from custom_operator.dbt_operator import DbtCoreOperator

config = load_env()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["phongtrantien294@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

AIRBYTE_CONNECTION_ID = config["airbyte"]["connections"]
ICEBERG_TABLE_NAMES = config["connector"]["list_tables"]


# def run_airbyte_sync(**context):
#
#     logging.info(f"[Airflow] Trigger Airbyte sync for connection {AIRBYTE_CONNECTION_ID}")
#     job_id = trigger_sync(AIRBYTE_CONNECTION_ID)
#     context["ti"].xcom_push(key="airbyte_job_id", value=job_id)
#     logging.info(f"[Airflow] Triggered job {job_id}")
#     return job_id
#
#
# def wait_for_airbyte_sync(**context):
#
#     job_id = context["ti"].xcom_pull(task_ids="trigger_airbyte", key="airbyte_job_id")
#     logging.info(f"[Airflow] Waiting for Airbyte job {job_id}")
#     wait_for_job(job_id)
#     logging.info("[Airflow] Airbyte job completed successfully.")


def create_iceberg_tables(**context):

    logging.info(f"[Airflow] Creating Iceberg tables: {ICEBERG_TABLE_NAMES}")
    for table in ICEBERG_TABLE_NAMES:
        try:
            logging.info(f"[Airflow] Creating table: {table}")
            create_iceberg_table_if_not_exists(table, auto_schema=True)
        except Exception as e:
            logging.error(f"[Airflow] Failed to create table {table}: {e}")
            raise


with (DAG(
    "airbyte_to_iceberg_pipeline",
    default_args=default_args,
    description="ETL pipeline: Airbyte → Iceberg → DBT",
    schedule_interval='@daily',
    start_date=datetime(2025, 10, 13),
    catchup=False,
    tags=["airbyte", "connector", "dbt"],
) as dag):

    # trigger_airbyte = PythonOperator(
    #     task_id="trigger_airbyte",
    #     python_callable=run_airbyte_sync,
    # )
    #
    # wait_for_completion = PythonOperator(
    #     task_id="wait_for_completion",
    #     python_callable=wait_for_airbyte_sync,
    # )

    create_iceberg = PythonOperator(
        task_id="create_iceberg_tables",
        python_callable=create_iceberg_tables,
    )


    # DBT_PROJECT_DIR = config["dbt"]["project_dir"]
    # DBT_PROFILES_DIR = config["dbt"]["profiles_dir"]
    #
    # dbt_run = DbtCoreOperator(
    #     task_id="dbt_run",
    #     dbt_command="run",
    #     dbt_project_dir=DBT_PROJECT_DIR,
    #     dbt_profiles_dir=DBT_PROFILES_DIR,
    #     target="dev",
    #     full_refresh=True,
    # )
    #
    # dbt_test = DbtCoreOperator(
    #     task_id="dbt_test",
    #     dbt_command="test",
    #     dbt_project_dir=DBT_PROJECT_DIR,
    #     dbt_profiles_dir=DBT_PROFILES_DIR,
    #     target="dev",
    # )

    # trigger_airbyte >> wait_for_completion >>
    create_iceberg
