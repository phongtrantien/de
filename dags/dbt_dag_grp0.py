from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from module.utils.iceberg import create_iceberg_table_if_not_exists
from module.config_loader import load_env
from dags.module.utils.metadata import get_tables
from dags.module.connector.spark_session_builder import build_spark_session

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
ICEBERG_TABLE_NAMES = config["iceberg"]["list_tables"]
group_id = 0
metadata_table= config["iceberg"]["metadata_table"]
spark_master = config["spark_config"]["master_url"]
deploy_mode = config["spark_config"]["deploy_mode"]

def create_iceberg_tables(**context):

    spark = build_spark_session("IcebergLoadMetadata", spark_master, deploy_mode, config)

    tables = get_tables(spark, metadata_table, group_id)

    for table in tables:
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

    create_iceberg = PythonOperator(
        task_id="create_iceberg_tables",
        python_callable=create_iceberg_tables,
    )


    create_iceberg
