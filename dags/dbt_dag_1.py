from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from dags.module.utils.airbyte import trigger_sync, wait_for_job
from dags.module.utils.iceberg import create_iceberg_table_if_not_exists
from module.config_loader import load_env
from custom_operator.dbt_operator import DbtCoreOperator
from airflow.utils.email import send_email

# Load config từ .env
config = load_env()

# Default DAG args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["phongtrantien294@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Connection ID Airbyte (lấy từ UI Airbyte)
AIRBYTE_CONNECTION_ID = config["airbyte"]["connection_id"]

# Table name trên Iceberg
ICEBERG_TABLE_NAME = "customer"  # Tùy chỉnh theo connection bạn ETL

# Hàm trigger Airbyte job
def run_airbyte_sync(**context):
    print(f"[Airflow] Trigger Airbyte sync for connection {AIRBYTE_CONNECTION_ID}")
    job_id = trigger_sync(AIRBYTE_CONNECTION_ID)
    context["ti"].xcom_push(key="airbyte_job_id", value=job_id)
    print(f"[Airflow] Triggered job {job_id}")
    return job_id


# Hàm chờ Airbyte hoàn tất
def wait_for_airbyte_sync(**context):
    job_id = context["ti"].xcom_pull(task_ids="trigger_airbyte", key="airbyte_job_id")
    print(f"[Airflow] Waiting for Airbyte job {job_id}")
    wait_for_job(job_id)
    print("[Airflow] Airbyte job completed successfully.")


# Hàm tạo bảng Iceberg sau khi sync
def create_iceberg_table(**context):
    print(f"[Airflow] Creating Iceberg table: {ICEBERG_TABLE_NAME}")
    create_iceberg_table_if_not_exists(ICEBERG_TABLE_NAME, auto_schema=True)


# DAG định nghĩa
with DAG(
    "airbyte_to_iceberg_pipeline",
    default_args=default_args,
    description="ETL pipeline: Airbyte → MinIO (raw avro) → Iceberg → dbt",
    schedule_interval=None,
    start_date=datetime(2025, 10, 9),
    catchup=False,
    tags=["airbyte", "iceberg", "minio", "dbt"],
) as dag:

    trigger_airbyte = PythonOperator(
        task_id="trigger_airbyte",
        python_callable=run_airbyte_sync,
        provide_context=True,
    )

    wait_for_completion = PythonOperator(
        task_id="wait_for_complete",
        python_callable=wait_for_airbyte_sync,
        provide_context=True,
    )

    create_iceberg = PythonOperator(
        task_id="create_iceberg_table",
        python_callable=create_iceberg_table,
        provide_context=True,
    )

    DBT_PROJECT_DIR = config["dbt"]["project_dir"]
    DBT_PROFILES_DIR = config["dbt"]["profiles_dir"]

    dbt_run = DbtCoreOperator(
        task_id="dbt_run",
        dbt_command="run",
        dbt_project_dir=DBT_PROJECT_DIR,
        dbt_profiles_dir=DBT_PROFILES_DIR,
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

    # pipeline: Airbyte → wait → Iceberg
    trigger_airbyte >> wait_for_completion >> create_iceberg >> dbt_run >> dbt_test
