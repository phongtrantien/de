import logging

from airflow.exceptions import AirflowException

from dags.module.env.java_spark_env import detect_and_set_java_home, detect_and_set_spark_home
from dags.module.normalize.extract_time import extract_timestamp_key
from dags.module.spark.spark_session_builder import get_spark_session
from dags.module.connector.writer import write_to_iceberg
import pandas as pd
import pyarrow.parquet as pq
from dags.module.config_loader import load_env
import s3fs

def main(table_name: str):
    detect_and_set_java_home()
    detect_and_set_spark_home()

    config = load_env()
    host = config["trino"]["host"]
    port = int(config["trino"]["port"])
    user = config["trino"]["user"]
    catalog = config["connector"]["catalog"]
    schema = config["connector"]["schema"]
    fmt = config["connector"]["format"]
    bucket = config["s3"]["bucket"]
    endpoint = config["s3"]["endpoint"]
    batch_size = int(config["size_config"]["batch_size"])
    log_table = config["connector"]["log_table"]
    metadata_table = config["connector"]["metadata_table"]
    source_path = f"s3://{bucket}/raw/{table_name}/"
    target_path = f"s3://{bucket}/connector/{schema}/{table_name}/"
    spark_master = config["spark_config"]["master_url"]
    deploy_mode = config["spark_config"]["deploy_mode"]

    logging.info(f"[Airflow] Processing table: {table_name}")
    logging.info(f"[S3] Reading from: {source_path}")
    spark = get_spark_session(config)

    try:
        fs = s3fs.S3FileSystem(
            key=config["s3"]["access_key"],
            secret=config["s3"]["secret_key"],
            client_kwargs={"endpoint_url": endpoint}
        )
    except Exception as e:
        raise AirflowException(f"[S3] Failed to create S3 client: {e}")

    files = fs.glob(f"{bucket}/raw/{table_name}/*.parquet")
    if not files:
        raise AirflowException(f"[S3] No Parquet files found for {table_name}")

    latest_file = sorted(files, key=extract_timestamp_key, reverse=True)[0]
    logging.info(f"[S3] Found latest parquet file for {table_name}: {latest_file}")

    dataset = pq.ParquetDataset([f"s3://{latest_file}"], filesystem=fs)
    df = dataset.read_pandas().to_pandas()


    full_table_name = f"{config['connector']['catalog']}.{config['connector']['schema']}.{table_name}"
    log_table = f"{config['connector']['catalog']}.{config['connector']['schema']}.{config['connector']['log_table']}"

    write_to_iceberg(spark, df, full_table_name, log_table)

    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
    main("customer")
