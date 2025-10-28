
import logging
import math
from datetime import datetime, date, timezone, time as dtime
import os
import pandas as pd
import numpy as np
import pyarrow.parquet as pq

from airflow.exceptions import AirflowException
from module.config_loader import load_env
from module.utils.schema_detect import extract_schema_from_parquet
from dags.module.env.java_spark_env import detect_and_set_java_home, detect_and_set_spark_home
from dags.module.logger.logger_table import write_log
from dags.module.spark.spark_session_builder import build_spark_session
from dags.module.normalize.format_sql import format_value_for_sql
from dags.module.normalize.extract_time import extract_timestamp_key
from dags.module.connector.connector import get_trino_connection, get_s3_filesystem
from dags.module.logger.logger_util import get_logger

logger = get_logger("iceberg_loader")

config = load_env()

def create_iceberg_table_if_not_exists(table: str, auto_schema: bool = True, columns: dict = None):
    detect_and_set_java_home()
    detect_and_set_spark_home()
    format_value_for_sql()

    catalog = config["connector"]["catalog"]
    schema = config["connector"]["schema"]
    fmt = config["connector"]["format"]
    bucket = config["s3"]["bucket"]
    endpoint = config["s3"]["endpoint"]
    batch_size = int(config["size_config"]["batch_size"])
    log_table = config["connector"]["log_table"]
    metadata_table = config["connector"]["metadata_table"]
    source_path = f"s3://{bucket}/raw/{table}/"
    target_path = f"s3://{bucket}/connector/{schema}/{table}/"
    spark_master = config["spark_config"]["master_url"]
    deploy_mode = config["spark_config"]["deploy_mode"]

    logger.info(f"Processing table: {table}")
    logger.info(f"Reading from: {source_path}")

    fs = get_s3_filesystem()
    files = fs.glob(f"{bucket}/raw/{table}/*.parquet")
    if not files:
        raise AirflowException(f"No Parquet files found for {table}")

    latest_file = sorted(files, key=extract_timestamp_key, reverse=True)[0]
    logger.info(f"Found latest parquet file: {latest_file}")

    latest_ts = extract_timestamp_key(latest_file)
    if isinstance(latest_ts, str):
        latest_ts = datetime.strptime(latest_ts, "%Y_%m_%d")

    if latest_ts.date() < datetime.now().date():
        logger.warning("Latest file is not up to date — skipping.")
        df = None
    else:
        dataset = pq.ParquetDataset([f"s3://{latest_file}"], filesystem=fs)
        df = dataset.read_pandas().to_pandas()

    if df is not None and not df.empty:
        for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
            try:
                df[col] = pd.to_datetime(df[col], utc=True).dt.tz_convert(None)
            except Exception as e:
                logger.warning(f"Could not normalize timestamp column {col}: {e}")

        for col in df.columns:
            if df[col].dtype == "object":
                sample_val = df[col].dropna().astype(str).head(5)
                if sample_val.str.match(r"\d{4}-\d{2}-\d{2}").any():
                    try:
                        df[col] = pd.to_datetime(df[col], errors="ignore")
                    except Exception:
                        pass
    else:
        logger.info("No data to normalize timestamps — skipping.")

    if df is not None and not df.empty:
        cols = [c for c in df.columns if isinstance(c, str) and not c.startswith("_")]
        df = df[cols]
    else:
        df = pd.DataFrame()

    if auto_schema:
        try:
            logger.info("Detecting schema automatically...")
            columns = extract_schema_from_parquet(bucket, f"raw/{table}/")
            logger.info(f"Extracted columns: {columns}")
        except Exception as e:
            logger.warning(f"Auto schema detection failed: {e}")

    conn = get_trino_connection()
    cur = conn.cursor()
    full_table_name = f"{catalog}.{schema}.{table}"
    log_table = f"{catalog}.{schema}.{log_table}"

    spark = build_spark_session("IcebergWriter", spark_master, deploy_mode, config)

    try:
        if df is not None and not df.empty:
            total_batches = math.ceil(len(df) / batch_size)
            count_rows = len(df)
            etl_date = datetime.now().strftime("%Y%m%d")
            logger.info(f"Start writing {count_rows} rows into {full_table_name} in {total_batches} batches")

            try:
                query = f"""
                    SELECT 1 FROM {log_table}
                    WHERE etl_date = '{etl_date}'
                      AND table_name = '{full_table_name}'
                      AND status = 'SUCCESS'
                    LIMIT 1
                """
                result = spark.sql(query)
                if result.count() > 0:
                    logger.info(f"Skip writing — already SUCCESS for {full_table_name} on {etl_date}")
                    return
            except Exception as e_check:
                logger.warning(f"Log check skipped: {e_check}")

            spark.sql(f"TRUNCATE TABLE {full_table_name}")
            logger.info(f"TRUNCATED TABLE: {full_table_name}")

            for i in range(0, count_rows, batch_size):
                chunk = df.iloc[i:i + batch_size]
                df_spark = spark.createDataFrame(chunk)
                logger.info(f"Writing chunk {i // batch_size + 1}/{total_batches}")

                try:
                    df_spark.writeTo(full_table_name).append()
                except Exception as e:
                    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                        logger.warning("Table not found, creating...")
                        df_spark.writeTo(full_table_name).create()
                    elif "NoSuchNamespaceException" in str(e):
                        namespace = full_table_name.split(".")[1]
                        logger.warning(f"Namespace {namespace} not found, creating...")
                        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
                        df_spark.writeTo(full_table_name).create()
                    else:
                        raise

            logger.info(f"Successfully inserted {count_rows} rows into {full_table_name}")
            write_log(spark, log_table, full_table_name, count_rows, "SUCCESS")
        else:
            logger.info("No data to write — skipping.")

    except Exception as err_message:
        logger.exception(f"Failed during write: {err_message}")
        try:
            write_log(spark, log_table, full_table_name, count_rows, "FAILED", err_message)
        except Exception as inner_log_err:
            logger.warning(f"Failed to log error: {inner_log_err}")
        raise
    finally:
        try:
            spark.stop()
        except Exception:
            logger.warning("spark.stop() raised an exception (ignored).")
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
