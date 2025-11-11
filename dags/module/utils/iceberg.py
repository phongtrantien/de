import math
from datetime import datetime, date, timezone, time as dtime
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.fs as pfs
#from airflow.exceptions import AirflowException
from ..env.config_loader import load_env
from .schema_detech import extract_schema_from_parquet
from ..env.java_spark_env import detect_and_set_java_home, detect_and_set_spark_home
from ..logger.logger_table import write_log
from ..connector.spark_session_builder import build_spark_session
from ..normalize.format_sql import format_value_for_sql
from ..normalize.extract_time import extract_timestamp_key
from ..connector.connector import get_trino_connection, get_s3_filesystem
from ..logger.logger_util import get_logger
from dags.module.normalize.auto_ddl import IcebergDDLFromTmpView

logger = get_logger("iceberg_loader")

config = load_env()

def create_iceberg_table_if_not_exists(table: str, spark, config: dict):
    detect_and_set_java_home()
    detect_and_set_spark_home()
    catalog = config["iceberg"]["catalog"]
    schema = config["iceberg"]["schema"]
    bucket = config["s3"]["bucket"]
    batch_size = int(config["size_config"]["batch_size"])
    log_table = config["iceberg"]["log_table"]
    source_path = f"s3://{bucket}/raw/VBI_DATA/{table}/"
    full_table_name = f"{catalog}.{schema}.{table}"
    log_table = f"{catalog}.{schema}.{log_table}"
    table_name = table
    logger.info(f"Processing table: {table}")
    logger.info(f"Reading from: {source_path}")

    fs = get_s3_filesystem()
    prefix = f"{bucket}/raw/VBI_DATA/{table}/"
    file_info = fs.get_file_info(pfs.FileSelector(prefix, recursive=True))

    files = [f.path for f in file_info if f.is_file and f.path.endswith(".parquet")]

    if not files:
        raise RuntimeError(f"No Parquet files found for {table}")

    file_paths = [f"s3://{file}" for file in files]
    logger.info(f"Found {len(file_paths)} parquet files.")

    try:
        dataset = ds.dataset(files, format="parquet", filesystem=fs)
        table = dataset.to_table()
        df = table.to_pandas()
        df = df[[c for c in df.columns if isinstance(c, str) and not c.startswith("_")]]
        logger.info(f"Final DataFrame columns: {df.columns.tolist()}")
        logger.info(f"Successfully read {len(df)} rows from {len(file_paths)} parquet files.")
    except Exception as e:
        logger.error(f"Failed to read dataset from files: {e}")
        df = pd.DataFrame()

    if df is not None and not df.empty:
        cols = [c for c in df.columns if isinstance(c, str) and not c.startswith("_")]
        df = df[cols]
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
        df = pd.DataFrame()
        logger.info("No data to normalize timestamps — skipping.")

    conn = get_trino_connection()
    cur = conn.cursor()

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

            # spark.sql(f"TRUNCATE TABLE {full_table_name}")
            # logger.info(f"TRUNCATED TABLE: {full_table_name}"
            import re

            safe_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
            full_table_name = f"{catalog}.{schema}.{safe_table_name}"
            table_location = f"s3a://lake-house/{schema}/{safe_table_name}"
            tmp_view = f"tmp_{safe_table_name.lower()}"


            for i in range(0, count_rows, batch_size):
                chunk = df.iloc[i:i + batch_size]
                df_spark = spark.createDataFrame(chunk)
                logger.info(f"Writing chunk {i // batch_size + 1}/{total_batches}")

                try:
                    df_spark.writeTo(full_table_name).append()
                except Exception as e:
                    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                        logger.warning("Table not found, creating...")
                        df_spark.createOrReplaceTempView(tmp_view)
                        builder = IcebergDDLFromTmpView(
                            iceberg_full_table_name=full_table_name,
                            tmp_view_name=tmp_view,
                            format_version="2",
                            extra_tblproperties={
                                "write.parquet.compression-codec": "zstd",
                                "gc.enabled": "false"
                            },
                            select_columns_from_schema=True,
                            sanitize_column_names=True
                        )

                        ddl = builder.build_from_dataframe(df)
                        logger.info(f"Generated DDL for table {full_table_name}: {ddl}")

                        spark.sql(ddl)
                        logger.info(f"Created new table: {full_table_name}")
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