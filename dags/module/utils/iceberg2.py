import re
from datetime import date

from ..env.config_loader import load_env
from ..env.java_spark_env import detect_and_set_java_home, detect_and_set_spark_home
from ..logger.logger_table import write_log
from ..logger.logger_util import get_logger
from dags.module.normalize.auto_ddl import IcebergDDLFromTmpView
from dags.module.connector.connector import minio_connector

from pyspark.sql import functions as F

logger = get_logger("iceberg_loader")
config = load_env()


def _ensure_namespace(spark, catalog: str, schema: str):

    namespace = f"{catalog}.{schema}"
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        logger.info(f"Ensured namespace exists: {namespace}")
    except Exception as e_ns:
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
            logger.info(f"Ensured schema exists (fallback): {namespace}")
        except Exception:
            logger.warning(f"Namespace/Schema creation skipped: {e_ns}")


def _create_table_from_tmp_view(
    spark, full_table_name: str,
    tmp_view: str,
    catalog: str,
    schema: str,
    df,
    tblprops: dict | None = None
):
    _ensure_namespace(spark, catalog, schema)

    try:
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
        if not ddl:
            raise RuntimeError("DDL builder unavailable: build_from_tmp_view() returned None.")
        logger.info(f"Generated DDL for table {full_table_name}: {ddl}")
        spark.sql(ddl)
        logger.info(f"Created table via DDL builder: {full_table_name}")
    except Exception as e_ddl:
        logger.exception(f"Fallback DDL creation failed: {e_ddl}")
        raise




def create_iceberg_table_if_not_exists(table: str, spark, config: dict):

    detect_and_set_java_home()
    detect_and_set_spark_home()
    minio_connector(spark, config)
    catalog = config["iceberg"]["catalog"]
    schema = config["iceberg"]["schema"]
    bucket = config["s3"]["bucket"]
    log_table = config["iceberg"]["log_table"]

    safe_table_name = re.sub(r"[^a-zA-Z0-9_]", "_", table)
    full_table_name = f"{catalog}.{schema}.{safe_table_name}"
    log_table_full = f"{catalog}.{schema}.{log_table}"
    source_path = f"s3a://{bucket}/raw/VBI_DATA/{table}/"

    logger.info(f"Processing table: {table}")
    logger.info(f"Reading from: {source_path}")
    logger.info(f"Iceberg target: {full_table_name}")

    try:
        spark.conf.set("spark.sql.shuffle.partitions", "200")
        spark.conf.set("spark.sql.adaptive.enabled", "true")
    except Exception as e:
        logger.warning(f"Cannot set Spark conf: {e}")

    try:
        df_spark = (
            spark.read.format("parquet")
            .option("recursiveFileLookup", "true")
            .load(source_path)
        )

        _ = df_spark.limit(1).count()
        logger.info("MinIO S3A read test OK.")
    except Exception as e_read:
        logger.exception(f"Failed to read parquet via Spark from {source_path}: {e_read}")
        raise

    selected_cols = [c for c in df_spark.columns if not c.startswith("_")]
    df_spark = df_spark.select(*selected_cols)
    logger.info(f"Columns after filtering underscores: {selected_cols}")

    try:
        for c, dtype in df_spark.dtypes:
            if dtype == "timestamp":
                df_spark = df_spark.withColumn(c, F.to_utc_timestamp(F.col(c), "UTC"))
    except Exception as e_ts:
        logger.warning(f"Timestamp normalization skipped: {e_ts}")

    try:
        current_partitions = df_spark.rdd.getNumPartitions()
        target_partitions = max(1, min(200, current_partitions))
        df_spark = df_spark.coalesce(target_partitions)
        logger.info(f"Coalesced partitions: {current_partitions} -> {target_partitions}")
    except Exception as e_part:
        logger.warning(f"Partition coalesce skipped: {e_part}")

    etl_date = date.today().strftime("%Y%m%d")
    try:
        check_q = f"""
            SELECT 1 FROM {log_table_full}
            WHERE etl_date = '{etl_date}'
              AND table_name = '{full_table_name}'
              AND status = 'SUCCESS'
            LIMIT 1
        """
        result = spark.sql(check_q)
        if result.count() > 0:
            logger.info(f"Skip writing — already SUCCESS for {full_table_name} on {etl_date}")
            return
    except Exception as e_check:
        logger.warning(f"Log check skipped: {e_check}")

    count_rows = None
    try:

        try:
            count_rows = df_spark.count()
            logger.info(f"Row count before write: {count_rows}")
        except Exception as e_cnt:
            logger.warning(f"Counting rows failed: {e_cnt}")
            count_rows = 0

        try:
            df_spark.writeTo(full_table_name).append()

        except Exception as e_write:
            msg = str(e_write)

            if ("TABLE_OR_VIEW_NOT_FOUND" in msg) or ("NoSuchTable" in msg):
                logger.warning("Table not found, attempting to create from tmp view schema...")
                tmp_view = f"tmp_{safe_table_name.lower()}"
                df_spark.limit(0).createOrReplaceTempView(tmp_view)

                _create_table_from_tmp_view(
                    spark,
                    full_table_name,
                    tmp_view,
                    catalog,
                    schema,
                    df_spark,
                    tblprops={
                        "write.parquet.compression-codec": "zstd",
                        "gc.enabled": "false",
                    },
                )
                df_spark.writeTo(full_table_name).append()

            elif "NoSuchNamespaceException" in msg:
                logger.warning(f"Namespace {catalog}.{schema} not found, creating...")
                tmp_view = f"tmp_{safe_table_name.lower()}"
                df_spark.limit(0).createOrReplaceTempView(tmp_view)

                _create_table_from_tmp_view(
                    spark,
                    full_table_name,
                    tmp_view,
                    catalog,
                    schema,
                    df_spark,
                    tblprops={
                        "write.parquet.compression-codec": "zstd",
                        "gc.enabled": "false",
                    },
                )
                df_spark.writeTo(full_table_name).append()

            else:
                raise

        logger.info(f"Successfully inserted {count_rows} rows into {full_table_name}")

        try:
            write_log(spark, log_table_full, full_table_name, count_rows, "SUCCESS")
        except Exception as e_log:
            logger.warning(f"Write log SUCCESS failed: {e_log}")

    except Exception as err_message:
        logger.exception(f"Failed during write: {err_message}")
        try:
            write_log(spark, log_table_full, full_table_name, count_rows or 0, "FAILED", str(err_message))
        except Exception as inner_log_err:
            logger.warning(f"Failed to log error: {inner_log_err}")
        raise

    finally:
        try:
            spark.stop()
        except Exception:
            logger.warning("spark.stop() raised an exception (ignored).")