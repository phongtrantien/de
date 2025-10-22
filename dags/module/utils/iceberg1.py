import logging
import math
import datetime
from uuid import uuid4
import os
import shutil
import importlib.util

import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import s3fs
from trino.dbapi import connect
from airflow.exceptions import AirflowException
from module.config_loader import load_env
from module.utils.schema_detect import extract_schema_from_parquet

config = load_env()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def _detect_and_set_java_home():

    java_home = os.environ.get("JAVA_HOME")
    if java_home and os.path.exists(os.path.join(java_home, "bin", "java")):

        pass
    else:
        java_path = shutil.which("java")
        if java_path:

            real = os.path.realpath(java_path)
            java_home = os.path.dirname(os.path.dirname(real))
        else:

            candidates = [
                "/usr/lib/jvm/java-11-openjdk-amd64",
                "/usr/lib/jvm/java-11-openjdk",
                "/usr/lib/jvm/java-17-openjdk-amd64",
                "/opt/airflow/jdk-11.0.2",
                "/opt/airflow/jdk1.8.0_202",
            ]
            for c in candidates:
                if os.path.exists(os.path.join(c, "bin", "java")):
                    java_home = c
                    break

    if java_home:
        os.environ["JAVA_HOME"] = java_home
        current_path = os.environ.get("PATH", "")
        new_path = os.path.join(java_home, "bin") + ":" + current_path
        os.environ["PATH"] = new_path
        logging.info(f"JAVA_HOME set to {java_home}")
    else:
        logging.warning("JAVA_HOME could not be detected or set. Java may not be available.")


def _detect_and_set_spark_home():

    spark_home = os.environ.get("SPARK_HOME")
    if spark_home and os.path.exists(os.path.join(spark_home, "bin", "spark-submit")):
        logging.info(f"SPARK_HOME already set: {spark_home}")
        return

    spark_submit = shutil.which("spark-submit")
    if spark_submit:

        try:
            spec = importlib.util.find_spec("pyspark")
            if spec and spec.origin:
                # spec.origin might be .../pyspark/__init__.py
                pkg_dir = os.path.dirname(os.path.dirname(spec.origin))
                # sometimes packaging places scripts under .../site-packages/pyspark
                candidate = pkg_dir
                if os.path.exists(os.path.join(candidate, "bin", "spark-submit")):
                    os.environ["SPARK_HOME"] = candidate
                    # ensure PATH includes spark bin
                    os.environ["PATH"] = os.path.join(candidate, "bin") + ":" + os.environ.get("PATH", "")
                    logging.info(f"SPARK_HOME set to {candidate} (derived from pyspark package)")
                    return
        except Exception:
            pass
        # fallback: ensure the directory containing spark-submit is on PATH (it already is)
        logging.info(f"Found spark-submit at {spark_submit}")
        return

    candidate = "/home/airflow/.local/lib/python3.12/site-packages/pyspark"
    if os.path.exists(os.path.join(candidate, "bin", "spark-submit")):
        os.environ["SPARK_HOME"] = candidate
        os.environ["PATH"] = os.path.join(candidate, "bin") + ":" + os.environ.get("PATH", "")
        logging.info(f"SPARK_HOME set to {candidate} (fallback)")
        return

    logging.warning("SPARK_HOME not detected; spark-submit not found in PATH.")


def format_value_for_sql(v):

    if v is None or (isinstance(v, float) and math.isnan(v)) or pd.isna(v):
        return "NULL"

    if isinstance(v, dict):
        if "member0" in v:
            v = v["member0"]
        else:
            import json
            return f"'{json.dumps(v, ensure_ascii=False)}'"

    if isinstance(v, (datetime.datetime, datetime.date, pd.Timestamp)):
        try:
            if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
                v = datetime.datetime.combine(v, datetime.time(0, 0))
            if v.tzinfo is None:
                v = v.replace(tzinfo=datetime.timezone.utc)
            else:
                v = v.astimezone(datetime.timezone.utc)
            iso_str = v.strftime("%Y-%m-%d %H:%M:%S")
            return f"'{iso_str}'"
        except Exception:
            return "NULL"

    # Boolean
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"

    # String
    if isinstance(v, str):
        safe_str = (
            v.replace("'", "''")
             .replace("\n", "\\n")
             .replace("\r", "\\r")
             .replace("\t", "\\t")
        )
        return f"'{safe_str}'"

    # Numeric
    if isinstance(v, (int, float, np.number)):
        if isinstance(v, float) and v.is_integer():
            return str(int(v))
        return str(v)

    return f"'{str(v)}'"


def create_iceberg_table_if_not_exists(table: str, auto_schema: bool = True, columns: dict = None):
    _detect_and_set_java_home()
    _detect_and_set_spark_home()

    host = config["trino"]["host"]
    port = int(config["trino"]["port"])
    user = config["trino"]["user"]
    catalog = config["iceberg"]["catalog"]
    schema = config["iceberg"]["schema"]
    fmt = config["iceberg"]["format"]
    bucket = config["s3"]["bucket"]
    endpoint = config["s3"]["endpoint"]
    batch_size = int(config["size_config"]["batch_size"])

    source_path = f"s3://{bucket}/raw/{table}/"
    target_path = f"s3://{bucket}/iceberg/{schema}/{table}/"

    logging.info(f"[Airflow] Processing table: {table}")
    logging.info(f"[S3] Reading from: {source_path}")

    try:
        fs = s3fs.S3FileSystem(
            key=config["s3"]["access_key"],
            secret=config["s3"]["secret_key"],
            client_kwargs={"endpoint_url": endpoint}
        )
    except Exception as e:
        raise AirflowException(f"[S3] Failed to create S3 client: {e}")

    files = fs.glob(f"{bucket}/raw/{table}/*.parquet")
    if not files:
        raise AirflowException(f"[S3] No Parquet files found for {table}")

    def extract_timestamp_key(path):
        import re
        import os
        import datetime

        fname = os.path.basename(path)

        match = re.search(r"^(\d{4}_\d{2}_\d{2})_(\d{13})", fname)
        if match:
            date_part = match.group(1)
            epoch_part = match.group(2)
            try:
                ts = datetime.datetime.fromtimestamp(int(epoch_part) / 1000)
                return ts
            except Exception:
                pass

        match = re.search(r"^(\d{4}_\d{2}_\d{2})", fname)
        if match:
            try:
                return datetime.datetime.strptime(match.group(1), "%Y_%m_%d")
            except Exception:
                pass

        return datetime.datetime.min

    latest_file = sorted(files, key=extract_timestamp_key, reverse=True)[0]
    logging.info(f"[S3] Found latest parquet file for {table}: {latest_file}")

    dataset = pq.ParquetDataset([f"s3://{latest_file}"], filesystem=fs)
    df = dataset.read_pandas().to_pandas()


    for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        try:
            df[col] = pd.to_datetime(df[col], utc=True).dt.tz_convert(None)
        except Exception as e:
            logging.warning(f"[Iceberg] Could not normalize timestamp column {col}: {e}")

    for col in df.columns:
        if df[col].dtype == "object":
            sample_val = df[col].dropna().astype(str).head(5)
            if sample_val.str.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}").any():
                try:
                    df[col] = pd.to_datetime(df[col], errors="ignore")
                except Exception:
                    pass

    if auto_schema:
        try:
            logging.info("[Iceberg] Detecting schema automatically...")
            columns = extract_schema_from_parquet(bucket, f"raw/{table}/")
            logging.info(f"[Iceberg] Extracted columns: {columns}")
        except Exception as e:
            logging.warning(f"[Iceberg] Auto schema detection failed: {e}")
            columns = {"id": "bigint", "data": "varchar", "ingested_at": "timestamp"}

    conn = connect(host=host, port=port, user=user, catalog=catalog, schema=schema)
    cur = conn.cursor()
    full_table_name = f"{catalog}.{schema}.{table}"

    # cur.execute(f"SHOW TABLES LIKE '{table}'")
    # if len(cur.fetchall()) > 0:
    #     logging.info(f"[Iceberg] Dropping existing table {full_table_name}")
    #     cur.execute(f"DROP TABLE {full_table_name}")
    #
    # filtered_columns = {n: t for n, t in columns.items() if not n.startswith("_")}
    # cols_def = ", ".join(f"{n} {t}" for n, t in filtered_columns.items())
    # create_sql = f"""
    #     CREATE TABLE {full_table_name} (
    #         {cols_def}
    #     )
    #     WITH (
    #         format = '{fmt}'
    #     )
    # """
    # logging.info(f"[Iceberg] Executing SQL:\n{create_sql.strip()}")
    # cur.execute(create_sql)
    # logging.info(f"[Iceberg] Created table {full_table_name}")

    df = df[[c for c in df.columns if not c.startswith("_")]]
    cols = ", ".join(df.columns)

    try:
        from pyspark.sql import SparkSession
    except Exception as e:
        raise AirflowException(f"[Spark] Cannot import pyspark: {e}")

    import os
    os.environ["AWS_REGION"] = "us-east-1"
    spark_master = os.environ.get("SPARK_MASTER_URL") or os.environ.get("SPARK_MASTER") or "local[*]"

    spark = (
        SparkSession.builder
        .appName("IcebergWriter")
        .master("local[*]")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.iceberg.uri", config["iceberg"]["nessie_url"])
        .config("spark.sql.catalog.iceberg.ref", "main")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://raw/iceberg")
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", config["s3"]["endpoint"])
        .config("spark.sql.catalog.iceberg.s3.access-key-id", config["s3"]["access_key"])
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", config["s3"]["secret_key"])
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.region", config["s3"]["region"])
        .config("spark.sql.catalog.iceberg.aws.region", config["s3"]["region"])
        .config("spark.driver.extraJavaOptions", "-Daws.region=us-east-1")
        .config("spark.executor.extraJavaOptions", "-Daws.region=us-east-1")
        .config("spark.sql.catalog.iceberg.authentication.type", "NONE")
        .getOrCreate()
    )

    print("\n=== Loaded Spark Classpath (jars) ===")
    for jar in spark.sparkContext._gateway.jvm.java.lang.System.getProperty("java.class.path").split(":"):
        if "nessie" in jar or "iceberg" in jar:
            print(jar)

    try:
        for i in range(0, len(df), batch_size):
            chunk = df.iloc[i:i + batch_size]
            df_spark = spark.createDataFrame(chunk)
            logging.info(f"[PySpark] Writing chunk {i // batch_size + 1} of {math.ceil(len(df) / batch_size)}")
            try:
                df_spark.writeTo(f"{full_table_name}").overwrite()
            except Exception as e:
                if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                    df_spark.writeTo(f"{full_table_name}").create()
                else:
                    raise
        logging.info(f"[PySpark] Successfully inserted {len(df)} rows into {full_table_name}")
    except Exception as e:
        logging.exception(f"[PySpark] Failed during write: {e}")
        raise
    finally:
        try:
            spark.stop()
        except Exception:
            logging.warning("spark.stop() raised an exception (ignored).")
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
