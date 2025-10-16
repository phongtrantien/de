import logging
import math
import datetime
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

def format_value_for_sql(v):
    """Format Python/Pandas values into valid Trino SQL literals."""
    if v is None or (isinstance(v, float) and math.isnan(v)) or pd.isna(v):
        return "NULL"

    if isinstance(v, dict):
        if "member0" in v:
            v = v["member0"]
        else:
            import json
            return f"'{json.dumps(v, ensure_ascii=False)}'"

    # Datetime / Timestamp
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
    """Read Parquet from S3/MinIO and insert into Iceberg table via Trino."""
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
            client_kwargs={"endpoint_url":endpoint}
        )
    except Exception as e:
        raise AirflowException(f"[S3] Failed to create S3 client: {e}")

    files = fs.glob(f"{bucket}/raw/{table}/*.parquet")
    if not files:
        raise AirflowException(f"[S3] No Parquet files found for {table}")

    def extract_timestamp_key(path):
        import re
        import os
        fname = os.path.basename(path)
        match = re.search(r"(\d{4}_\d{2}_\d{2})", fname)
        if match:
            try:
                return datetime.datetime.strptime(match.group(1), "%Y_%m_%d")
            except Exception:
                return datetime.datetime.min
        return datetime.datetime.min

    latest_file = sorted(files, key=extract_timestamp_key, reverse=True)[0]
    logging.info(f"[S3] Found latest parquet file for {table}: {latest_file}")

    dataset = pq.ParquetDataset([f"s3://{latest_file}"], filesystem=fs)
    df = dataset.read_pandas().to_pandas()

    # --- Normalize timestamps ---
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

    # --- Drop if exists ---
    cur.execute(f"SHOW TABLES LIKE '{table}'")
    if len(cur.fetchall()) > 0:
        logging.info(f"[Iceberg] Dropping existing table {full_table_name}")
        cur.execute(f"DROP TABLE {full_table_name}")

    # --- Create new table ---
    filtered_columns = {n: t for n, t in columns.items() if not n.startswith("_")}
    cols_def = ", ".join(f"{n} {t}" for n, t in filtered_columns.items())
    create_sql = f"""
        CREATE TABLE {full_table_name} (
            {cols_def}
        )
        WITH (
            format = '{fmt}'
        )
    """
    logging.info(f"[Iceberg] Executing SQL:\n{create_sql.strip()}")
    cur.execute(create_sql)
    logging.info(f"[Iceberg] Created table {full_table_name}")

    # --- Insert data ---
    df = df[[c for c in df.columns if not c.startswith("_")]]
    cols = ", ".join(df.columns)
    logging.info(f"[Iceberg] Inserting {len(df)} rows with batch_size = {batch_size}")

    for i in range(0, len(df), batch_size):
        chunk = df.iloc[i:i + batch_size]
        values_sql = []

        for row in chunk.itertuples(index=False, name=None):
            formatted = [format_value_for_sql(v) for v in row]
            values_sql.append(f"({', '.join(formatted)})")

        insert_sql = f"INSERT INTO {full_table_name} ({cols}) VALUES {', '.join(values_sql)}"
        logging.info(f"[Iceberg] Detail SQL INSERT:\n{insert_sql}")
        cur.execute(insert_sql)

    logging.info(f"[Iceberg] Successfully inserted {len(df)} rows into {full_table_name}")
    cur.close()
    conn.close()