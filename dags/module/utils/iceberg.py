import logging
from trino.dbapi import connect
from airflow.exceptions import AirflowException
from module.config_loader import load_env
from module.utils.schema_detect import extract_schema_from_parquet
import pyarrow.parquet as pq
import pandas as pd
import s3fs
import datetime

config = load_env()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def format_value_for_sql(v):
    if v is None:
        return "NULL"
    if isinstance(v, (datetime.datetime, datetime.date, pd.Timestamp)):
        return f"TIMESTAMP '{v}'"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    return str(v)


def create_iceberg_table_if_not_exists(table: str, auto_schema: bool = True, columns: dict = None):
    host = config["trino"]["host"]
    port = int(config["trino"]["port"])
    user = config["trino"]["user"]
    catalog = config["iceberg"]["catalog"]
    schema = config["iceberg"]["schema"]
    fmt = config["iceberg"]["format"]
    bucket = config["s3"]["bucket"]
    endpoint = "http://minio:9000"
    batch_size = config["size_config"]["batch_size"]

    source_path = f"s3://{bucket}/raw/{table}/"
    target_path = f"s3://{bucket}/iceberg/{schema}/{table}/"

    fs = s3fs.S3FileSystem(
        key=config["s3"]["access_key"],
        secret=config["s3"]["secret_key"],
        client_kwargs={"endpoint_url": endpoint}
    )

    files = fs.glob(f"{bucket}/raw/{table}/*.parquet")
    if not files:
        raise AirflowException(f"No Parquet files found for {table}")

    dataset = pq.ParquetDataset([f"s3://{f}" for f in files], filesystem=fs)
    df = dataset.read_pandas().to_pandas()

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

    cur.execute(f"SHOW TABLES LIKE '{table}'")
    if len(cur.fetchall()) > 0:
        logging.info(f"[Iceberg] Dropping existing table {full_table_name}")
        cur.execute(f"DROP TABLE {full_table_name}")

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

    df = df[[c for c in df.columns if not c.startswith("_")]]

    logging.info(f"[Iceberg] Inserting {len(df)} rows from Parquet for table {table} with batch_size = {batch_size}")
    cols = ", ".join(df.columns)

    for i in range(0, len(df), batch_size):
        chunk = df.iloc[i:i + batch_size]
        values_sql = []

        for row in chunk.itertuples(index=False, name=None):
            formatted = [format_value_for_sql(v) for v in row]
            values_sql.append(f"({', '.join(formatted)})")

        insert_sql = f"INSERT INTO {full_table_name} ({cols}) VALUES {', '.join(values_sql)}"
        logging.info(f"[Iceberg] Detail SQL INSERT:\n{insert_sql}")
        cur.execute(insert_sql)

    logging.info(f"[Iceberg] Inserted {len(df)} rows into {full_table_name}")
    cur.close()
    conn.close()
