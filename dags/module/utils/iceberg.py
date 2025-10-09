from trino.dbapi import connect
from airflow.exceptions import AirflowException
from dags.module.config_loader import load_env
from dags.module.utils.schema_detect import extract_schema_from_avro

config = load_env()


def create_iceberg_table_if_not_exists(table: str, auto_schema: bool = True, columns: dict = None):
    """
    Tạo bảng Iceberg nếu chưa có.
    Nếu auto_schema=True, tự động lấy schema từ Avro trên MinIO.
    """
    host = config["trino"]["host"]
    port = config["trino"]["port"]
    user = config["trino"]["user"]
    catalog = config["iceberg"]["catalog"]
    schema = config["iceberg"]["schema"]
    fmt = config["iceberg"]["format"]
    bucket = config["s3"]["bucket"]
    s3_path = f"s3a://{bucket}/raw/{table}/"

    # Nếu auto_schema -> đọc schema từ MinIO
    if auto_schema:
        try:
            columns = extract_schema_from_avro(bucket, f"raw/{table}/")
        except Exception as e:
            print(f"[Iceberg] Auto schema detection failed: {e}")
            columns = {"id": "bigint", "data": "varchar", "ingested_at": "timestamp"}

    conn = connect(host=host, port=port, user=user, catalog=catalog, schema=schema)
    cur = conn.cursor()

    full_table_name = f"{catalog}.{schema}.{table}"

    # Check tồn tại
    cur.execute(f"SHOW TABLES LIKE '{table}'")
    exists = len(cur.fetchall()) > 0

    if exists:
        print(f"[Iceberg] Table {full_table_name} already exists, skipping create.")
        return

    cols_def = ", ".join([f"{name} {dtype}" for name, dtype in columns.items()])
    sql = f"""
        CREATE TABLE {full_table_name} (
            {cols_def}
        )
        WITH (
            location = '{s3_path}',
            format = '{fmt}'
        )
    """

    print(f"[Iceberg] Creating table {full_table_name}")
    try:
        cur.execute(sql)
        print(f"[Iceberg] Created table {full_table_name} successfully.")
    except Exception as e:
        raise AirflowException(f"Failed to create Iceberg table {full_table_name}: {e}")
    finally:
        cur.close()
        conn.close()
