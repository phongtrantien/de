import s3fs
import pyarrow.fs
from trino.dbapi import connect
from airflow.exceptions import AirflowException
from module.config_loader import load_env

config = load_env()


def get_s3_filesystem():
    try:
        return pyarrow.fs.S3FileSystem(
            access_key=config["s3"]["access_key"],
            secret_key=config["s3"]["secret_key"],
            endpoint_override=config["s3"]["endpoint"],
            scheme="http"
        )
    except Exception as e:
        raise AirflowException(f"Failed to create Arrow S3 filesystem: {e}")



def get_trino_connection():

    return connect(
        host=config["trino"]["host"],
        port=int(config["trino"]["port"]),
        user=config["trino"]["user"],
        catalog=config["iceberg"]["catalog"],
        schema=config["iceberg"]["schema"]
    )
