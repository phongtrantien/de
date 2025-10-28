import s3fs
from trino.dbapi import connect
from airflow.exceptions import AirflowException
from module.config_loader import load_env

config = load_env()

def get_s3_filesystem():

    try:
        return s3fs.S3FileSystem(
            key=config["s3"]["access_key"],
            secret=config["s3"]["secret_key"],
            client_kwargs={"endpoint_url": config["s3"]["endpoint"]}
        )
    except Exception as e:
        raise AirflowException(f"Failed to create S3 client: {e}")


def get_trino_connection():

    return connect(
        host=config["trino"]["host"],
        port=int(config["trino"]["port"]),
        user=config["trino"]["user"],
        catalog=config["iceberg"]["catalog"],
        schema=config["iceberg"]["schema"]
    )
