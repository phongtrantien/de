import s3fs
import pyarrow.fs
from trino.dbapi import connect
from airflow.exceptions import AirflowException
from module.config_loader import load_env
from dags.module.logger.logger_util import get_logger
config = load_env()

logger = get_logger("iceberg_loader")
def minio_connector(spark, config: dict):

    endpoint = config["s3"]["endpoint"]
    access_key = config["s3"]["access_key"]
    secret_key = config["s3"]["secret_key"]

    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hconf.set("fs.s3a.fast.upload", "true")

    if not endpoint:
        raise RuntimeError("MinIO endpoint is missing in config['s3']['endpoint'].")

    hconf.set("fs.s3a.endpoint", endpoint)
    hconf.set("fs.s3a.path.style.access", "true")
    if endpoint.startswith("http://"):
        hconf.set("fs.s3a.connection.ssl.enabled", "false")
    else:
        hconf.set("fs.s3a.connection.ssl.enabled", "true")

    hconf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    if not access_key or not secret_key:
        raise RuntimeError("MinIO access/secret key missing in config['s3'] (access_key/secret_key).")
    hconf.set("fs.s3a.access.key", access_key)
    hconf.set("fs.s3a.secret.key", secret_key)
    hconf.set("fs.s3a.attempts.maximum", "10")
    hconf.set("fs.s3a.connection.maximum", "64")
    hconf.set("fs.s3a.paging.maximum", "1000")

    logger.info(f"Applied MinIO S3A conf: endpoint={endpoint}, path.style.access=true, ssl={not endpoint.startswith('http://')}, provider=SimpleAWSCredentialsProvider")




def get_trino_connection():

    return connect(
        host=config["trino"]["host"],
        port=int(config["trino"]["port"]),
        user=config["trino"]["user"],
        catalog=config["iceberg"]["catalog"],
        schema=config["iceberg"]["schema"]
    )
