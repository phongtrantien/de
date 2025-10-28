from pyspark.sql import SparkSession
import os
from dags.module.logger.logger_util import get_logger

logger = get_logger("iceberg_loader")

def build_spark_session(app_name: str, master_url: str, deploy_mode: str, config: dict) -> SparkSession:
    os.environ["AWS_REGION"] = "us-east-1"

    spark_builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        .config("spark.submit.deployMode", deploy_mode)
        .config("spark.sql.catalog.connector", "org.apache.connector.spark.SparkCatalog")
        .config("spark.sql.catalog.connector.catalog-impl", "org.apache.connector.nessie.NessieCatalog")
        .config("spark.sql.catalog.connector.uri", config["connector"]["nessie_url"])
        .config("spark.sql.catalog.connector.ref", "main")
        .config("spark.sql.catalog.connector.warehouse", "s3a://raw/connector")
        .config("spark.sql.catalog.connector.io-impl", "org.apache.connector.aws.s3.S3FileIO")
        .config("spark.sql.catalog.connector.s3.endpoint", config["s3"]["endpoint"])
        .config("spark.sql.catalog.connector.s3.access-key-id", config["s3"]["access_key"])
        .config("spark.sql.catalog.connector.s3.secret-access-key", config["s3"]["secret_key"])
        .config("spark.sql.catalog.connector.s3.path-style-access", "true")
        .config("spark.sql.catalog.connector.s3.region", config["s3"]["region"])
        .config("spark.sql.catalog.connector.authentication.type", "NONE")
    )

    spark = spark_builder.getOrCreate()

    logger.info("\n=== Spark Loaded JARs ===")
    for jar in spark.sparkContext._gateway.jvm.java.lang.System.getProperty("java.class.path").split(":"):
        if "connector" in jar or "nessie" in jar:
            print(jar)

    return spark
