from pyspark.sql import SparkSession
import os
from module.logger.logger_util import get_logger

logger = get_logger("iceberg_loader")

def build_spark_session(app_name: str, master_url: str, deploy_mode: str, config: dict) -> SparkSession:
    os.environ["AWS_REGION"] = "us-east-1"

    spark_builder = (
        SparkSession.builder
        .appName("IcebergWriter")
        .master("k8s://https://192.169.203.38:6443")
        .config("spark.submit.deployMode", "cluster")
        .config("spark.kubernetes.container.image", "registry.registry.svc.cluster.local:5000/spark:3.4.2")
        .config("spark.kubernetes.namespace", "spark")
        .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")
        .config("spark.driverEnv.JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64")
        .config("spark.executorEnv.JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.iceberg.uri", config["iceberg"]["nessie_url"])
        .config("spark.sql.catalog.iceberg.ref", "main")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://lake-house")
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", config["s3"]["endpoint"])
        .config("spark.sql.catalog.iceberg.s3.access-key-id", config["s3"]["access_key"])
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", config["s3"]["secret_key"])
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.region", config["s3"]["region"])
        .config("spark.sql.catalog.iceberg.authentication.type", "NONE")
    )


    spark = spark_builder.getOrCreate()
    logger.info("Syncing Iceberg S3 config to Hadoop S3A config ...")
    conf_map = {
        "spark.sql.catalog.iceberg.s3.endpoint": config["s3"]["endpoint"],
        "spark.sql.catalog.iceberg.s3.access-key-id": config["s3"]["access_key"],
        "spark.sql.catalog.iceberg.s3.secret-access-key": config["s3"]["secret_key"],
        "spark.sql.catalog.iceberg.s3.path-style-access": "true",
        "spark.sql.catalog.iceberg.s3.region": "us-east-1",
    }

    jconf = spark.sparkContext._jsc.hadoopConfiguration()
    sconf = spark.sparkContext.getConf()

    for src, dest in conf_map.items():
        if sconf.contains(src):
            val = sconf.get(src)
            jconf.set(dest, val)
            logger.info(f"  synced {src} -> {dest} = {val}")

    jconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    jconf.set("fs.s3a.connection.ssl.enabled", "false")
    jconf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    logger.info("SparkSession created successfully.")

    logger.info("\n=== Spark Loaded JARs ===")
    for jar in spark.sparkContext._gateway.jvm.java.lang.System.getProperty("java.class.path").split(":"):
        if "iceberg" in jar or "nessie" in jar or "aws" in jar:
            print(jar)

    return spark