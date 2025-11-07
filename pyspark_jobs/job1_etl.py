from datetime import datetime
import logging
from module.utils.iceberg import create_iceberg_table_if_not_exists
from module.config_loader import load_env
from module.utils.metadata import get_tables
from module.connector.spark_session_builder import build_spark_session


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logging.info("Starting Iceberg Metadata Loader Pipeline")

    config = load_env()

    metadata_table = config["iceberg"]["metadata_table"]
    spark_master = config["spark_config"]["master_url"]
    deploy_mode = config["spark_config"]["deploy_mode"]
    group_id = 1

    logging.info(f"Initializing Spark session (master={spark_master}, mode={deploy_mode})")
    spark = build_spark_session("IcebergLoadMetadata", spark_master, deploy_mode, config)

    logging.info(f"Fetching table metadata from: {metadata_table} (group_id={group_id})")
    tables = get_tables(spark, metadata_table, group_id)
    logging.info(f"Found {len(tables)} tables to process: {tables}")

    for table in tables:
        try:
            logging.info(f"Creating Iceberg table: {table}")
            create_iceberg_table_if_not_exists(table)
        except Exception as e:
            logging.error(f"Failed to create Iceberg table {table}: {e}")
            raise

    logging.info("All Iceberg tables created successfully.")
    logging.info("ETL Pipeline finished.")


if __name__ == "__main__":
    main()
