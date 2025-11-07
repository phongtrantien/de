from datetime import datetime
import logging
import traceback
from typing import Iterable, Optional

from pyspark.sql import SparkSession

from module.utils.iceberg import create_iceberg_table_if_not_exists
from module.env.config_loader import load_env
from module.utils.metadata import get_tables


def iter_tables_stream(spark: SparkSession,
                       metadata_table: str,
                       group_id: int,
                       select_col: str = "table_name") -> Iterable[str]:

    df_or_list = get_tables(spark, metadata_table, group_id)

    if hasattr(df_or_list, "toLocalIterator"):

        df = df_or_list.select(select_col).distinct()
        # Coalesce nhỏ để giảm số partition nếu dataset nhỏ/metadata
        df = df.coalesce(1)
        for row in df.toLocalIterator():
            yield row[select_col]
    else:
        for t in df_or_list:
            yield t


def create_with_retry(spark: SparkSession,
                      table: str,
                      config: dict,
                      max_retries: int = 3,
                      backoff_secs: float = 1.5) -> bool:

    attempt = 0
    last_err: Optional[Exception] = None
    while attempt < max_retries:
        try:
            create_iceberg_table_if_not_exists(table, spark=spark, config=config)
            return True
        except Exception as e:
            last_err = e
            attempt += 1
            wait = backoff_secs ** attempt
            logging.warning(
                f"[Retry] Failed to create Iceberg table '{table}' (attempt {attempt}/{max_retries}): {e}. "
                f"Retrying in {wait:.2f}s..."
            )

            import time
            time.sleep(wait)
    logging.error(f"[Error] Exhausted retries creating table '{table}': {last_err}")
    logging.error(traceback.format_exc())
    return False


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logging.info("Starting Iceberg Metadata Loader Pipeline")

    config = load_env()
    metadata_table = config["iceberg"]["metadata_table"]
    group_id = config.get("group_id", 1)

    logging.info("Initializing single Spark session (K8s cluster mode)")
    spark = SparkSession.builder.appName("IcebergLoadMetadata").getOrCreate()

    #broadcast config (giảm serialize nặng)
    bc_config = spark.sparkContext.broadcast(config)

    logging.info(f"Fetching table metadata from: {metadata_table} (group_id={group_id})")
    tables_iter = iter_tables_stream(spark, metadata_table, group_id, select_col=config.get("metadata_col", "table_name"))

    total = 0
    ok = 0
    fail = 0

    for table in tables_iter:
        total += 1
        logging.info("=" * 80)
        logging.info(f"[{total}] Creating Iceberg table: {table}")

        if create_with_retry(spark, table, bc_config.value, max_retries=config.get("retry", 3)):
            ok += 1
            logging.info(f"[{total}] Created: {table}")
        else:
            fail += 1
            logging.info(f"[{total}] Failed: {table}")

        spark.catalog.clearCache()

    logging.info("=" * 80)
    logging.info(f"Done. Total: {total}, Success: {ok}, Failed: {fail}")
    logging.info("ETL Pipeline finished.")

    try:
        spark.stop()
        logging.info("Spark session stopped.")
    except Exception as stop_err:
        logging.warning(f"Failed to stop Spark session: {stop_err}")


if __name__ == "__main__":
    main()