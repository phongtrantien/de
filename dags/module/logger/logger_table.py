from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, TimestampType
from module.logger.logger_util import get_logger
from pyspark.sql.functions import col, to_json

logger = get_logger("iceberg_loader")

def write_log(spark, log_table, table_name, row_count, status, message=""):
    etl_date = int(datetime.now().strftime("%Y%m%d"))
    updated_at = datetime.now()

    if not isinstance(message, str):
        import json
        try:
            message = json.dumps(message)
        except Exception:
            message = str(message)

    log_df = spark.createDataFrame(
        [(etl_date, updated_at, table_name, row_count, status, message)],
        ["etl_date", "updated_at", "table_name", "row_count", "status", "message"]
    ).withColumn("etl_date", F.col("etl_date").cast(LongType())) \
     .withColumn("updated_at", F.col("updated_at").cast(TimestampType())) \
     .withColumn("row_count", F.col("row_count").cast(LongType())) \
     .withColumn("message", to_json(col("message")))

    try:
        log_df.writeTo(log_table).append()
        logger.info(f"[PySpark] Marked sync ETL status for {table_name} is: {status}\n ===============================")
    except Exception as log_err:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(log_err):
            namespace = log_table.split(".")[1]
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
            log_df.writeTo(log_table).create()
        else:
            logger.warning(f"[PySpark] Could not write to log table: {log_err}")

