from dags.module.logger.logger_util import get_logger
from dags.module.config_loader import load_env

config = load_env()

logger = get_logger("fetch table_name")
def get_tables(spark, metadata_table, group_id):
    try:

        query = f"""
            SELECT table_name
            FROM {metadata_table}
            WHERE group_id = '{group_id}'
              AND enabled = true
            ORDER BY priority ASC
        """
        result_df = spark.sql(query)
        return [row["table_name"] for row in result_df.collect()]
    except Exception as e:
        logger.error(f"Failed to fetch tables for group_id={group_id}: {e}")
        return []