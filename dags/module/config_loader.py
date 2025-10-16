import os
from dotenv import load_dotenv

def load_env():

    load_dotenv()

    config = {
        "airbyte": {
            "api": os.getenv("AIRBYTE_API"),
            "connections": [c.strip() for c in os.getenv("AIRBYTE_CONNECTIONS", "").split(",") if c.strip()],
	    "user": os.getenv("AIRBYTE_USER"),
	    "password": os.getenv("AIRBYTE_PASSWORD")
        },
        "trino": {
            "host": os.getenv("TRINO_HOST"),
            "port": int(os.getenv("TRINO_PORT", 8080)),
            "user": os.getenv("TRINO_USER", "batman"),
        },
        "iceberg": {
            "catalog": os.getenv("ICEBERG_CATALOG", "nessie"),
            "schema": os.getenv("ICEBERG_SCHEMA", "raw"),
            "format": os.getenv("ICEBERG_FORMAT", "parquet"),
            "list_tables": [t.strip() for t in os.getenv("ICEBERG_TABLE_NAMES", "").split(",") if t.strip()],

        },
        "s3": {
            "endpoint": os.getenv("S3_ENDPOINT"),
            "bucket": os.getenv("S3_BUCKET"),
            "region": os.getenv("S3_REGION", "us-east-1"),
            "access_key": os.getenv("S3_ACCESS_KEY"),
            "secret_key": os.getenv("S3_SECRET_KEY"),
        },
        "dbt": {
            "project_dir": os.getenv("DBT_PROJECT_DIR"),
            "profiles_dir": os.getenv("DBT_PROFILES_DIR"),
        },
        "size_config": {
            "batch_size": int(os.getenv("BATCH_SIZE",100))
        }
    }

    return config