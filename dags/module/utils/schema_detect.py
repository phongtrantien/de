import io
import pyarrow.parquet as pq
from minio import Minio, S3Error
from dags.module.config_loader import load_env
import logging
config = load_env()


def get_minio_client():

    try:
        endpoint = config["minio"]["endpoint"]
        access_key = config["minio"]["access_key"]
        secret_key = config["minio"]["secret_key"]
        secure = config["minio"].get("secure", "false").lower() == "true"

        endpoint = endpoint.replace("http://", "").replace("https://", "")
        if "/" in endpoint:
            endpoint = endpoint.split("/")[0]

        client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

        client.list_buckets()
        logging.info(f" Kết nối MinIO thành công tới endpoint: {endpoint}")
        return client

    except KeyError as e:
        logging.error(f" Thiếu khóa cấu hình trong file config: {e}")
        raise

    except S3Error as e:
        logging.error(f" Lỗi S3 khi kết nối MinIO: {e}")
        raise

    except Exception as e:
        logging.error(f" Lỗi không xác định khi khởi tạo MinIO client: {e}", exc_info=True)
        raise

def list_parquet_files(bucket: str, prefix: str):

    client = get_minio_client()
    objects = client.list_objects(bucket, prefix=prefix, recursive=True)
    return [obj.object_name for obj in objects if obj.object_name.endswith(".parquet")]


def extract_schema_from_parquet(bucket: str, prefix: str) -> dict:

    client = get_minio_client()
    parquet_files = list_parquet_files(bucket, prefix)

    if not parquet_files:
        raise ValueError(f"No Parquet files found in {bucket}/{prefix}")

    file_path = parquet_files[0]
    print(f"[Parquet] Reading schema from: {file_path}")

    # Đọc dữ liệu từ MinIO
    response = client.get_object(bucket, file_path)
    data = response.read()
    response.close()
    response.release_conn()

    # Đọc schema bằng pyarrow
    parquet_file = pq.ParquetFile(io.BytesIO(data))
    schema = parquet_file.schema_arrow

    columns = {}
    for field in schema:
        iceberg_type = parquet_to_iceberg_type(str(field.type))
        columns[field.name] = iceberg_type

    print(f"[Parquet] Extracted columns: {columns}")
    return columns


def parquet_to_iceberg_type(parquet_type: str) -> str:

    parquet_type = parquet_type.lower()

    mapping = {
        "string": "varchar",
        "utf8": "varchar",
        "large_string": "varchar",
        "int32": "integer",
        "int64": "bigint",
        "float": "real",
        "double": "double",
        "boolean": "boolean",
        "binary": "varbinary",
        "large_binary": "varbinary",
        "timestamp[us]": "timestamp",
        "timestamp[ms]": "timestamp",
        "timestamp[ns]": "timestamp",
        "date32[day]": "date",
        "decimal128": "decimal",
    }

    for k, v in mapping.items():
        if k in parquet_type:
            return v
    return "varchar"
