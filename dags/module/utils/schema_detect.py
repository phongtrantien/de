import io
import avro.schema
import avro.datafile
import avro.io
from minio import Minio
from dags.module.config_loader import load_env

config = load_env()


def get_minio_client():
    """
    Tạo client MinIO từ file .env
    """
    return Minio(
        endpoint=config["minio"]["endpoint"],
        access_key=config["minio"]["access_key"],
        secret_key=config["minio"]["secret_key"],
        secure=config["minio"].get("secure", "false").lower() == "true",
    )


def list_avro_files(bucket, prefix):
    """
    Liệt kê file .avro trong path cụ thể
    """
    client = get_minio_client()
    objects = client.list_objects(bucket, prefix=prefix, recursive=True)
    return [obj.object_name for obj in objects if obj.object_name.endswith(".avro")]


def extract_schema_from_avro(bucket: str, prefix: str) -> dict:
    """
    Lấy schema (cột + kiểu dữ liệu) từ file Avro đầu tiên trong bucket/prefix
    """
    client = get_minio_client()
    avro_files = list_avro_files(bucket, prefix)

    if not avro_files:
        raise ValueError(f"No Avro files found in {bucket}/{prefix}")

    file_path = avro_files[0]
    print(f"[Avro] Reading schema from: {file_path}")

    data = client.get_object(bucket, file_path).read()
    bytes_reader = io.BytesIO(data)
    reader = avro.datafile.DataFileReader(bytes_reader, avro.io.DatumReader())
    schema = reader.datum_reader.writers_schema
    reader.close()

    columns = {}
    for field in schema.fields:
        iceberg_type = avro_to_iceberg_type(field.type)
        columns[field.name] = iceberg_type

    print(f"[Avro] Extracted columns: {columns}")
    return columns


def avro_to_iceberg_type(avro_type) -> str:
    """
    Chuyển kiểu Avro sang kiểu Iceberg / Trino tương ứng
    """
    if isinstance(avro_type, list):
        # Xử lý nullable type, ví dụ ["null", "string"]
        non_null = [t for t in avro_type if t != "null"]
        avro_type = non_null[0] if non_null else "string"

    if isinstance(avro_type, avro.schema.PrimitiveSchema):
        t = avro_type.type
    elif isinstance(avro_type, str):
        t = avro_type
    else:
        t = "string"

    mapping = {
        "string": "varchar",
        "int": "integer",
        "long": "bigint",
        "double": "double",
        "float": "real",
        "boolean": "boolean",
        "bytes": "varbinary",
    }

    return mapping.get(t, "varchar")
