import re
from typing import List, Optional

try:
    from pyspark.sql.types import (
        StructType, StructField, ArrayType, MapType,
        StringType, BinaryType, BooleanType, DateType, TimestampType,
        ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
        DecimalType
    )
except Exception:
    StructType = object
    StructField = object
    ArrayType = object
    MapType = object
    StringType = object
    BinaryType = object
    BooleanType = object
    DateType = object
    TimestampType = object
    ByteType = object
    ShortType = object
    IntegerType = object
    LongType = object
    FloatType = object
    DoubleType = object
    DecimalType = object


class IcebergDDLFromTmpView:

    def __init__(
        self,
        iceberg_full_table_name: str,
        tmp_view_name: str,
        format_version: str = "2",
        extra_tblproperties: Optional[dict] = None,
        select_columns_from_schema: bool = True,
        sanitize_column_names: bool = True,
    ):

        self.iceberg_full_table_name = iceberg_full_table_name
        self.tmp_view_name = tmp_view_name
        self.format_version = format_version
        self.extra_tblproperties = extra_tblproperties or {}
        self.select_columns_from_schema = select_columns_from_schema
        self.sanitize_column_names = sanitize_column_names

    @staticmethod
    def _sanitize(name: str) -> str:
        return re.sub(r"[^A-Za-z0-9_]", "_", name).lower()

    def build_from_dataframe(self, df) -> str:
        return self._build_from_struct(df.schema)

    def build_from_schema(self, schema) -> str:
        return self._build_from_struct(schema)

    def _build_from_struct(self, schema: StructType) -> str:

        if not isinstance(schema, StructType):
            raise TypeError("schema must be pyspark.sql.types.StructType")
        #normalize column
        select_cols: List[str] = []
        for f in schema.fields:
            col = f.name
            if self.sanitize_column_names:
                sanitized = self._sanitize(col)
                if sanitized != col:
                    # alias trong SELECT để cột ra bảng đích có tên an toàn
                    select_cols.append(f"`{col}` AS `{sanitized}`")
                else:
                    select_cols.append(f"`{col}`")
            else:
                select_cols.append(f"`{col}`")

        # SELECT list
        if self.select_columns_from_schema:
            select_expr = ", ".join(select_cols)
        else:
            select_expr = "*"

        # TBLPROPERTIES
        props = {"format-version": str(self.format_version)}
        props.update(self.extra_tblproperties)
        if props:
            kv = ",\n    ".join([f"'{k}'='{v}'" for k, v in props.items()])
            props_sql = f"\nTBLPROPERTIES (\n    {kv}\n)"
        else:
            props_sql = ""

        ddl = f"""
        CREATE TABLE {self.iceberg_full_table_name}
        USING ICEBERG{props_sql}
        AS
        SELECT {select_expr}
        FROM {self.tmp_view_name}
        """.strip()
        return ddl