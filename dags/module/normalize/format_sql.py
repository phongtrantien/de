import math
import pandas as pd
import numpy as np
import datetime
from dateutil import parser

def format_value_for_sql(v):
    """Convert Python/Pandas/Numpy value into safe SQL literal for Trino/Iceberg."""


    if v is None or (isinstance(v, float) and math.isnan(v)) or pd.isna(v):
        return "NULL"


    if isinstance(v, (datetime.datetime, datetime.date, pd.Timestamp, np.datetime64, str)):
        try:

            if isinstance(v, np.datetime64):
                v = pd.to_datetime(v)

            if isinstance(v, str):

                v = parser.parse(v)

            if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
                v = datetime.datetime.combine(v, datetime.time(0, 0))

            if v.tzinfo is None:
                v = v.replace(tzinfo=datetime.timezone.utc)
            else:
                v = v.astimezone(datetime.timezone.utc)

            iso_str = v.strftime('%Y-%m-%dT%H:%M:%SZ')
            return f"{iso_str}"

        except Exception as e:
            return "NULL"

    # --- BOOLEAN ---
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"

    # --- STRING ---
    if isinstance(v, str):
        safe_str = (
            v.replace("'", "''")
             .replace("\n", "\\n")
             .replace("\r", "\\r")
             .replace("\t", "\\t")
        )
        return f"'{safe_str}'"

    # --- NUMERIC ---
    if isinstance(v, (int, float, np.number)):
        if isinstance(v, float) and v.is_integer():
            return str(int(v))
        return str(v)

    return f"'{str(v)}'"
