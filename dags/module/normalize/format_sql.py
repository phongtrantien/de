import math
import pandas as pd
import numpy as np
from datetime import datetime, date, timezone, time as dtime


def format_value_for_sql(v):
    if v is None or (isinstance(v, float) and math.isnan(v)) or pd.isna(v):
        return "NULL"

    if isinstance(v, dict):
        if "member0" in v:
            v = v["member0"]
        else:
            import json
            return f"'{json.dumps(v, ensure_ascii=False)}'"

    if isinstance(v, (datetime, date, pd.Timestamp)):
        try:
            if isinstance(v, date) and not isinstance(v, datetime):
                v = datetime.combine(v, dtime(0, 0))
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            else:
                v = v.astimezone(timezone.utc)
            return f"'{v.strftime('%Y-%m-%d %H:%M:%S')}'"
        except Exception:
            return "NULL"

    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"

    if isinstance(v, str):
        safe_str = v.replace("'", "''").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
        return f"'{safe_str}'"

    if isinstance(v, (int, float, np.number)):
        if isinstance(v, float) and v.is_integer():
            return str(int(v))
        return str(v)

    return f"'{str(v)}'"
