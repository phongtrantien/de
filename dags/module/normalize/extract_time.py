import re
import os
from datetime import datetime

def extract_timestamp_key(path):
    fname = os.path.basename(path)
    match = re.search(r"^(\d{4}_\d{2}_\d{2})_(\d{13})", fname)
    if match:
        try:
            return datetime.fromtimestamp(int(match.group(2)) / 1000)
        except Exception:
            pass
    match = re.search(r"^(\d{4}_\d{2}_\d{2})", fname)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y_%m_%d")
        except Exception:
            pass
    return datetime.min