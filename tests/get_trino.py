from trino.dbapi import connect
from trino.auth import BasicAuthentication
import sys

def query_trino_sample(
    host="192.169.203.40",
    port=9090,
    user="admin",
    catalog="iceberg",
    schema="raw",
    sql="SELECT * FROM par_etl_table LIMIT 10",
    username=None,
    password=None,
    http_scheme="http",
):

    auth = None
    if username and password:
        auth = BasicAuthentication(username, password)

    try:
        conn = connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema,
            http_scheme=http_scheme,
            auth=auth,

        )
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        print(f"Fetched {len(rows)} rows:")
        for r in rows:
            print(r)

        if cur.description:
            cols = [d[0] for d in cur.description]
            print("\nColumns:", cols)

    except Exception as e:
        print("Query failed:", e, file=sys.stderr)
        raise

if __name__ == "__main__":
    query_trino_sample()