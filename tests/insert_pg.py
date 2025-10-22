import psycopg2
from faker import Faker
from tqdm import tqdm
import random
from datetime import datetime


conn = psycopg2.connect(
    dbname="airbyte",  # tên database
    user="airbyte",
    password="airbyte",
    host="192.168.146.132",
    port="5432"
)
cur = conn.cursor()


fake = Faker()


batch_size = 1
total_rows = 20

product_name = ["Sức khỏe vip", "HSSV", "Xe trọn đời", "An ninh mạng"]
product_type = ["Health", "Life","Car"]
for i in tqdm(range(0, total_rows, batch_size)):
    batch = []
    for j in range(batch_size):
        product_id = i + j + 4
        product_name = random.choice(product_name)
        product_type = random.choice(product_type)
        created_at = fake.date_time_between(start_date="-2y", end_date="now")
        updated_at = datetime.now()
        batch.append((product_id, product_name, product_type, created_at, updated_at))


    cur.executemany("""
                    INSERT INTO product (product_id, product_name, product_type, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s)
                    """, batch)

    conn.commit()
    print(f" Inserted {i + batch_size:,} rows...")

cur.close()
conn.close()

print(f" Done: {total_rows} rows inserted successfully.")
