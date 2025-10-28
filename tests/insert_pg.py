import psycopg2
from faker import Faker
from tqdm import tqdm
import random
from datetime import datetime


conn = psycopg2.connect(
    dbname="airbyte",
    user="airbyte",
    password="a169ac3cb579f1171fa9ada095f1c9c8b95533bc524ef6a95ce209a2b399a76e",
    host="192.169.203.40",
    port="5432"
)
cur = conn.cursor()


fake = Faker()


batch_size = 10000
total_rows = 500000

for i in tqdm(range(0, total_rows, batch_size)):
    batch = []
    for j in range(batch_size):
        policy_id = i + j
        customer_id = i + j +100
        policy_type = random.choice(['Xe máy', 'Sức khỏe', 'An ninh mạng', 'HSSV'])
        start_date = fake.date()
        end_date = fake.date()
        premium_amount = random.randint(10000,1000000)
        created_at = datetime.now()
        updated_at = datetime.now()
        batch.append((policy_id, customer_id, policy_type, start_date, end_date, premium_amount,created_at, updated_at))


    cur.executemany("""
                    INSERT INTO vbi_data.policy (policy_id, customer_id, policy_type, start_date, end_date, premium_amount,created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, batch)

    conn.commit()
    print(f" Inserted {i + batch_size:,} rows...")

cur.close()
conn.close()

print(f" Done: {total_rows} rows inserted successfully.")
