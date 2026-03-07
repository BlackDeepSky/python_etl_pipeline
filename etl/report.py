import psycopg2
from dotenv import load_dotenv
import os
import csv

#load env variables
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

cur = conn.cursor()
cur.execute("select status, sum(amount) as summ_amount, count(*) as count_transactions, round(avg(amount),2) as avg_summ from transactions group by status;")
rows = cur.fetchall()
columns = [desc[0] for desc in cur.description]

with open('report.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['status', 'summ_amount', 'count_transactions', 'avg_summ'])
    writer.writeheader()
    for row in rows:
        row_dict = dict(zip(columns, row))
        writer.writerow(row_dict)

cur.close()
conn.close()
