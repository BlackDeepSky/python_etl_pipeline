import csv
import psycopg2
from psycopg2 import errors
from dotenv import load_dotenv
import os

#load env variables
load_dotenv()

def load_large_transactions(filepath, min_amount):
    #db connect
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

    cursor = conn.cursor()

    #find_max_id from db
    cursor.execute("SELECT MAX(id) FROM transactions")
    max_id = cursor.fetchone()[0]
    if max_id is None:
        max_id = 0
    with open(filepath, 'r') as file:
        #use DictReader for parsing CSV
        reader = csv.DictReader(file)
        counter = 0
        for row in reader:
            try:
                if int(row["id"]) > max_id and int(row["amount"]) > min_amount and row["status"] == 'success':
                    cursor.execute(
                        "INSERT INTO transactions (id, amount, status) VALUES (%s, %s, %s)",
                        (row["id"], row["amount"], row["status"])
                    )
                    counter += 1
            #skip bad rows
            except ValueError:
                print(f"Skipping bad row: {row}")
            #skip duplicate rows and rollback transaction
            except errors.UniqueViolation:
                conn.rollback()
                print(f"Skipping duplicate row: {row}")
            except Exception as e:
                print(f"Skipping unknown error: {row}, {e}")
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Строк загружено: {counter}")

load_large_transactions('transactions.csv', 100)
