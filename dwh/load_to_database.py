import csv
import psycopg2
from psycopg2 import errors
from dotenv import load_dotenv
import os
from datetime import datetime

#load env variables
load_dotenv()

def load_csv_to_database():
    #load db
    conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
    )

    cursor = conn.cursor()

    counter = 0

    #LOAD table DIM_CUSTOMER
    cursor.execute("SELECT MAX(customer_id) FROM dim_customer")
    max_id = cursor.fetchone()[0]
    if max_id is None:
        max_id = 0

    with open("customers.csv", 'r') as file:
        #use DictReader for parsing CSV
        reader = csv.DictReader(file)
        for row in reader:
            try:
                if int(row["customer_id"]) > max_id:
                    cursor.execute(
                        "INSERT INTO dim_customer (customer_id, sex, birth_date, region) VALUES (%s, %s, %s, %s)",
                        (row["customer_id"], row["sex"], row["birth_date"], row["region"])
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

    #LOAD table DIM_PRODUCT
    cursor.execute("SELECT MAX(product_id) FROM dim_product")
    max_id = cursor.fetchone()[0]
    if max_id is None:
        max_id = 0
    with open("products.csv", 'r') as file:
        #use DictReader for parsing CSV
        reader = csv.DictReader(file)
        for row in reader:
            try:
                if int(row["product_id"]) > max_id:
                    cursor.execute(
                        "INSERT INTO dim_product (product_id, name, category) VALUES (%s, %s, %s)",
                        (row["product_id"], row["name"], row["category"])
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

    #LOAD table DIM_DATE
    cursor.execute("SELECT MAX(date_id) FROM dim_date")
    max_id = cursor.fetchone()[0]
    if max_id is None:
        max_id = 0
    
    with open("sales.csv", 'r') as file:
        #use DictReader for parsing CSV
        reader = csv.DictReader(file)
        for row in reader:
            try:
                if int(row["order_id"]) > max_id and row["date"]:
                    date = datetime.strptime(row["date"], "%Y-%m-%d")
                    month = date.month
                    year = date.year
                    quarter = (date.month - 1) // 3 + 1
                    cursor.execute(
                        """INSERT INTO dim_date (full_date, month, year, quarter)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (full_date) DO NOTHING""",
                        (date, month, year, quarter)
                    )
                    counter += 1
            #skip bad rows
            except ValueError:
                print(f"Skipping bad row: {row}")
            except Exception as e:
                print(f"Skipping unknown error: {row}, {e}")
        conn.commit()

    #LOAD table FACT_SALES
    cursor.execute("SELECT MAX(order_id) FROM fact_sales")
    max_id = cursor.fetchone()[0]
    if max_id is None:
        max_id = 0
    with open("sales.csv", 'r') as file:
        #use DictReader for parsing CSV
        reader = csv.DictReader(file)
        for row in reader:
            try:
                if int(row["order_id"]) > max_id:
                    date = datetime.strptime(row["date"], "%Y-%m-%d")
                    cursor.execute("SELECT date_id FROM dim_date WHERE full_date = %s", (date,))
                    date_id = cursor.fetchone()[0]
                    cursor.execute(
                        """INSERT INTO fact_sales (order_id, date_id, product_id, customer_id, amount, quantity)
                        VALUES (%s, %s, %s, %s, %s, %s)""",
                        (row["order_id"], date_id, row["product_id"], row["customer_id"], row["amount"], row["quantity"])
                    )
                    counter += 1
            #skip bad rows
            except ValueError:
                print(f"Skipping bad row: {row}")
            except errors.UniqueViolation:
                conn.rollback()
                print(f"Skipping duplicate row: {row}")
            except Exception as e:
                print(f"Skipping unknown error: {row}, {e}")
        conn.commit()
    cursor.close()
    conn.close()   
    print(f"Строк загружено {counter}")

load_csv_to_database()