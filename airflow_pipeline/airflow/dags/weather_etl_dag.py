from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def extract_weather():
    try:
        response = requests.get("https://api.open-meteo.com/v1/forecast?latitude=55.75&longitude=37.62&current_weather=true")
        if response.status_code == 200:
            data = response.json()
        else:
            print(f"API вернул ошибку: {response.status_code}")
    except KeyError:
        print(f"найдена ошибка {KeyError}")
    except ConnectionError:
        print("API not allowed")
    return data

def transform_weather(ti):
    data = ti.xcom_pull(task_ids = "extract_weather")
    result = {"temperature": data["current_weather"]["temperature"], "windspeed": data["current_weather"]["windspeed"]}
    return result

def load_weather(ti):
    data = ti.xcom_pull(task_ids="transform_weather")

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

    cursor = conn.cursor()

    cursor.execute(
                "INSERT INTO weather (temperature, windspeed ) VALUES (%s, %s)",
                (data["temperature"], data["windspeed"])
            )
    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id = "weather_etl",
    default_args = default_args,
    start_date = datetime(2026, 3, 8),
    schedule = "@daily",
    catchup = False
) as dag:
    
    task1 = PythonOperator(
        task_id = "extract_weather",
        python_callable = extract_weather,
    )
    task2 = PythonOperator(
            task_id = "transform_weather",
            python_callable = transform_weather,
    )
    task3 = PythonOperator(
        task_id = "load_weather",
        python_callable = load_weather,
    )

task1 >> task2 >> task3