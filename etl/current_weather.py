import requests
import psycopg2
from psycopg2 import errors
from dotenv import load_dotenv
import os

load_dotenv()

def tosql_api_weather():
    conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )

    cursor = conn.cursor()

    try:
        response = requests.get("https://api.open-meteo.com/v1/forecast?latitude=55.75&longitude=37.62&current_weather=true")
        if response.status_code == 200:
            data = response.json()  # превращает JSON в словарь Python
            api_temperature = data["current_weather"]["temperature"]
            api_windspeed = data["current_weather"]["windspeed"]

            cursor.execute(
                "INSERT INTO weather (temperature, windspeed ) VALUES (%s, %s)",
                (api_temperature, api_windspeed)
            )
            conn.commit()
            print(f"Записано: temperature={api_temperature}, windspeed={api_windspeed}")
        else:
            print(f"API вернул ошибку: {response.status_code}")
    except KeyError:
        print(f"найдена ошибка {KeyError}")
    except ConnectionError:
        print("API not allowed")
    
    cursor.close()
    conn.close()

tosql_api_weather()
