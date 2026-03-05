# DE ETL Pipeline

ETL пайплайн для загрузки и обработки данных с инкрементальной загрузкой, интеграцией с внешним API и генерацией отчётов.

## Скрипты

- `load_large_transactions.py` — инкрементальная загрузка транзакций из CSV в PostgreSQL. Фильтрует по сумме и статусу, обрабатывает дубликаты и битые строки.
- `report.py` — агрегация данных из PostgreSQL (SUM, COUNT, AVG по статусам) с сохранением результата в `report.csv`
- `weather_etl.py` — загрузка данных о погоде из Open-Meteo API в PostgreSQL с обработкой ошибок сети и структуры ответа

## Стек

- Python 3.13
- PostgreSQL 16
- psycopg2, python-dotenv, requests

## Установка

```bash
git clone https://github.com/BlackDeepSky/de-etl-pipeline
cd de-etl-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Настройка

Создай файл `.env` в корне проекта:

```
DB_HOST=localhost
DB_NAME=de_practice
DB_USER=postgres
DB_PASSWORD=your_password
```

## База данных

Создай таблицы в PostgreSQL перед запуском:

```sql
CREATE TABLE transactions (
    id INTEGER PRIMARY KEY,
    amount INTEGER,
    status VARCHAR(20)
);

CREATE TABLE weather (
    id SERIAL PRIMARY KEY,
    temperature FLOAT,
    windspeed FLOAT,
    collected_at TIMESTAMP DEFAULT NOW()
);
```

## Использование

```bash
# Загрузка транзакций
python load_large_transactions.py

# Генерация отчёта
python report.py

# Загрузка данных о погоде
python weather_etl.py
```
