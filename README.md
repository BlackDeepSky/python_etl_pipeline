# Data Engineering Portfolio

Учебные проекты по Data Engineering: ETL пайплайны и аналитическое хранилище на PostgreSQL.

---

## Проекты

### 📂 [etl/](./etl/) — ETL Pipeline

Инкрементальная загрузка данных из CSV и внешнего API в PostgreSQL.

| Скрипт | Описание |
|---|---|
| `load_large_transactions.py` | Загрузка транзакций из CSV с фильтрацией и обработкой дублей |
| `report.py` | Агрегация данных и экспорт отчёта в CSV |
| `current_weather.py` | Загрузка погоды из Open-Meteo API в PostgreSQL |

**Стек:** Python 3.13, PostgreSQL 16, psycopg2, python-dotenv, requests

---

### 📂 [dwh/](./dwh/) — DWH (Star Schema)

Аналитическое хранилище для продаж интернет-магазина. Star Schema на PostgreSQL с ETL загрузкой и аналитическими запросами.

| Файл | Описание |
|---|---|
| `load_to_database.py` | ETL скрипт: CSV → PostgreSQL с инкрементальной загрузкой |
| `schema.sql` | DDL таблиц: fact_sales, dim_date, dim_product, dim_customer |
| `queries.sql` | Аналитика: выручка по категориям, топ регионов, средний чек |

**Стек:** Python 3.13, PostgreSQL 16, psycopg2, python-dotenv

---

## Установка

```bash
git clone https://github.com/BlackDeepSky/python_etl_pipeline.git
cd python_etl_pipeline

# ETL проект
cd etl
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# DWH проект
cd ../dwh
python3 -m venv venv
source venv/bin/activate
pip install psycopg2-binary python-dotenv
```

## Настройка

Создай `.env` в нужной папке:

```
DB_HOST=localhost
DB_NAME=de_practice   # для etl / de_dwh для dwh
DB_USER=postgres
DB_PASSWORD=your_password
```