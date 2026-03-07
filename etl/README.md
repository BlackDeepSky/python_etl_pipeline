# DE ETL Pipeline

Учебный Data Engineering проект. Включает ETL пайплайны, интеграцию с API и построение аналитического хранилища (DWH) на PostgreSQL.

---

## Проекты

### 📂 ETL Pipeline (корневая папка)

Инкрементальная загрузка данных из CSV и внешнего API в PostgreSQL.

| Скрипт | Описание |
|---|---|
| `load_large_transactions.py` | Инкрементальная загрузка транзакций из CSV. Фильтрация по сумме и статусу, обработка дублей и битых строк |
| `report.py` | Агрегация данных из PostgreSQL (SUM, COUNT, AVG по статусам), сохранение результата в `report.csv` |
| `weather_etl.py` | Загрузка погоды из Open-Meteo API в PostgreSQL с обработкой ошибок сети и структуры ответа |

---

### 📂 [DWH — Аналитическое хранилище](./dwh/)

Star Schema на PostgreSQL для аналитики продаж интернет-магазина.

**Схема данных:**
- `dim_customer` — покупатели (регион, пол, дата рождения)
- `dim_product` — товары (название, категория)
- `dim_date` — календарь (дата, месяц, квартал, год)
- `fact_sales` — факты продаж (выручка, количество, FK на все dimensions)

| Файл | Описание |
|---|---|
| `load_to_database.py` | ETL скрипт: загрузка CSV → PostgreSQL с инкрементальной логикой |
| `schema.sql` | DDL всех таблиц Star Schema |
| `queries.sql` | Аналитические запросы: выручка по категориям, топ регионов, средний чек по возрастам |
| `customers.csv` | Тестовые данные — покупатели |
| `products.csv` | Тестовые данные — товары |
| `sales.csv` | Тестовые данные — продажи |

---

## Стек

- Python 3.13
- PostgreSQL 16
- psycopg2, python-dotenv, requests

---

## Установка

```bash
git clone https://github.com/BlackDeepSky/python_etl_pipeline
cd python_etl_pipeline/etl
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## Настройка

Создай файл `.env` в нужной папке:

```
DB_HOST=localhost
DB_NAME=de_practice
DB_USER=postgres
DB_PASSWORD=your_password
```

---

## Использование

**ETL Pipeline:**
```bash
python load_large_transactions.py
python report.py
python weather_etl.py
```

**DWH:**
```bash
cd dwh
# Создать схему в PostgreSQL (выполнить schema.sql в DBeaver)
python load_to_database.py
```
