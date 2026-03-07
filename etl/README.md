# ETL Pipeline

Инкрементальный ETL пайплайн для загрузки транзакций, генерации отчётов и интеграции с внешним API. PostgreSQL в качестве хранилища.

---

## Скрипты

| Скрипт | Описание |
|---|---|
| `load_large_transactions.py` | Инкрементальная загрузка транзакций из CSV в PostgreSQL. Фильтрация по сумме и статусу, обработка дублей и битых строк |
| `report.py` | Агрегация данных из PostgreSQL (SUM, COUNT, AVG по статусам), экспорт результата в `report.csv` |
| `current_weather.py` | Загрузка погоды из Open-Meteo API в PostgreSQL с обработкой ошибок сети и структуры ответа |

---

## Установка

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## Настройка

Создай `.env` файл:

```
DB_HOST=localhost
DB_NAME=de_practice
DB_USER=postgres
DB_PASSWORD=your_password
```

---

## База данных

Создай таблицы в PostgreSQL перед запуском:

```sql
CREATE TABLE transactions (
    id      INTEGER PRIMARY KEY,
    amount  INTEGER,
    status  VARCHAR(20)
);

CREATE TABLE weather (
    id           SERIAL PRIMARY KEY,
    temperature  FLOAT,
    windspeed    FLOAT,
    collected_at TIMESTAMP DEFAULT NOW()
);
```

---

## Использование

```bash
# Загрузка транзакций
python load_large_transactions.py

# Генерация отчёта
python report.py

# Загрузка погоды
python current_weather.py
```

---

## Стек

- Python 3.13
- PostgreSQL 16
- psycopg2, python-dotenv, requests
