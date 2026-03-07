# DWH — Аналитическое хранилище

Star Schema на PostgreSQL для аналитики продаж интернет-магазина.

---

## Схема данных

```
fact_sales
├── order_id      PK
├── date_id    → dim_date
├── product_id → dim_product
├── customer_id→ dim_customer
├── amount
└── quantity

dim_date        dim_product     dim_customer
├── date_id     ├── product_id  ├── customer_id
├── full_date   ├── name        ├── sex
├── month       └── category   ├── birth_date
├── year                       └── region
└── quarter
```

---

## Файлы

| Файл | Описание |
|---|---|
| `load_to_database.py` | ETL скрипт: CSV → PostgreSQL с инкрементальной загрузкой |
| `schema.sql` | DDL всех таблиц Star Schema |
| `queries.sql` | Аналитические запросы по витрине |
| `customers.csv` | Тестовые данные — покупатели |
| `products.csv` | Тестовые данные — товары |
| `sales.csv` | Тестовые данные — продажи |

---

## Установка

```bash
python3 -m venv venv
source venv/bin/activate
pip install psycopg2-binary python-dotenv
```

---

## Настройка

Создай `.env` файл:

```
DB_HOST=localhost
DB_NAME=de_dwh
DB_USER=postgres
DB_PASSWORD=your_password
```

---

## Запуск

```bash
# 1. Создать схему — выполнить schema.sql в DBeaver
# 2. Загрузить данные
python load_to_database.py
```

---

## Аналитические запросы

```sql
-- Выручка по категориям за 2024 год
SELECT dp.category, SUM(amount) AS summ
FROM fact_sales
JOIN dim_product dp USING(product_id)
JOIN dim_date dd USING(date_id)
WHERE dd.year = 2024
GROUP BY dp.category
ORDER BY summ DESC;

-- Топ-3 региона по количеству заказов
SELECT dc.region, COUNT(*) AS count_orders
FROM fact_sales
JOIN dim_customer dc USING(customer_id)
GROUP BY dc.region
ORDER BY count_orders DESC
LIMIT 3;

-- Средний чек по возрастным группам
SELECT ROUND(AVG(amount), 2) AS avg_amount,
  CASE
    WHEN EXTRACT(YEAR FROM AGE(dc.birth_date)) < 25 THEN '<25'
    WHEN EXTRACT(YEAR FROM AGE(dc.birth_date)) BETWEEN 25 AND 34 THEN '25-34'
    WHEN EXTRACT(YEAR FROM AGE(dc.birth_date)) BETWEEN 35 AND 44 THEN '35-44'
    WHEN EXTRACT(YEAR FROM AGE(dc.birth_date)) > 45 THEN '45+'
  END AS age_category
FROM fact_sales
JOIN dim_customer dc USING(customer_id)
GROUP BY age_category;
```

---

## Стек

- Python 3.13
- PostgreSQL 16
- psycopg2, python-dotenv