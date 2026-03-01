# ETL Pipeline — Transactions

## Описание
ETL пайплайн для загрузки транзакций из CSV в PostgreSQL с инкрементальной загрузкой и генерацией отчётов.

## Скрипты
- `load_large_transactions.py` — загрузка отчета в БД
- `report.py` — создание отчета по заданному SQL запросу

## Установка
```bash
git clone https://github.com/BlackDeepSky/de-etl-pipeline
cd de-etl-pipeline
pip install -r requirements.txt
```

## Использование
```
Создать базу данных и таблицу de_practice
Запуск load_large_transactions.py
Запуск report.py для создания отчета - report.csv
```
