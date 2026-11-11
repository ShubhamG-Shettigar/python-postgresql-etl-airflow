# Python PostgreSQL ETL Pipeline with Apache Airflow

## Overview
A Python-based ETL project that extracts order data from CSV, validates and transforms records, and loads them into PostgreSQL.

An Apache Airflow DAG is also included to demonstrate workflow orchestration.

## Tech Stack
- Python
- PostgreSQL
- Psycopg
- Apache Airflow
- python-dotenv

## Features
- CSV data extraction.
- Data validation and rejection handling.
- Data transformation using Python.
- PostgreSQL data loading.
- JSONB storage for rejected records.
- Idempotent loading using UPSERT.
- Post-load data quality checks.
- Airflow DAG with task dependencies, XCom and retries.

## Project Structure

```text
├── dags/
│   └── orders_etl_dag.py
├── extract.py
├── validate.py
├── transform.py
├── load.py
├── config.py
├── logger.py
├── main.py
├── orders.csv
├── .env.example
├── .gitignore
└── requirements.txt
```

## Setup

1. Clone the repository.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create a PostgreSQL database named `ecommerce_analytics`.
4. Create the required tables: `etl_orders` and `etl_rejected_orders`.
5. Create a `.env` file using `.env.example` and configure database credentials.
6. Run the pipeline:

```bash
python main.py
```

## Expected Results

The sample CSV contains 13 records:
- 9 valid records.
- 