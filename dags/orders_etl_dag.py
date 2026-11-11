import sys
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from extract import extract_data
from validate import validate_record
from transform import transform_record
from decimal import Decimal
from load import load_valid_records, load_rejected_records
from config import DB_CONFIG
import psycopg

default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}


def extract_task_function():
    records = extract_data(str(CSV_PATH))
    print(f"Extracted {len(records)} records")
    return records

def validate_task_function(ti):
    records = ti.xcom_pull(task_ids="extract")
    valid_records = []
    rejected_records = []
    for record in records:
        is_valid, reason = validate_record(record)
        if is_valid:
            valid_records.append(record)
        else:
            rejected_records.append({
                "record": record,
                "reason": reason
            })
    print(f"Valid records: {len(valid_records)}")
    print(f"Rejected records: {len(rejected_records)}")
    return {
        "valid_records": valid_records,
        "rejected_records": rejected_records
    }


def transform_task_function(ti):
    validation_result = ti.xcom_pull(task_ids="validate")
    valid_records = validation_result["valid_records"]
    transformed_records = []
    for record in valid_records:
        transformed = transform_record(record)
        # Decimal is not JSON serializable by default
        transformed["order_price"] = str(transformed["order_price"])
        transformed_records.append(transformed)
    return {
        "transformed_records": transformed_records,
        "rejected_records": validation_result["rejected_records"]
    }


def load_task_function(ti):
    result = ti.xcom_pull(task_ids="transform")
    valid_records = result["transformed_records"]
    rejected_records = result["rejected_records"]
    for record in valid_records:
        record["order_price"] = Decimal(record["order_price"])
    load_valid_records(valid_records)
    load_rejected_records(rejected_records)
    print(f"Loaded valid records: {len(valid_records)}")
    print(f"Loaded rejected records: {len(rejected_records)}")
    return {
        "loaded_valid": len(valid_records),
        "loaded_rejected": len(rejected_records)
    }
    
    
def quality_check_task_function():
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            # Check valid records
            cursor.execute("""
                SELECT COUNT(*)
                FROM etl_orders
                WHERE id BETWEEN 101 AND 113
                AND id NOT IN (104, 105, 106, 108)
            """)
            valid_count = cursor.fetchone()[0]
            # Check distinct rejected records
            cursor.execute("""
                SELECT COUNT(DISTINCT raw_record->>'id')
                FROM etl_rejected_orders
                WHERE raw_record->>'id' IN ('104', '105', '106', '108')
            """)
            rejected_count = cursor.fetchone()[0]
    print(f"Quality check - Valid: {valid_count}, Rejected: {rejected_count}")
    if valid_count != 9 or rejected_count != 4:
        raise ValueError("Data quality check failed!")
    print("Data quality check passed!")
    return True


with DAG(
    dag_id="orders_etl_pipeline",
    default_args=default_args,
    description="CSV to PostgreSQL ETL pipeline",
    start_date=datetime(2026, 11, 9),
    schedule=None,
    catchup=False
) as dag:

    
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_task_function
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=validate_task_function
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_task_function
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_task_function
    )

    quality_check_task = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check_task_function
    )
    extract_task >> validate_task >> transform_task >> load_task >> quality_check_task
