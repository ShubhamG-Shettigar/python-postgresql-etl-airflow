import psycopg, json
from config import DB_CONFIG
from logger import logger

def load_valid_records(records):
    conn = None
    cursor = None

    try:
        conn = psycopg.connect(**DB_CONFIG)
        cursor = conn.cursor()

        query = """
            INSERT INTO etl_orders
(id, name, order_name, order_price, status)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (id)
DO UPDATE SET
    name = EXCLUDED.name,
    order_name = EXCLUDED.order_name,
    order_price = EXCLUDED.order_price,
    status = EXCLUDED.status
WHERE
    (etl_orders.name, etl_orders.order_name,
     etl_orders.order_price, etl_orders.status)
    IS DISTINCT FROM
    (EXCLUDED.name, EXCLUDED.order_name,
     EXCLUDED.order_price, EXCLUDED.status);
        """

        for record in records:
            cursor.execute(query, (
                record["id"],
                record["name"],
                record["order_name"],
                record["order_price"],
                record["status"]
            ))

        conn.commit()

    except Exception:
        logger.exception("Failed to load valid records")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    


def load_rejected_records(records):
    conn = psycopg.connect(**DB_CONFIG)
    cursor = conn.cursor()
    query = """
        INSERT INTO etl_rejected_orders
        (raw_record, rejection_reason)
        VALUES (%s, %s)
    """
    for item in records:
        cursor.execute(query, (
            json.dumps(item["record"]),
            item["reason"]
        ))
    conn.commit()
    cursor.close()
    conn.close()