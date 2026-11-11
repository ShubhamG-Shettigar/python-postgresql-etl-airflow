from decimal import Decimal

def transform_record(record):
    return {
        "id": int(record["id"]),
        "name": record["name"].strip(),
        "order_name": record["order_name"].strip(),
        "order_price": Decimal(record["order_price"]),
        "status": record["status"].strip().upper()
    }