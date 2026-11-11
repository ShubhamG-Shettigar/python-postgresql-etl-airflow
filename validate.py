ALLOWED_STATUSES = {"PLACED", "SHIPPED", "DELIVERED", "CANCELLED"}

def validate_record(record):

    # Validate ID
    try:
        order_id = int(record["id"])
        if order_id <= 0:
            return False, "ID must be greater than zero"
    except (ValueError, TypeError):
        return False, "Invalid ID"

    # Validate customer name
    if not record["name"].strip():
        return False, "Customer name is missing"

    # Validate order name
    if not record["order_name"].strip():
        return False, "Order name is missing"

    # Validate price
    try:
        price = float(record["order_price"])
        if price <= 0:
            return False, "Order price must be greater than zero"
    except (ValueError, TypeError):
        return False, "Invalid order price"

    # Validate status
    status = record["status"].strip().upper()

    if status not in ALLOWED_STATUSES:
        return False, "Invalid order status"

    return True, "Valid record"