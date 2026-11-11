from extract import extract_data
from validate import validate_record
from transform import transform_record
from load import load_valid_records, load_rejected_records
from logger import logger

logger.info("ETL pipeline started")
records = extract_data("orders.csv")
logger.info(f"Extracted {len(records)} records")

transformed_records = []
rejected_records = []

for record in records:
    is_valid, reason = validate_record(record)

    if is_valid:
        transformed_records.append(transform_record(record))
    else:
        rejected_records.append({
            "record": record,
            "reason": reason
        })

print("Valid records:", len(transformed_records))
print("Rejected records:", len(rejected_records))
print("Rejected data:", rejected_records)

# After validation and transformation
logger.info(f"Valid records: {len(transformed_records)}")
logger.warning(f"Rejected records: {len(rejected_records)}")


load_valid_records(transformed_records)
load_rejected_records(rejected_records)

# After loading
logger.info("Valid records loaded into PostgreSQL")
logger.info("Rejected records loaded into PostgreSQL")

logger.info("ETL pipeline completed")

