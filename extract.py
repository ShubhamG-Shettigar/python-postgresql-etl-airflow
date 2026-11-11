import csv
from logger import logger

def extract_data(file_path):
    try:
        with open(file_path, mode="r", newline="") as file:
            reader = csv.DictReader(file)
            records = list(reader)

        return records

    except FileNotFoundError:
        logger.exception(f"CSV file -> {file_path} not found")
        raise