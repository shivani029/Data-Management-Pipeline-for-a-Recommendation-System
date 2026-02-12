import os
import json
import logging
import requests
from datetime import datetime

# API endpoint
API_URL = "http://127.0.0.1:8000/products"

# Data lake base path
DATA_LAKE_BASE = "data_lake/raw/products/api"

# Log directory
LOG_DIR = "p003_ingestion/logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Logging setup
log_file = os.path.join(LOG_DIR, "ingest_products_api.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def ingest():
    print("\n=== PRODUCT API INGESTION STARTED ===")
    try:
        print(f"Calling Product API: {API_URL}")
        response = requests.get(API_URL)
        response.raise_for_status()

        data = response.json()
        print(f"Fetched {len(data)} product records from API")

        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        timestamp = now.strftime("%Y%m%d_%H%M%S")

        target_dir = os.path.join(DATA_LAKE_BASE, year, month, day)
        os.makedirs(target_dir, exist_ok=True)

        file_name = f"products_{timestamp}.json"
        target_path = os.path.join(target_dir, file_name)

        with open(target_path, "w") as f:
            json.dump(data, f, indent=4)

        success_msg = (
            f"SUCCESS: Ingested product data to {target_path} "
            f"(records: {len(data)})"
        )

        print(success_msg)
        logging.info(success_msg)

    except Exception as e:
        error_msg = f"FAILED: {str(e)}"
        print(error_msg)
        logging.error(error_msg)

    print("=== PRODUCT API INGESTION COMPLETED ===\n")


if __name__ == "__main__":
    ingest()
