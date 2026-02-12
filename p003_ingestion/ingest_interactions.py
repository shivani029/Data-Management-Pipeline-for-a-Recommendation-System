import os
import shutil
from datetime import datetime
import logging

# Paths
SOURCE_DIR = "p002_synthetic_data/output"
DATA_LAKE_BASE = "data_lake/raw/interactions/csv"
LOG_DIR = "p003_ingestion/logs"

os.makedirs(LOG_DIR, exist_ok=True)

# Logging setup
log_file = os.path.join(LOG_DIR, "ingest_interactions.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def ingest():
    try:
        files = os.listdir(SOURCE_DIR)
        if not files:
            logging.info("No new interaction files to ingest.")
            print("No new interaction files to ingest.")
            return
        
        for file in files:
            source_path = os.path.join(SOURCE_DIR, file)

            now = datetime.now()
            year = now.strftime("%Y")
            month = now.strftime("%m")
            day = now.strftime("%d")

            target_dir = os.path.join(
                DATA_LAKE_BASE, year, month, day
            )
            os.makedirs(target_dir, exist_ok=True)

            target_path = os.path.join(target_dir, file)

            shutil.move(source_path, target_path)

            msg = f"SUCCESS: Ingested {file} to {target_path}"
            logging.info(msg)
            print(msg)

    except Exception as e:
        err = f"FAILED: {str(e)}"
        logging.error(err)
        print(err)


if __name__ == "__main__":
    ingest()
