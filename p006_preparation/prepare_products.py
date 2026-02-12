import os
import json
from datetime import datetime
from p010_lineage.log_lineage import log_pipeline_run

RAW_BASE_PATH = "data_lake/raw/products/api"
PREPARED_BASE_PATH = "data_lake/prepared/products"


def get_latest_raw_file():
    all_files = []
    for root, dirs, files in os.walk(RAW_BASE_PATH):
        for file in files:
            if file.endswith(".json"):
                all_files.append(os.path.join(root, file))

    if not all_files:
        raise Exception("No raw product files found.")

    return max(all_files, key=os.path.getmtime)


def prepare_directories():
    today = datetime.now()
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")

    prepared_path = os.path.join(PREPARED_BASE_PATH, year, month, day)
    os.makedirs(prepared_path, exist_ok=True)
    return prepared_path


def clean_products(data):
    print("\n================ DATA PREPARATION : PRODUCTS =================")
    print(f"Initial number of records: {len(data)}")

    cleaned = []
    dropped = 0

    for p in data:
        valid = True

        # Step 1: Mandatory fields check
        if not p.get("item_id") or not p.get("name") or not p.get("category"):
            valid = False

        # Step 2: Price validation
        if p["price"] <= 0:
            valid = False

        # Step 3: Rating range validation
        if p["rating_avg"] < 1 or p["rating_avg"] > 5:
            valid = False

        # Step 4: Popularity score validation
        if p["popularity_score"] < 0 or p["popularity_score"] > 1:
            valid = False

        if valid:
            cleaned.append(p)
        else:
            dropped += 1

    print(f"Invalid product records dropped: {dropped}")
    print(f"Final number of clean product records: {len(cleaned)}")
    print("================ DATA PREPARATION COMPLETED =================")

    return cleaned


def save_prepared_data(cleaned_data, prepared_path):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"products_prepared_{timestamp}.json"
    file_path = os.path.join(prepared_path, filename)

    with open(file_path, "w") as f:
        json.dump(cleaned_data, f, indent=4)

    print(f"\nPrepared product dataset saved at: {file_path}")
    return file_path


if __name__ == "__main__":
    print("\n=== PRODUCTS DATA PREPARATION PIPELINE STARTED ===")

    # 1. Get latest raw file
    raw_file = get_latest_raw_file()
    print(f"Using latest raw file: {raw_file}")

    # 2. Load raw data
    with open(raw_file, "r") as f:
        data = json.load(f)

    # 3. Clean data
    cleaned_data = clean_products(data)

    # 4. Prepare output directory
    prepared_dir = prepare_directories()

    # 5. Save prepared file and capture path
    prepared_file_path = save_prepared_data(cleaned_data, prepared_dir)

    # 6. Log lineage
    log_pipeline_run(
        stage="prepare_products",
        input_files=[raw_file],
        output_files=[prepared_file_path]
    )

    print("\n=== PRODUCTS DATA PREPARATION PIPELINE COMPLETED ===")
