import os
import pandas as pd
from datetime import datetime
from p010_lineage.log_lineage import log_pipeline_run

RAW_BASE_PATH = "data_lake/raw/interactions/csv"
PREPARED_BASE_PATH = "data_lake/prepared/interactions"


def get_latest_raw_file():
    all_files = []
    for root, dirs, files in os.walk(RAW_BASE_PATH):
        for file in files:
            if file.endswith(".csv"):
                all_files.append(os.path.join(root, file))

    if not all_files:
        raise Exception("No raw interaction files found.")

    return max(all_files, key=os.path.getmtime)


def prepare_directories():
    today = datetime.now()
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")

    prepared_path = os.path.join(PREPARED_BASE_PATH, year, month, day)
    os.makedirs(prepared_path, exist_ok=True)
    return prepared_path


def clean_and_prepare(df):
    print("\n================ DATA PREPARATION : INTERACTIONS =================")

    print("Initial shape:", df.shape)

    # Step 1: Remove duplicate rows
    print("Step 1: Removing duplicate records")
    before = len(df)
    df = df.drop_duplicates()
    print(f"Duplicates removed: {before - len(df)}")

    # Step 2: Handle missing ratings
    print("Step 2: Handling missing ratings (only rating events must have rating)")
    rating_mask = (df["event_type"] == "rating") & (df["rating"].isnull())
    missing_ratings = rating_mask.sum()
    df = df[~rating_mask]
    print(f"Invalid rating records removed: {missing_ratings}")

    # Step 3: Ensure rating range
    print("Step 3: Validating rating range (1 to 5)")
    invalid_ratings = df[(df["rating"] < 1) | (df["rating"] > 5)]
    df = df[~df.index.isin(invalid_ratings.index)]
    print(f"Invalid rating values removed: {len(invalid_ratings)}")

    # Step 4: Convert timestamp
    print("Step 4: Converting timestamp to datetime format")
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    print("Final shape after cleaning:", df.shape)
    print("================ DATA PREPARATION COMPLETED =================\n")

    return df


def save_prepared_file(df, prepared_dir):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"interactions_prepared_{timestamp}.csv"
    path = os.path.join(prepared_dir, filename)
    df.to_csv(path, index=False)
    print(f"Prepared interactions file saved at: {path}")
    return path


if __name__ == "__main__":
    print("\n=== INTERACTIONS DATA PREPARATION PIPELINE STARTED ===")

    # 1. Get latest raw file
    raw_file = get_latest_raw_file()
    print(f"Using raw file: {raw_file}")

    # 2. Load data
    df = pd.read_csv(raw_file)

    # 3. Clean and prepare
    prepared_df = clean_and_prepare(df)

    # 4. Prepare directory
    prepared_dir = prepare_directories()

    # 5. Save prepared file
    prepared_file_path = save_prepared_file(prepared_df, prepared_dir)

    # 6. Log lineage
    log_pipeline_run(
        stage="prepare_interactions",
        input_files=[raw_file],
        output_files=[prepared_file_path]
    )

    print("\n=== INTERACTIONS DATA PREPARATION PIPELINE COMPLETED ===")
