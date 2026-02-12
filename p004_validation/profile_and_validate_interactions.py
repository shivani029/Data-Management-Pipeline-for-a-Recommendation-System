import os
import pandas as pd
from datetime import datetime
import json

# Base path of raw interaction data
BASE_PATH = "data_lake/raw/interactions/csv"

# Where to store data quality reports
REPORT_DIR = "p005_data_quality_reports/interactions"
os.makedirs(REPORT_DIR, exist_ok=True)


def get_latest_file():
    all_files = []
    for root, dirs, files in os.walk(BASE_PATH):
        for file in files:
            if file.endswith(".csv"):
                full_path = os.path.join(root, file)
                all_files.append(full_path)

    if not all_files:
        raise Exception("No interaction CSV files found in the data lake.")

    # Pick the most recently modified file
    latest_file = max(all_files, key=os.path.getmtime)
    return latest_file


def profile_data(df):
    print("\n--- DATA PROFILING (INTERACTIONS) ---")
    print("Shape (rows, columns):", df.shape)
    print("\nNull values per column:")
    print(df.isnull().sum())
    print("\nUnique users:", df["user_id"].nunique())
    print("Unique items:", df["item_id"].nunique())
    print("\nEvent type distribution:")
    print(df["event_type"].value_counts())


def validate_data(df):
    print("\n--- DATA VALIDATION (INTERACTIONS) ---")

    issues = {}

    # Rating must be between 1 and 5 (if present)
    invalid_ratings = df[
        (df["rating"].notnull()) &
        ((df["rating"] < 1) | (df["rating"] > 5))
    ]
    issues["invalid_ratings"] = len(invalid_ratings)

    # Event type must be one of the allowed values
    valid_events = ["view", "click", "purchase", "rating"]
    invalid_events = df[~df["event_type"].isin(valid_events)]
    issues["invalid_events"] = len(invalid_events)

    # user_id and item_id should not be null
    missing_user_item = df[
        df["user_id"].isnull() | df["item_id"].isnull()
    ]
    issues["missing_user_or_item"] = len(missing_user_item)

    # Duplicate rows
    duplicates = df[df.duplicated()]
    issues["duplicate_rows"] = len(duplicates)

    for k, v in issues.items():
        print(f"{k}: {v}")

    return issues


def save_report(file_used, issues, df):
    report = {
        "dataset": "interactions",
        "file_used": file_used,
        "total_records": len(df),
        "null_counts": df.isnull().sum().to_dict(),
        "validation_results": issues,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(REPORT_DIR, f"dq_report_{timestamp}.json")

    with open(report_file, "w") as f:
        json.dump(report, f, indent=4)

    print(f"\nData Quality Report saved at: {report_file}")


if __name__ == "__main__":
    latest_file = get_latest_file()
    print(f"\nUsing latest raw file: {latest_file}")

    df = pd.read_csv(latest_file)

    profile_data(df)
    issues = validate_data(df)

    save_report(latest_file, issues, df)

    print("\nValidation completed and report generated.")
