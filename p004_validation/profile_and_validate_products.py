import os
import json
from datetime import datetime

# Base path of raw product data
BASE_PATH = "data_lake/raw/products/api"

# Where to store data quality reports
REPORT_DIR = "p005_data_quality_reports/products"
os.makedirs(REPORT_DIR, exist_ok=True)


def get_latest_file():
    all_files = []
    for root, dirs, files in os.walk(BASE_PATH):
        for file in files:
            if file.endswith(".json"):
                full_path = os.path.join(root, file)
                all_files.append(full_path)

    if not all_files:
        raise Exception("No product JSON files found in the data lake.")

    # Pick the most recently modified file
    latest_file = max(all_files, key=os.path.getmtime)
    return latest_file


def profile_data(data):
    print("\n--- DATA PROFILING (PRODUCTS) ---")
    print("Total records:", len(data))

    categories = set(p["category"] for p in data)
    brands = set(p["brand"] for p in data)

    prices = [p["price"] for p in data]
    ratings = [p["rating_avg"] for p in data]
    popularity = [p["popularity_score"] for p in data]

    print("Unique categories:", categories)
    print("Unique brands:", brands)
    print("Price range:", min(prices), "to", max(prices))
    print("Rating Avg range:", min(ratings), "to", max(ratings))
    print("Popularity score range:", min(popularity), "to", max(popularity))


def validate_data(data):
    print("\n--- DATA VALIDATION (PRODUCTS) ---")

    issues = {
        "missing_item_id": 0,
        "missing_name": 0,
        "missing_category": 0,
        "invalid_price": 0,
        "invalid_rating_avg": 0,
        "invalid_popularity_score": 0
    }

    for p in data:
        # Mandatory fields
        if not p.get("item_id"):
            issues["missing_item_id"] += 1
        if not p.get("name"):
            issues["missing_name"] += 1
        if not p.get("category"):
            issues["missing_category"] += 1

        # Price must be positive
        if p["price"] <= 0:
            issues["invalid_price"] += 1

        # rating_avg must be between 1 and 5
        if p["rating_avg"] < 1 or p["rating_avg"] > 5:
            issues["invalid_rating_avg"] += 1

        # popularity_score must be between 0 and 1
        if p["popularity_score"] < 0 or p["popularity_score"] > 1:
            issues["invalid_popularity_score"] += 1

    for k, v in issues.items():
        print(f"{k}: {v}")

    return issues


def save_report(file_used, issues, data):
    report = {
        "dataset": "products",
        "file_used": file_used,
        "total_records": len(data),
        "validation_results": issues,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(REPORT_DIR, f"dq_report_{timestamp}.json")

    with open(report_file, "w") as f:
        json.dump(report, f, indent=4)

    print(f"\nProduct Data Quality Report saved at: {report_file}")


if __name__ == "__main__":
    latest_file = get_latest_file()
    print(f"\nUsing latest raw product file: {latest_file}")

    with open(latest_file, "r") as f:
        data = json.load(f)

    profile_data(data)
    issues = validate_data(data)

    save_report(latest_file, issues, data)

    print("\nProduct validation completed and report generated.")
