import os
import json
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler
from p010_lineage.log_lineage import log_pipeline_run

PREPARED_INTERACTIONS_PATH = "data_lake/prepared/interactions"
PREPARED_PRODUCTS_PATH = "data_lake/prepared/products"
FEATURE_STORE_PATH = "p009_feature_store/data"
os.makedirs(FEATURE_STORE_PATH, exist_ok=True)


def get_latest_file(base_path, extension):
    all_files = []
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith(extension):
                all_files.append(os.path.join(root, file))

    if not all_files:
        raise Exception(f"No files found in {base_path}")

    return max(all_files, key=os.path.getmtime)


def load_latest_interactions():
    path = get_latest_file(PREPARED_INTERACTIONS_PATH, ".csv")
    print(f"Using prepared interactions: {path}")
    return path, pd.read_csv(path)


def load_latest_products():
    path = get_latest_file(PREPARED_PRODUCTS_PATH, ".json")
    print(f"Using prepared products: {path}")
    with open(path, "r") as f:
        data = json.load(f)
    return path, pd.DataFrame(data)


def build_features(interactions_df, products_df):
    print("\n================ FEATURE ENGINEERING STARTED ================")

    # Step 1: Join interactions with product metadata
    print("Step 1: Joining interactions with product metadata (on item_id)")
    df = interactions_df.merge(products_df, on="item_id", how="left")
    print(f"Total records after join: {len(df)}")

    # Step 2: User Activity Frequency
    print("\nStep 2: Creating User Activity Frequency feature")
    df["user_activity_frequency"] = df.groupby("user_id")["item_id"].transform("count")

    # Step 3: Average Rating Per User
    print("\nStep 3: Creating Average Rating Per User feature")
    df["avg_rating_per_user"] = df.groupby("user_id")["rating"].transform("mean")

    # Step 4: Average Rating Per Item
    print("\nStep 4: Creating Average Rating Per Item feature")
    df["avg_rating_per_item"] = df.groupby("item_id")["rating"].transform("mean")

    # Step 5: Session Co-occurrence
    print("\nStep 5: Creating Session Co-occurrence feature")
    df["session_unique_items"] = df.groupby("session_id")["item_id"].transform("nunique")

    # Step 6: Session Interaction Count
    print("\nStep 6: Creating Session Interaction Count feature")
    df["session_interaction_count"] = df.groupby("session_id")["item_id"].transform("count")

    # Step 7: Price Bucket
    print("\nStep 7: Creating Price Bucket feature")
    df["price_bucket"] = pd.cut(
        df["price"],
        bins=[0, 500, 2000, 5000, 10000],
        labels=["low", "medium", "high", "premium"]
    )

    # Step 8: Is Rating Event
    print("\nStep 8: Creating Is-Rating-Event feature")
    df["is_rating_event"] = (df["event_type"] == "rating").astype(int)

    # Step 9: Popularity Score (already 0–1, keep copy)
    print("\nStep 9: Creating Normalized Popularity Score feature")
    df["popularity_score_norm"] = df["popularity_score"]

    # -------------------------------------------------
    # Step 10: Encode categorical variables
    # -------------------------------------------------
    print("\nStep 10: Encoding categorical features")
    categorical_cols = ["category", "brand", "price_bucket"]
    df = pd.get_dummies(df, columns=categorical_cols, prefix=categorical_cols)

    # -------------------------------------------------
    # Step 11: Normalize numerical variables
    # -------------------------------------------------
    print("\nStep 11: Normalizing numerical features")

    numeric_cols = [
        "price",
        "popularity_score",
        "user_activity_frequency",
        "avg_rating_per_user",
        "avg_rating_per_item",
        "session_unique_items",
        "session_interaction_count"
    ]

    scaler = MinMaxScaler()
    df[numeric_cols] = scaler.fit_transform(df[numeric_cols])

    print("\n================ FEATURE ENGINEERING COMPLETED ================")
    return df


def save_features(df):
    df["feature_created_at"] = datetime.now()
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"features_{timestamp}.csv"
    path = os.path.join(FEATURE_STORE_PATH, filename)

    df.to_csv(path, index=False)
    print(f"\nFeature store dataset saved at: {path}")
    return path


if __name__ == "__main__":
    print("\n=== FEATURE ENGINEERING PIPELINE STARTED ===")

    # Load prepared data
    latest_interactions_file, interactions = load_latest_interactions()
    latest_products_file, products = load_latest_products()

    # Build features
    features_df = build_features(interactions, products)

    # Save feature store CSV
    feature_csv_path = save_features(features_df)

    # Log lineage
    log_pipeline_run(
        stage="build_features",
        input_files=[latest_interactions_file, latest_products_file],
        output_files=[feature_csv_path]
    )

    print("\n=== FEATURE ENGINEERING PIPELINE COMPLETED ===")
