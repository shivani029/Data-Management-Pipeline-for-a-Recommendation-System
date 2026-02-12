import os
import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score
from p010_lineage.log_lineage import log_pipeline_run

FEATURE_STORE_PATH = "p009_feature_store/data"

def get_latest_feature_file():
    files = [
        os.path.join(FEATURE_STORE_PATH, f)
        for f in os.listdir(FEATURE_STORE_PATH)
        if f.endswith(".csv")
    ]
    if not files:
        raise Exception("No feature CSV files found.")
    return max(files, key=os.path.getmtime)


def train_model(feature_file):
    print(f"\nUsing feature file: {feature_file}")
    df = pd.read_csv(feature_file)

    # Target: predict whether an interaction is a purchase
    df["target"] = (df["event_type"] == "purchase").astype(int)

    feature_cols = [
        "user_activity_frequency",
        "avg_rating_per_user",
        "avg_rating_per_item",
        "session_unique_items",
        "session_interaction_count",
        "is_rating_event",
        "popularity_score_norm"
    ]

    print("\nChecking missing values in feature columns:")
    print(df[feature_cols].isna().sum())

    # Handle missing values
    print("\nHandling missing values: filling NaNs with 0")
    df[feature_cols] = df[feature_cols].fillna(0)

    X = df[feature_cols]
    y = df["target"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = LogisticRegression(max_iter=200)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    acc = accuracy_score(y_test, y_pred)
    prec = precision_score(y_test, y_pred)
    rec = recall_score(y_test, y_pred)

    print("\nModel Performance:")
    print("Accuracy :", acc)
    print("Precision:", prec)
    print("Recall   :", rec)

    return model, acc, prec, rec


if __name__ == "__main__":
    print("\n=== MODEL TRAINING PIPELINE STARTED ===")

    latest_feature_file = get_latest_feature_file()

    mlflow.set_experiment("RecoMart_Recommender")

    with mlflow.start_run():
        model, acc, prec, rec = train_model(latest_feature_file)

        # Log parameters
        mlflow.log_param("model_type", "LogisticRegression")
        mlflow.log_param("max_iter", 200)

        # Log metrics
        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("precision", prec)
        mlflow.log_metric("recall", rec)

        # Log model
        mlflow.sklearn.log_model(model, "model")

        run_id = mlflow.active_run().info.run_id
        print(f"\nMLflow Run ID: {run_id}")

    # Lineage logging
    log_pipeline_run(
        stage="model_training",
        input_files=[latest_feature_file],
        output_files=[f"MLflow model run_id={run_id}"]
    )

    print("\n=== MODEL TRAINING PIPELINE COMPLETED ===")
