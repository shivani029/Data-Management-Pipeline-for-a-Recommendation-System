import os
import mlflow
import pandas as pd
from pathlib import Path
from datetime import datetime
import pickle
from collections import defaultdict

from surprise import Dataset, Reader, SVD
from surprise.model_selection import train_test_split
from surprise import accuracy

# PDF generation
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer

# --------------------------------------------------
# Configuration
# --------------------------------------------------

EXPERIMENT_NAME = "RecoMart_Recommender_SVD"
BASE_DATA_PATH = "data_lake/prepared/interactions"
REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

# --------------------------------------------------
# Utility: Get latest interaction file automatically
# --------------------------------------------------

def get_latest_interaction_file(base_path=BASE_DATA_PATH):
    base = Path(base_path)

    years = sorted([p for p in base.iterdir() if p.is_dir()])
    if not years:
        raise FileNotFoundError(f"No year folders found in {base}")
    latest_year = years[-1]

    months = sorted([p for p in latest_year.iterdir() if p.is_dir()])
    if not months:
        raise FileNotFoundError(f"No month folders found in {latest_year}")
    latest_month = months[-1]

    days = sorted([p for p in latest_month.iterdir() if p.is_dir()])
    if not days:
        raise FileNotFoundError(f"No day folders found in {latest_month}")
    latest_day = days[-1]

    csv_files = sorted(latest_day.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV file found in {latest_day}")

    latest_file = csv_files[-1]
    print(f"Using interaction data from: {latest_file}")
    return latest_file


# --------------------------------------------------
# Evaluation: Precision@K and Recall@K
# --------------------------------------------------

def precision_recall_at_k(predictions, k=5, threshold=3.5):
    user_est_true = defaultdict(list)

    for uid, iid, true_r, est, _ in predictions:
        user_est_true[uid].append((est, true_r))

    precisions = []
    recalls = []

    for uid, user_ratings in user_est_true.items():
        # Sort by estimated rating
        user_ratings.sort(key=lambda x: x[0], reverse=True)

        # Top K predictions
        top_k = user_ratings[:k]

        # Number of relevant items
        n_rel = sum((true_r >= threshold) for (_, true_r) in user_ratings)

        # Number of relevant & recommended in top K
        n_rel_and_rec_k = sum((true_r >= threshold) for (_, true_r) in top_k)

        precision = n_rel_and_rec_k / k if k else 0
        recall = n_rel_and_rec_k / n_rel if n_rel else 0

        precisions.append(precision)
        recalls.append(recall)

    avg_precision = sum(precisions) / len(precisions) if precisions else 0
    avg_recall = sum(recalls) / len(recalls) if recalls else 0

    return avg_precision, avg_recall


# --------------------------------------------------
# Main Training Pipeline
# --------------------------------------------------

if __name__ == "__main__":

    # ---------------------------
    # Load latest prepared data
    # ---------------------------
    print("Locating latest interaction data...")
    DATA_PATH = get_latest_interaction_file()

    print("Loading interaction data...")
    df = pd.read_csv(DATA_PATH)

    print("Raw data preview:")
    print(df.head())

    # ---------------------------
    # Data cleaning
    # ---------------------------
    df = df[df["rating"].notna()]
    df["user_id"] = df["user_id"].astype(str)
    df["item_id"] = df["item_id"].astype(str)
    df["rating"] = df["rating"].astype(float)

    print(f"Total training rows after cleaning: {len(df)}")

    if df.empty:
        raise ValueError("No valid rating data found. Cannot train SVD model.")

    # ---------------------------
    # Surprise dataset preparation
    # ---------------------------
    reader = Reader(rating_scale=(df["rating"].min(), df["rating"].max()))
    data = Dataset.load_from_df(df[["user_id", "item_id", "rating"]], reader)

    trainset, testset = train_test_split(
        data,
        test_size=0.2,
        random_state=42
    )

    # ---------------------------
    # MLflow Setup
    # ---------------------------
    mlflow.set_experiment(EXPERIMENT_NAME)
    run_name = f"SVD_Run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    with mlflow.start_run(run_name=run_name) as run:

        # ---------------------------
        # Model Training
        # ---------------------------
        print("Training SVD model...")

        model = SVD(
            n_factors=100,
            n_epochs=20,
            lr_all=0.005,
            reg_all=0.02,
            random_state=42
        )

        model.fit(trainset)

        # ---------------------------
        # Evaluation
        # ---------------------------
        print("Evaluating model...")
        predictions = model.test(testset)

        rmse = accuracy.rmse(predictions)
        precision_at_5, recall_at_5 = precision_recall_at_k(
            predictions, k=5, threshold=3.5
        )

        print(f"Precision@5: {precision_at_5:.4f}")
        print(f"Recall@5: {recall_at_5:.4f}")

        # ---------------------------
        # Logging to MLflow
        # ---------------------------
        mlflow.log_param("model_type", "SVD")
        mlflow.log_param("n_factors", 100)
        mlflow.log_param("n_epochs", 20)
        mlflow.log_param("learning_rate", 0.005)
        mlflow.log_param("regularization", 0.02)
        mlflow.log_param("training_data_path", str(DATA_PATH))
        mlflow.log_param("training_rows", len(df))

        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("precision_at_5", precision_at_5)
        mlflow.log_metric("recall_at_5", recall_at_5)

        # ---------------------------
        # Save and log model artifact
        # ---------------------------
        model_path = "svd_model.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(model, f)

        mlflow.log_artifact(model_path, artifact_path="model")

        # --------------------------------------------------
        # Auto-generate PDF performance report
        # --------------------------------------------------
        report_path = REPORTS_DIR / f"model_report_{run_name}.pdf"

        doc = SimpleDocTemplate(
            str(report_path),
            pagesize=A4,
            rightMargin=50,
            leftMargin=50,
            topMargin=50,
            bottomMargin=50
        )

        styles = getSampleStyleSheet()
        story = []

        title = f"Model Training Report - {EXPERIMENT_NAME}"
        story.append(Paragraph(f"<b>{title}</b>", styles["Title"]))
        story.append(Spacer(1, 20))

        content = [
            f"Run Name: {run_name}",
            f"Run ID: {run.info.run_id}",
            f"Training Data Path: {DATA_PATH}",
            "",
            "Metrics:",
            f"RMSE: {rmse}",
            f"Precision@5: {precision_at_5}",
            f"Recall@5: {recall_at_5}",
            "",
            f"Generated At: {datetime.now()}"
        ]

        for line in content:
            story.append(Paragraph(line, styles["Normal"]))
            story.append(Spacer(1, 12))

        doc.build(story)

        print("\nSVD model training completed successfully.")
        print(f"RMSE: {rmse}")
        print(f"Precision@5: {precision_at_5}")
        print(f"Recall@5: {recall_at_5}")
        print("Model artifact and metrics logged in MLflow.")
        print(f"PDF report generated at: {report_path}")
        print("Each MLflow run = one model version.")
