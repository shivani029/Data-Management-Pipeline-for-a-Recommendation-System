from prefect import flow, task
import sys
import subprocess
import logging
import os

# --------------------------------------------------
# Orchestration Logging Setup
# --------------------------------------------------

LOG_DIR = "p012_orchestration/logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, "orchestration.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, mode="a"),
        logging.StreamHandler()
    ],
    force=True   # <-- VERY IMPORTANT for Prefect
)

logging.info("Orchestration logging initialized")



# --------------------------------------------------
# Utility to run python -m modules
# --------------------------------------------------

def run_module(module_name):
    msg = f"Running module: {module_name}"
    print(f"\n{msg}")
    logging.info(msg)

    result = subprocess.run(
        [sys.executable, "-m", module_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    # Log only, don't print twice
    logging.info(f"[{module_name}] STDOUT:\n{result.stdout}")

    if result.stderr:
        logging.error(f"[{module_name}] STDERR:\n{result.stderr}")

    if result.returncode != 0:
        raise RuntimeError(f"Module {module_name} failed")

    success_msg = f"Module {module_name} completed successfully"
    print(success_msg)
    logging.info(success_msg)




# --------------------------------------------------
# Prefect Tasks
# --------------------------------------------------

@task(retries=1, retry_delay_seconds=10)
def generate_interactions():
    run_module("p002_synthetic_data.generate_interactions")


@task(retries=2, retry_delay_seconds=30)
def ingest_interactions():
    run_module("p003_ingestion.ingest_interactions")


@task(retries=2, retry_delay_seconds=30)
def ingest_products():
    run_module("p003_ingestion.ingest_products_api")


@task
def validate_interactions():
    run_module("p004_validation.profile_and_validate_interactions")


@task
def validate_products():
    run_module("p004_validation.profile_and_validate_products")


@task
def generate_dq_pdf():
    run_module("p005_data_quality_reports.generate_dq_pdf")


@task
def prepare_interactions():
    run_module("p006_preparation.prepare_interactions")


@task
def prepare_products():
    run_module("p006_preparation.prepare_products")


@task
def build_features():
    run_module("p008_feature_engineering.build_features")


@task
def load_features_to_db():
    run_module("p008_feature_engineering.load_features_to_db")


@task
def train_logistic_model():
    run_module("p011_model_training.train_model")


@task
def train_svd_model():
    run_module("p011_model_training.train_svd_model")


# --------------------------------------------------
# Prefect Flow
# --------------------------------------------------

@flow(name="RecoMart_End_to_End_Pipeline")
def full_pipeline():

    # 1. Synthetic Data Generation
    gen = generate_interactions()

    # 2. Ingestion
    ing_int = ingest_interactions(wait_for=[gen])
    ing_prod = ingest_products(wait_for=[gen])

    # 3. Validation
    val_int = validate_interactions(wait_for=[ing_int])
    val_prod = validate_products(wait_for=[ing_prod])

    # 4. Data Quality PDF
    dq = generate_dq_pdf(wait_for=[val_int, val_prod])

    # 5. Data Preparation
    prep_int = prepare_interactions(wait_for=[dq])
    prep_prod = prepare_products(wait_for=[dq])

    # 6. Feature Engineering
    feats = build_features(wait_for=[prep_int, prep_prod])

    # 7. Load Feature Store to DB
    load_db = load_features_to_db(wait_for=[feats])

    # 8. Model Training
    train_logi = train_logistic_model(wait_for=[load_db])
    train_svd = train_svd_model(wait_for=[load_db])

    return {
        "generate": gen,
        "ingest_interactions": ing_int,
        "ingest_products": ing_prod,
        "validate_interactions": val_int,
        "validate_products": val_prod,
        "dq_pdf": dq,
        "prepare_interactions": prep_int,
        "prepare_products": prep_prod,
        "build_features": feats,
        "load_features_to_db": load_db,
        "train_logistic": train_logi,
        "train_svd": train_svd
    }


# --------------------------------------------------
# Run Manually
# --------------------------------------------------

if __name__ == "__main__":
    full_pipeline()
