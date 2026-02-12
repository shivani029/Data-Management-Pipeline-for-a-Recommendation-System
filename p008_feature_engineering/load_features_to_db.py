import os
import psycopg2
import pandas as pd
from p010_lineage.log_lineage import log_pipeline_run

FEATURE_STORE_PATH = "p009_feature_store/data"
TABLE_NAME = "feature_store"

DB_CONFIG = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# Validate environment variables
for k, v in DB_CONFIG.items():
    if v is None:
        raise Exception(f"Environment variable {k} is not set.")


def get_latest_feature_file():
    files = [
        os.path.join(FEATURE_STORE_PATH, f)
        for f in os.listdir(FEATURE_STORE_PATH)
        if f.endswith(".csv")
    ]
    if not files:
        raise Exception("No feature CSV files found.")
    return max(files, key=os.path.getmtime)


def pandas_type_to_postgres(dtype):
    if "int" in str(dtype):
        return "INTEGER"
    elif "float" in str(dtype):
        return "DOUBLE PRECISION"
    else:
        return "TEXT"


def create_table_if_not_exists(conn, df):
    columns = []
    for col, dtype in zip(df.columns, df.dtypes):
        pg_type = pandas_type_to_postgres(dtype)
        columns.append(f'"{col}" {pg_type}')

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        {", ".join(columns)}
    );
    """

    cur = conn.cursor()
    cur.execute(create_table_sql)
    conn.commit()
    cur.close()

    print(f"Table '{TABLE_NAME}' checked/created successfully.")


def load_to_db(csv_file):
    print(f"\nLoading feature file into database: {csv_file}")
    df = pd.read_csv(csv_file)

    print("Columns detected in CSV:")
    print(list(df.columns))
    print(f"Total rows to insert: {len(df)}")

    conn = psycopg2.connect(**DB_CONFIG)

    # Create table dynamically if not exists
    create_table_if_not_exists(conn, df)

    cur = conn.cursor()

    columns = list(df.columns)
    placeholders = ", ".join(["%s"] * len(columns))
    column_names = ", ".join([f'"{c}"' for c in columns])

    insert_query = f"""
        INSERT INTO {TABLE_NAME} ({column_names})
        VALUES ({placeholders})
    """

    inserted = 0
    for _, row in df.iterrows():
        cur.execute(insert_query, tuple(row))
        inserted += 1

        if inserted % 500 == 0:
            print(f"{inserted} rows inserted...")

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nFeature store updated successfully.")
    print(f"Total rows inserted: {inserted}")


if __name__ == "__main__":
    print("\n=== LOADING FEATURE STORE DATA INTO DATABASE ===")

    # 1. Get latest feature CSV
    latest_csv = get_latest_feature_file()
    print(f"Latest feature store file detected: {latest_csv}")

    # 2. Load into PostgreSQL
    load_to_db(latest_csv)

    # 3. Log lineage
    log_pipeline_run(
        stage="load_features_to_db",
        input_files=[latest_csv],
        output_files=[f"PostgreSQL Table: {TABLE_NAME}"]
    )

    print("\n=== FEATURE STORE LOAD COMPLETED ===")
