import os
import psycopg2
import pandas as pd

DB_CONFIG = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

for k, v in DB_CONFIG.items():
    if v is None:
        raise Exception(f"Environment variable {k} is not set.")


def get_latest_features(limit=10):
    conn = psycopg2.connect(**DB_CONFIG)

    query = """
        SELECT *
        FROM feature_store
        ORDER BY feature_created_at DESC
        LIMIT %s;
    """

    df = pd.read_sql(query, conn, params=(limit,))
    conn.close()
    return df


if __name__ == "__main__":
    print("Fetching latest 5 feature rows from Feature Store DB:\n")
    print(get_latest_features(5))
