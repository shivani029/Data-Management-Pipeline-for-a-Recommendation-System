import pickle
import pandas as pd
from pathlib import Path

# Load model
MODEL_PATH = "svd_model.pkl"

with open(MODEL_PATH, "rb") as f:
    model = pickle.load(f)

print("SVD model loaded.")

# Load latest interaction data
BASE_DATA_PATH = Path("data_lake/prepared/interactions")
latest_file = sorted(BASE_DATA_PATH.rglob("*.csv"))[-1]
df = pd.read_csv(latest_file)

# Show available users (optional, but helpful)
available_users = df["user_id"].astype(str).unique()
print(f"\nTotal users available: {len(available_users)}")
print("Sample users:", list(available_users[:10]))

# Take user input from terminal
user_id = input("\nEnter a user_id from the above list: ").strip()

if user_id not in available_users:
    print(f"User {user_id} not found in dataset.")
    print("Please rerun the script and enter a valid user_id.")
    exit()

print(f"\nGenerating recommendations for user: {user_id}")

# Get all unique items
all_items = df["item_id"].astype(str).unique()

# Predict ratings
predictions = []
for item in all_items:
    pred = model.predict(user_id, item)
    predictions.append((item, pred.est))

# Sort and show top 5 recommendations
top_n = sorted(predictions, key=lambda x: x[1], reverse=True)[:5]

print(f"\nTop recommendations for user {user_id}:")
for item, score in top_n:
    print(f"Item: {item}, Predicted rating: {round(score, 2)}")
