import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

# Get directory of this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Config
NUM_USERS = 500
NUM_ITEMS = 200
NUM_RECORDS = 20000
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

os.makedirs(OUTPUT_DIR, exist_ok=True)

users = [f"U{str(i).zfill(4)}" for i in range(1, NUM_USERS + 1)]
items = [f"P{str(i).zfill(4)}" for i in range(1, NUM_ITEMS + 1)]
event_types = ["view", "click", "purchase", "rating"]
devices = ["web", "mobile"]

data = []

base_time = datetime.now() - timedelta(days=1)

for _ in range(NUM_RECORDS):
    user = random.choice(users)
    item = random.choice(items)
    event = random.choice(event_types)
    device = random.choice(devices)
    session_id = f"S{random.randint(1000,9999)}"
    
    timestamp = base_time + timedelta(seconds=random.randint(0, 86400))

    rating = None
    if event == "rating":
        # Intentionally inject bad data sometimes
        if random.random() < 0.1:
            rating = random.choice([-1, 6, None])
        else:
            rating = random.randint(1,5)

    data.append([
        user, item, event, rating, timestamp, device, session_id
    ])

df = pd.DataFrame(data, columns=[
    "user_id", "item_id", "event_type", "rating",
    "timestamp", "device", "session_id"
])

# Inject duplicate rows
df = pd.concat([df, df.sample(10)], ignore_index=True)

file_name = f"interactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
file_path = os.path.join(OUTPUT_DIR, file_name)

df.to_csv(file_path, index=False)

print(f"Generated synthetic interaction file: {file_path}")
print(f"Total rows: {len(df)}")
