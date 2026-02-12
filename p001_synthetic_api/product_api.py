from fastapi import FastAPI
import random
from datetime import datetime

app = FastAPI()

# Configuration
NUM_ITEMS = 200
CATEGORIES = ["Electronics", "Clothing", "Books", "Home", "Sports"]
BRANDS = ["BrandA", "BrandB", "BrandC", "BrandD"]

# Synthetic product universe
PRODUCTS = []

# Create synthetic products once when server starts
for i in range(1, NUM_ITEMS + 1):
    product = {
        "item_id": f"P{str(i).zfill(4)}",
        "name": f"Product_{i}",
        "category": random.choice(CATEGORIES),
        "price": round(random.uniform(100, 5000), 2),
        "brand": random.choice(BRANDS),
        "rating_avg": round(random.uniform(1, 5), 2),
        "popularity_score": round(random.uniform(0, 1), 2),
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    PRODUCTS.append(product)


@app.get("/products")
def get_all_products():
    return PRODUCTS


@app.get("/products/{item_id}")
def get_product(item_id: str):
    for p in PRODUCTS:
        if p["item_id"] == item_id:
            return p
    return {"error": "Product not found"}
