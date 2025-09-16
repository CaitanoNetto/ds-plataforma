import numpy as np
from src.ingestion.generate_sales_data import generate_sales_data

np.random.seed(42)
df = generate_sales_data(10, random_state=42)
print(df.head())
assert len(df) == 10
required = {"sales_id", "store_id", "product_id",
            "ts", "qty", "price", "discount"}
assert required.issubset(df.columns)
