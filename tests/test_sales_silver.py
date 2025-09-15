from src.transform.sales_silver import build_sales_silver
import pandas as pd

path = build_sales_silver()

print("Silver em:", path)

df = pd.read_parquet(path, engine="pyarrow")
print(df.head())
print(df.dtypes)
print("linhas:", len(df))
print("revenue média:", df["revenue"].mean())
