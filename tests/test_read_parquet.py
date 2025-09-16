import pandas as pd

df = pd.read_parquet("data/bronze/sales", engine="pyarrow")
print(df.head())
print("linhas", len(df))
