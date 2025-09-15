import pandas as pd

df = pd.read_parquet("data/bronze/sales", engine="pyarrow")
print(df.head())
print(df.dtypes)
print("linhas", len(df))
