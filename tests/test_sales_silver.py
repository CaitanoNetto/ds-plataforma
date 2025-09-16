from src.transform.sales_silver import build_sales_silver
import pyarrow.dataset as ds

path = build_sales_silver(overwrite=True)
print("Silver em:", path)

d = ds.dataset(path, format="parquet", partitioning="hive")
df = d.to_table().to_pandas()
print(df.head())

assert {"revenue", "year", "month", "day"}.issubset(df.columns)
assert (df["revenue"] >= 0).all()
assert df["ts"].between("2023-01-01", df["ts"].max()).all()
