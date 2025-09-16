from src.transform.sales_gold import build_sales_gold
import pyarrow.dataset as ds

path = build_sales_gold(overwrite=True)
print("Gold em:", path)

d = ds.dataset(path, format="parquet", partitioning="hive")
df = d.to_table().to_pandas()
print(df.head())

for c in ["daily_qty", "target_qty_tplus1", "lag_qty_1"]:
    assert c in df.columns, f"faltou coluna {c}"
assert (df["target_qty_tplus1"] >= 0).all()
