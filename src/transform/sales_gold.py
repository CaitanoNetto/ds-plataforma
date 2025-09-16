import pandas as pd
from pathlib import Path
import pyarrow.dataset as ds
import shutil


def build_sales_gold(
        silver_dir: str = "data/silver/sales",
        gold_dir: str = "data/gold/sales_ml_ready",

) -> str:
    dataset = ds.dataset(silver_dir, format="parquet", partitioning="hive")
    df = dataset.to_table().to_pandas()

    required = {"ts", "store_id", "product_id",
                "qty", "price", "discount", "revenue"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Faltando colunas no silver: {sorted(missing)}")

    daily = (
        df.groupby(["ts", "store_id", "product_id"], as_index=False)
        .agg(
            daily_qty=("qty", "sum"),
            daily_revenue=("revenue", "sum"),
            avg_price=("price", "mean"),
            avg_discount=("discount", "mean"),
        )
    )

    daily = daily.sort_values(["store_id", "product_id", "ts"])

    g = daily.groupby(["store_id", "product_id"], group_keys=False)

    daily["lag_qty_1"] = g["daily_qty"].shift(1)
    daily["lag_qty_7"] = g["daily_qty"].shift(7)
    daily["lag_revenue_1"] = g["daily_revenue"].shift(1)
    daily["lag_revenue_7"] = g["daily_revenue"].shift(7)

    daily["roll_qty_7"] = g["daily_qty"].rolling(
        7, min_periods=1).mean().reset_index(level=[0, 1], drop=True)
    daily["roll_qty_28"] = g["daily_qty"].rolling(
        28, min_periods=1).mean().reset_index(level=[0, 1], drop=True)
    daily["roll_rev_7"] = g["daily_revenue"].rolling(
        7, min_periods=1).mean().reset_index(level=[0, 1], drop=True)
    daily["roll_rev_28"] = g["daily_revenue"].rolling(
        28, min_periods=1).mean().reset_index(level=[0, 1], drop=True)

    daily["ts"] = pd.to_datetime(daily["ts"], errors="raise")
    daily["year"] = daily["ts"].dt.year
    daily["month"] = daily["ts"].dt.month
    daily["day"] = daily["ts"].dt.day
    daily["dow"] = daily["ts"].dt.dayofweek
    daily["is_weekend"] = daily["dow"].isin([5, 6])

    g = daily.groupby(["store_id", "product_id"], group_keys=False)
    daily["target_qty_tplus1"] = g["daily_qty"].shift(-1)

    daily = daily.dropna(subset=["target_qty_tplus1"])
    daily = daily.copy()

    for c in ["store_id", "product_id", "year", "month", "day", "dow"]:
        daily.loc[:, c] = daily[c].astype("int32")
    daily.loc[:, "is_weekend"] = daily["is_weekend"].astype(bool)

    cols = [
        "ts", "store_id", "product_id",
        "daily_qty", "daily_revenue", "avg_price", "avg_discount",
        "lag_qty_1", "lag_qty_7", "lag_revenue_1", "lag_revenue_7",
        "roll_qty_7", "roll_qty_28", "roll_rev_7", "roll_rev_28",
        "year", "month", "day", "dow", "is_weekend",
        "target_qty_tplus1",
    ]

    daily = daily[cols]

    if Path(gold_dir).exists():
        shutil.rmtree(gold_dir)

    Path(gold_dir).mkdir(parents=True, exist_ok=True)
    daily.to_parquet(
        gold_dir,
        engine="pyarrow",
        partition_cols=["year", "month"]
    )
    return gold_dir
