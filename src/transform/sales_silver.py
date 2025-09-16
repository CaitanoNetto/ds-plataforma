import pandas as pd
from pathlib import Path
from datetime import datetime
from shutil import rmtree


def build_sales_silver(
        bronze_dir: str = "data/bronze/sales",
        silver_dir: str = "data/silver/sales",
        overwrite: bool = False,
) -> str:
    df = pd.read_parquet(bronze_dir, engine="pyarrow")
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df["sales_id"] = pd.to_numeric(
        df["sales_id"], errors="coerce").astype("Int64")
    for c in ["store_id", "product_id", "qty", "year", "month", "day"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["discount"] = pd.to_numeric(df["discount"], errors="coerce")

    df = df.dropna(
        subset=["ts", "sales_id", "store_id", "product_id", "qty", "price", "discount"])
    df[["sales_id", "store_id", "product_id", "qty", "year", "month", "day"]] = df[[
        "sales_id", "store_id", "product_id", "qty", "year", "month", "day"]].astype(int)

    start = pd.Timestamp(2023, 1, 1)
    end = pd.Timestamp(datetime.today().date())

    df = df[(df["qty"] > 0) & (df["price"] > 0)]
    df = df[(df["discount"] >= 0) & (df["discount"] <= 0.7)]
    df = df[(df["ts"] >= start) & (df["ts"] <= end)]

    df = df.sort_values(["ts", "sales_id"])
    df = df.drop_duplicates(subset=["sales_id"], keep="last")
    df["revenue"] = df["qty"] * df["price"] * (1 - df["discount"])

    df["price"] = df["price"].round(2)
    df["discount"] = df["discount"].round(3)
    df["revenue"] = df["revenue"].round(2)

    cols = [
        "sales_id", "store_id", "product_id", "ts", "qty", "price", "discount", "revenue", "year", "month", "day",
    ]
    df = df[cols]

    if Path(silver_dir).exists():
        if overwrite:
            rmtree(silver_dir)
        else:
            raise FileExistsError(
                f"Diretório já existe: {silver_dir}. Use overwrite=True se quiser sobrescrever.")

    Path(silver_dir).mkdir(parents=True, exist_ok=True)
    df.to_parquet(
        silver_dir,
        engine="pyarrow",
        partition_cols=["year", "month", "day"]
    )
    return silver_dir
