import pandas as pd
from pathlib import Path
from shutil import rmtree


def save_sales_parquet(
        df: pd.DataFrame,
        base_dir: str = "data/bronze/sales",
        overwrite: bool = False,
) -> str:
    required = {"sales_id", "store_id", "product_id",
                "ts", "qty", "price", "discount"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Colunas faltando: {sorted(missing)}")
    df["ts"] = pd.to_datetime(df["ts"], errors="raise")
    df["year"] = df["ts"].dt.year
    df["month"] = df["ts"].dt.month
    df["day"] = df["ts"].dt.day

    if Path(base_dir).exists():
        if overwrite:
            rmtree(base_dir)
        else:
            raise FileExistsError(
                f"Diretório já existe: {base_dir}. Use overwrite=True se quiser sobrescrever.")

    Path(base_dir).mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        base_dir,
        engine="pyarrow",
        partition_cols=["year", "month", "day"]
    )

    return base_dir
