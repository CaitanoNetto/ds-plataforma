import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generate_sales_data(n: int = 1000, random_state: int | None = None) -> pd.DataFrame:
    rng = np.random.default_rng(random_state)
    sales_id = np.arange(1, n + 1)
    store_id = rng.integers(1, 11, size=n)
    product_id = rng.integers(1, 51, size=n)
    start_date = datetime(2023, 1, 1)
    end_date = datetime.today()
    days_range = (end_date - start_date).days
    ts = [start_date + timedelta(days=int(x))
          for x in rng.integers(0, days_range, size=n)]
    qty = rng.integers(1, 21, size=n)
    price = rng.uniform(10, 500, size=n)
    discount = rng.uniform(0, 0.3, size=n)

    df = pd.DataFrame({
        "sales_id": sales_id,
        "store_id": store_id,
        "product_id": product_id,
        "ts": ts,
        "qty": qty,
        "price": price,
        "discount": discount,
    })
    return df
