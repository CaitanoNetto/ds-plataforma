from src.ingestion.generate_sales_data import generate_sales_data
from src.ingestion.save_sales_parquet import save_sales_parquet

df = generate_sales_data(50)

base_path = save_sales_parquet(df)

print("Dados salvoes em:", base_path)
