from src.ingestion.generate_sales_data import generate_sales_data
from src.ingestion.save_sales_parquet import save_sales_parquet

df = generate_sales_data(50)
base_path = save_sales_parquet(df, overwrite=True)
print("Dados salvos em:", base_path)
