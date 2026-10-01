from credito_pa.bronze.source_db import seed_source_db
from credito_pa.config import load_settings

if __name__ == "__main__":
    stats = seed_source_db(load_settings())
    print(f"Seed concluído: {stats}")
