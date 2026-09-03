import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
QUARENTENA_DIR = DATA_DIR / "quarentena"

# Cria as pastas se não existirem
for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, QUARENTENA_DIR]:
    d.mkdir(parents=True, exist_ok=True)
