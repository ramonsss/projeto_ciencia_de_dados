"""
Wrapper de compatibilidade para scripts/silver/clean_ibge_populacao.py
"""
import sys
from pathlib import Path

silver_dir = Path(__file__).resolve().parent / "silver"
if str(silver_dir) not in sys.path:
    sys.path.insert(0, str(silver_dir))

from clean_ibge_populacao import process_silver_ibge

if __name__ == "__main__":
    process_silver_ibge()
