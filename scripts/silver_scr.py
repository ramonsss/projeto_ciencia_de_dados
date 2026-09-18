"""
Wrapper de compatibilidade para scripts/silver/clean_scr_credito.py
"""
import sys
from pathlib import Path

silver_dir = Path(__file__).resolve().parent / "silver"
if str(silver_dir) not in sys.path:
    sys.path.insert(0, str(silver_dir))

from clean_scr_credito import process_silver_scr

if __name__ == "__main__":
    process_silver_scr()
