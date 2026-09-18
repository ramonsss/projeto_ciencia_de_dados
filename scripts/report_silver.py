"""
Ponto de entrada raiz para o Relatório da Camada Silver:
python scripts/report_silver.py
"""
import sys
from pathlib import Path

reports_dir = Path(__file__).resolve().parent / "reports"
if str(reports_dir) not in sys.path:
    sys.path.insert(0, str(reports_dir))

from report_silver import run_silver_report

if __name__ == "__main__":
    run_silver_report()
