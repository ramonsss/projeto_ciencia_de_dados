"""
Ponto de entrada raiz para o Relatório da Camada Bronze:
python scripts/report_bronze.py
"""
import sys
from pathlib import Path

reports_dir = Path(__file__).resolve().parent / "reports"
if str(reports_dir) not in sys.path:
    sys.path.insert(0, str(reports_dir))

from report_bronze import run_bronze_report

if __name__ == "__main__":
    run_bronze_report()
