"""
Orquestrador Principal da Camada Silver (Pipeline Unificado).
Executa o tratamento, padronização, quarentena e deduplicação das três fontes:
  1. IBGE População Residente (API)
  2. IBGE PIB Municipal (PostgreSQL)
  3. BACEN SCR Risco de Crédito (CSV / Parquet)
"""
import time
import sys
import os
from pathlib import Path

# Adiciona o próprio diretório ao sys.path para imports limpos
current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

from base import print_banner
from clean_ibge_populacao import process_silver_ibge
from clean_pib_municipal import process_silver_pib
from clean_scr_credito import process_silver_scr


def run_all_silver():
    start_total = time.time()

    print("\n" + "#" * 65)
    print("      INICIANDO PIPELINE COMPLETO DA CAMADA SILVER (PRATA)     ")
    print("#" * 65)

    # 1. IBGE População
    t0 = time.time()
    out_ibge = process_silver_ibge()
    t_ibge = time.time() - t0

    # 2. PIB Municipal
    t0 = time.time()
    out_pib = process_silver_pib()
    t_pib = time.time() - t0

    # 3. SCR BACEN
    t0 = time.time()
    out_scr = process_silver_scr()
    t_scr = time.time() - t0

    elapsed_total = time.time() - start_total

    print("\n" + "#" * 65)
    print("      PIPELINE DA CAMADA SILVER FINALIZADO COM SUCESSO!        ")
    print("#" * 65)
    print(f" • Tempo IBGE População : {t_ibge:.2f}s")
    print(f" • Tempo PIB Municipal  : {t_pib:.2f}s")
    print(f" • Tempo SCR BACEN      : {t_scr:.2f}s")
    print(f" • Tempo Total          : {elapsed_total:.2f}s")
    print("-" * 65)
    print("ARQUIVOS GERADOS NA SILVER:")
    print(f" [1] IBGE População -> {out_ibge}")
    print(f" [2] PIB Municipal  -> {out_pib}")
    print(f" [3] SCR Crédito    -> {out_scr}")
    print("#" * 65 + "\n")


if __name__ == "__main__":
    run_all_silver()
