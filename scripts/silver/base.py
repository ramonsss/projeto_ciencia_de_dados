"""
Módulo Base para a Camada Silver.
Fornece funções utilitárias compartilhadas, mapeamento geográfico do Brasil,
funil de qualidade de dados (Data Quality Funnel) e auditoria de quarentena.
"""
import os
import sys
import glob
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict

# Garante compatibilidade total de codificação no Windows console
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

# Mapeamento oficial das 27 Unidades Federativas do Brasil
MAPA_UF_BRASIL = {
    '11': ('RO', 'Rondônia', 'Norte'),
    '12': ('AC', 'Acre', 'Norte'),
    '13': ('AM', 'Amazonas', 'Norte'),
    '14': ('RR', 'Roraima', 'Norte'),
    '15': ('PA', 'Pará', 'Norte'),
    '16': ('AP', 'Amapá', 'Norte'),
    '17': ('TO', 'Tocantins', 'Norte'),
    '21': ('MA', 'Maranhão', 'Nordeste'),
    '22': ('PI', 'Piauí', 'Nordeste'),
    '23': ('CE', 'Ceará', 'Nordeste'),
    '24': ('RN', 'Rio Grande do Norte', 'Nordeste'),
    '25': ('PB', 'Paraíba', 'Nordeste'),
    '26': ('PE', 'Pernambuco', 'Nordeste'),
    '27': ('AL', 'Alagoas', 'Nordeste'),
    '28': ('SE', 'Sergipe', 'Nordeste'),
    '29': ('BA', 'Bahia', 'Nordeste'),
    '31': ('MG', 'Minas Gerais', 'Sudeste'),
    '32': ('ES', 'Espírito Santo', 'Sudeste'),
    '33': ('RJ', 'Rio de Janeiro', 'Sudeste'),
    '35': ('SP', 'São Paulo', 'Sudeste'),
    '41': ('PR', 'Paraná', 'Sul'),
    '42': ('SC', 'Santa Catarina', 'Sul'),
    '43': ('RS', 'Rio Grande do Sul', 'Sul'),
    '50': ('MS', 'Mato Grosso do Sul', 'Centro-Oeste'),
    '51': ('MT', 'Mato Grosso', 'Centro-Oeste'),
    '52': ('GO', 'Goiás', 'Centro-Oeste'),
    '53': ('DF', 'Distrito Federal', 'Centro-Oeste'),
}


def print_banner(title: str):
    """Exibe um banner visual moderno no terminal."""
    border = "=" * 70
    print(f"\n{border}")
    print(f"  CAMADA SILVER (PRATA)  |  {title.upper()}")
    print(f"{border}\n")


def print_funnel_report(
    name: str,
    total_entered: int,
    total_passed: int,
    total_quarantine: int,
    total_duplicates: int,
    rejection_reasons: Optional[Dict[str, int]] = None,
    output_files: Optional[list] = None
):
    """
    Exibe o Funil de Qualidade de Dados detalhado:
    Quantos dados entraram, quantos NÃO passaram e por quais motivos, e quantos passaram.
    """
    total_failed = total_quarantine + total_duplicates
    pass_rate = (total_passed / total_entered * 100) if total_entered > 0 else 0.0
    fail_rate = (total_failed / total_entered * 100) if total_entered > 0 else 0.0

    print("\n" + "-" * 70)
    print(f"FUNIL DE QUALIDADE E AUDITORIA: {name}")
    print("-" * 70)
    print(f" [>] DADOS QUE ENTRARAM (Bronze)  : {total_entered:>10,}  (100.0%)")
    print(f" [X] DADOS QUE NAO PASSARAM       : {total_failed:>10,}  ({fail_rate:>5.2f}%)")
    print(f"     |-- Registros em Quarentena  : {total_quarantine:>10,}")
    print(f"     `-- Duplicatas Descartadas   : {total_duplicates:>10,}")

    if rejection_reasons and any(rejection_reasons.values()):
        print("     `-- Motivos de Reprovação (Quarentena):")
        for reason, count in rejection_reasons.items():
            if count > 0:
                print(f"         * {reason:<30}: {count:>8,}")

    print(f" [+] DADOS QUE PASSARAM (Silver)  : {total_passed:>10,}  ({pass_rate:>5.2f}%)")
    
    if output_files:
        print("\n [O] Tabelas e Arquivos Gerados:")
        for f in output_files:
            print(f"     -> {f}")
    print("-" * 70 + "\n")


def get_latest_file(directory: str, extension: str) -> str:
    """Retorna o caminho do arquivo mais recente em um diretório com a extensão informada."""
    if not os.path.exists(directory):
        raise FileNotFoundError(f"Diretório não encontrado: {directory}")

    pattern = os.path.join(directory, f"*.{extension.lstrip('.')}")
    files = sorted(glob.glob(pattern), key=os.path.getmtime)
    
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo *.{extension} encontrado em {directory}")
    
    return files[-1]
