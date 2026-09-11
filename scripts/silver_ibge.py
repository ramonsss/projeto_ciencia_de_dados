"""
Camada Silver — Limpeza dos dados da API do IBGE (População)
Lê o JSON bruto da Bronze, limpa e salva em data/silver/ibge_populacao/
"""
import json
import pandas as pd
import os
from datetime import datetime

def clean_ibge():
    print("=== SILVER: Limpeza dos dados IBGE (População) ===")
    
    # Encontra o arquivo mais recente na Bronze
    bronze_dir = "data/bronze/ibge_populacao"
    bronze_files = sorted([f for f in os.listdir(bronze_dir) if f.endswith(".json")])
    if not bronze_files:
        raise FileNotFoundError("Nenhum arquivo JSON encontrado na camada Bronze do IBGE!")
    
    bronze_path = os.path.join(bronze_dir, bronze_files[-1])
    print(f"Lendo arquivo Bronze: {bronze_path}")
    
    with open(bronze_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    df = pd.DataFrame(data[1:])
    print(f"Total de registros brutos: {len(df)}")
    
    df = df.rename(columns={
        "NN": "nivel_territorial",
        "MN": "unidade_medida",
        "V": "populacao",
        "D1N": "municipio_uf",
        "D2N": "variavel",
        "D3N": "ano",
    })
    
    # Separa pelo último " - " (pois nomes de cidade podem ter hífen)
    df["nome_municipio"] = df["municipio_uf"].str.rsplit(" - ", n=1).str[0]
    df["uf"] = df["municipio_uf"].str.rsplit(" - ", n=1).str[1]
    
    df["populacao"] = pd.to_numeric(df["populacao"], errors="coerce")
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    
    mask_quarentena = df["populacao"].isna() | df["uf"].isna()
    df_quarentena = df[mask_quarentena].copy()
    df_limpo = df[~mask_quarentena].copy()
    
    print(f"Registros limpos: {len(df_limpo)}")
    print(f"Registros em quarentena: {len(df_quarentena)}")
    
    antes_dedup = len(df_limpo)
    df_limpo = df_limpo.drop_duplicates(subset=["nome_municipio", "uf", "ano"], keep="first")
    print(f"Duplicatas removidas: {antes_dedup - len(df_limpo)}")
    
    df_limpo = df_limpo[["nome_municipio", "uf", "ano", "populacao"]]
    
    df_limpo["uf"] = df_limpo["uf"].str.strip().str.upper()
    
    output_dir = "data/silver/ibge_populacao"
    quarentena_dir = "data/silver/quarentena"
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(quarentena_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    output_path = os.path.join(output_dir, f"ibge_populacao_limpo_{timestamp}.parquet")
    df_limpo.to_parquet(output_path, index=False)
    print(f"Dados limpos salvos em: {output_path}")
    
    if len(df_quarentena) > 0:
        quarentena_path = os.path.join(quarentena_dir, f"ibge_quarentena_{timestamp}.parquet")
        df_quarentena.to_parquet(quarentena_path, index=False)
        print(f"Quarentena salva em: {quarentena_path}")
    
    print("=== Silver IBGE concluída! ===")

if __name__ == "__main__":
    clean_ibge()
