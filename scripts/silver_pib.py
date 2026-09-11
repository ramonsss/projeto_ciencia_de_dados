"""
Camada Silver — Limpeza dos dados do PIB Municipal (PostgreSQL)
Lê o parquet bruto da Bronze, limpa e salva em data/silver/pib/
"""
import pandas as pd
import os
from datetime import datetime

def clean_pib():
    print("=== SILVER: Limpeza dos dados PIB Municipal ===")
    
    bronze_dir = "data/bronze/pib_municipal"
    bronze_files = sorted([f for f in os.listdir(bronze_dir) if f.endswith(".parquet")])
    if not bronze_files:
        raise FileNotFoundError("Nenhum arquivo parquet encontrado na camada Bronze do PIB!")
    
    bronze_path = os.path.join(bronze_dir, bronze_files[-1])
    print(f"Lendo arquivo Bronze: {bronze_path}")
    
    df = pd.read_parquet(bronze_path)
    print(f"Total de registros brutos: {len(df)}")
    
    df["codigo_municipio"] = df["codigo_municipio"].astype(str).str.strip()
    df["nome_municipio"] = df["nome_municipio"].astype(str).str.strip()
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    df["pib_corrente_mil_reais"] = pd.to_numeric(df["pib_corrente_mil_reais"], errors="coerce")
    
    df["uf"] = df["nome_municipio"].str.rsplit(" - ", n=1).str[1]
    df["nome_municipio_limpo"] = df["nome_municipio"].str.rsplit(" - ", n=1).str[0]
    
    mask_quarentena = (
        df["pib_corrente_mil_reais"].isna() | 
        df["codigo_municipio"].isin(["nan", "", "None"])
    )
    df_quarentena = df[mask_quarentena].copy()
    df_limpo = df[~mask_quarentena].copy()
    
    print(f"Registros limpos: {len(df_limpo)}")
    print(f"Registros em quarentena: {len(df_quarentena)}")
    
    antes_dedup = len(df_limpo)
    df_limpo = df_limpo.drop_duplicates(subset=["codigo_municipio", "ano"], keep="first")
    print(f"Duplicatas removidas: {antes_dedup - len(df_limpo)}")
    
    df_limpo = df_limpo[["codigo_municipio", "nome_municipio_limpo", "uf", "ano", "pib_corrente_mil_reais"]]
    df_limpo = df_limpo.rename(columns={"nome_municipio_limpo": "nome_municipio"})
    
    df_limpo["uf"] = df_limpo["uf"].str.strip().str.upper()
    
    output_dir = "data/silver/pib"
    quarentena_dir = "data/silver/quarentena"
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(quarentena_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    output_path = os.path.join(output_dir, f"pib_limpo_{timestamp}.parquet")
    df_limpo.to_parquet(output_path, index=False)
    print(f"Dados limpos salvos em: {output_path}")
    
    if len(df_quarentena) > 0:
        quarentena_path = os.path.join(quarentena_dir, f"pib_quarentena_{timestamp}.parquet")
        df_quarentena.to_parquet(quarentena_path, index=False)
        print(f"Quarentena salva em: {quarentena_path}")
    
    print("=== Silver PIB concluída! ===")

if __name__ == "__main__":
    clean_pib()
