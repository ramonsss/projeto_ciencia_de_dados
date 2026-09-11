"""
Camada Silver — Limpeza dos dados do SCR (Banco Central)
Lê o parquet bruto da Bronze, limpa e salva em data/silver/scr/
"""
import polars as pl
import os
from datetime import datetime

def clean_scr():
    print("=== SILVER: Limpeza dos dados SCR (BACEN) ===")
    
    bronze_dir = "data/bronze/scr"
    bronze_files = sorted([f for f in os.listdir(bronze_dir) if f.endswith(".parquet")])
    if not bronze_files:
        raise FileNotFoundError("Nenhum arquivo parquet encontrado na camada Bronze do SCR!")
    
    bronze_path = os.path.join(bronze_dir, bronze_files[-1])
    print(f"Lendo arquivo Bronze: {bronze_path}")
    
    q = pl.scan_parquet(bronze_path)
    
    colunas_monetarias = [
        "a_vencer_ate_90_dias",
        "a_vencer_de_91_ate_360_dias",
        "a_vencer_de_361_ate_1080_dias",
        "a_vencer_de_1081_ate_1800_dias",
        "a_vencer_de_1801_ate_5400_dias",
        "a_vencer_acima_de_5400_dias",
        "carteira_a_vencer",
        "vencido_de_15_ate_90_dias",
        "vencido_acima_de_90_dias",
        "carteira_vencida",
        "carteira_ativa",
        "carteira_inadimplencia",
        "ativo_problematico",
    ]
    
    for col in colunas_monetarias:
        q = q.with_columns(
            pl.col(col)
            .str.replace_all(r"\.", "")
            .str.replace(",", ".")
            .cast(pl.Float64, strict=False)
        )
    
    q = q.with_columns(
        pl.col("data_base").str.to_date("%Y-%m-%d", strict=False).alias("data_base")
    )
    
    q = q.with_columns(
        pl.col("uf").str.to_uppercase().alias("uf")
    )
    
    colunas_negocio = [
        "data_base", "uf", "segmento", "cliente", "cnae_ocupacao", "porte",
        "modalidade", "submodalidade", "origem", "indexador", "numero_de_operacoes"
    ]
    q = q.unique(subset=colunas_negocio, keep="first")
    
    print("Processando limpeza (isso pode levar alguns minutos com 40M de linhas)...")
    df = q.collect(streaming=True)
    
    total_antes = len(df)
    print(f"Total de registros após dedup: {total_antes}")
    
    mask_quarentena = df["carteira_ativa"].is_null() | df["data_base"].is_null()
    df_quarentena = df.filter(mask_quarentena)
    df_limpo = df.filter(~mask_quarentena)
    
    print(f"Registros limpos: {len(df_limpo)}")
    print(f"Registros em quarentena: {len(df_quarentena)}")
    
    df_limpo = df_limpo.with_columns(
        pl.col("data_base").dt.strftime("%Y-%m").alias("ano_mes")
    )
    
    colunas_remover = ["_metadata_ingestion_time", "_metadata_source_system", "_metadata_record_hash"]
    df_limpo = df_limpo.drop(colunas_remover)
    if len(df_quarentena) > 0:
        df_quarentena = df_quarentena.drop(colunas_remover)
    
    output_dir = "data/silver/scr"
    quarentena_dir = "data/silver/quarentena"
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(quarentena_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    output_path = os.path.join(output_dir, f"scr_limpo_{timestamp}.parquet")
    df_limpo.write_parquet(output_path)
    print(f"Dados limpos salvos em: {output_path}")
    
    if len(df_quarentena) > 0:
        quarentena_path = os.path.join(quarentena_dir, f"scr_quarentena_{timestamp}.parquet")
        df_quarentena.write_parquet(quarentena_path)
        print(f"Quarentena salva em: {quarentena_path}")
    
    print("=== Silver SCR concluída! ===")

if __name__ == "__main__":
    clean_scr()
