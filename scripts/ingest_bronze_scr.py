"""
Camada Bronze — Ingestão do SCR (Banco Central)
Lê o scrdata.parquet já consolidado em data/, filtra para o Estado do Pará (PA)
conforme especificação do projeto, adiciona metadados técnicos da Camada Bronze
e salva em data/bronze/scr/.
"""
import polars as pl
import os
import hashlib
from datetime import datetime


FONTE_PARQUET = "data/scrdata.parquet"
OUTPUT_DIR = "data/bronze/scr"


def ingest_bronze_scr():
    print("=== BRONZE: Ingestão SCR (Banco Central - BACEN) ===")

    if not os.path.exists(FONTE_PARQUET):
        raise FileNotFoundError(
            f"Arquivo '{FONTE_PARQUET}' não encontrado. "
            "Execute unzip_and_consolidate.py primeiro."
        )

    file_size_mb = os.path.getsize(FONTE_PARQUET) / (1024 * 1024)
    print(f"Lendo {FONTE_PARQUET} ({file_size_mb:.0f} MB)...")

    ingestion_time = datetime.now().isoformat()
    source_system = "BACEN_SCR_CSV"

    # Filtragem lazy para o Pará (PA) e adição de metadados técnicos
    print("Filtrando registros do Pará (PA) via Polars Lazy...")
    
    # 1. Filtra registros do PA primeiro
    lf_pa = pl.scan_parquet(FONTE_PARQUET).filter(
        pl.col("uf").str.to_uppercase() == "PA"
    )

    # 2. Adiciona metadados
    df = (
        lf_pa
        .with_columns([
            pl.lit(ingestion_time).alias("_metadata_ingestion_time"),
            pl.lit(source_system).alias("_metadata_source_system"),
            pl.concat_str(pl.all(), separator="|")
              .map_elements(
                  lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest(),
                  return_dtype=pl.String
              )
              .alias("_metadata_record_hash"),
        ])
        .collect(streaming=True)
    )

    n_rows = len(df)
    print(f"Total de registros do Pará (PA) carregados: {n_rows:,}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(OUTPUT_DIR, f"scr_bronze_{timestamp}.parquet")

    print(f"Salvando em {output_path}...")
    df.write_parquet(output_path, compression="snappy")

    out_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Arquivo Bronze salvo com sucesso! Tamanho: {out_size_mb:.2f} MB")
    print(f"=== Bronze SCR concluída: {n_rows:,} registros em {output_path} ===")
    return output_path


if __name__ == "__main__":
    ingest_bronze_scr()
