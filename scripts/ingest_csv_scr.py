import polars as pl
import os
from datetime import datetime

def ingest_csv_scr():
    print("Iniciando ingestão do CSV gigante (SCR Banco Central)...")
    
    csv_path = "csv/scrdata_completo.csv"
    output_dir = "data/bronze/scr"
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"scr_completo_{timestamp}.parquet")
    
    print("Mapeando o arquivo (Lazy Evaluation) e adicionando metadados técnicos...")
    
    q = pl.scan_csv(csv_path, separator=";")
    
    q = q.filter(pl.col("uf") == "PA")
    
    ingestion_time = datetime.now().isoformat()
    q = q.with_columns([
        pl.lit(ingestion_time).alias("_metadata_ingestion_time"),
        pl.lit("CSV_SCR_BACEN").alias("_metadata_source_system"),
        pl.concat_str(pl.all(), separator="|").hash().alias("_metadata_record_hash")
    ])
    
    print(f"Iniciando gravação particionada no formato Parquet em: {output_path}")
    print("Isso pode levar vários minutos devido ao tamanho do arquivo (13 GB)...")
    
    q.sink_parquet(output_path)
    
    print("Ingestão do CSV concluída com sucesso! Os dados estão prontos na camada Bronze.")

if __name__ == "__main__":
    ingest_csv_scr()
