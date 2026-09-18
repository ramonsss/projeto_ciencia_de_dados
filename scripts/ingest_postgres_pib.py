import pandas as pd
from sqlalchemy import create_engine
import os
import json
import hashlib
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "projeto_cd")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "cesupa")
DB_PORT = os.getenv("DB_PORT", "5432")

WATERMARK_FILE = "data/bronze/pib_municipal/.watermark"

import urllib.parse

def get_db_engine():
    encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
    connection_string = f"postgresql+psycopg2://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def get_last_watermark():
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE, "r") as f:
            return f.read().strip()
    return "1900-01-01 00:00:00"

def update_watermark(new_watermark):
    os.makedirs(os.path.dirname(WATERMARK_FILE), exist_ok=True)
    with open(WATERMARK_FILE, "w") as f:
        f.write(str(new_watermark))

def add_bronze_metadata(df):
    """Adiciona metadados técnicos obrigatórios (Camada Bronze)."""
    ingestion_time = datetime.now().isoformat()
    
    df['_metadata_ingestion_time'] = ingestion_time
    df['_metadata_source_system'] = "POSTGRES_PROJETO_CD"
    
    def generate_hash(row):
        row_dict = row.to_dict()
        row_str = json.dumps(row_dict, sort_keys=True, default=str)
        return hashlib.sha256(row_str.encode('utf-8')).hexdigest()
        
    df['_metadata_record_hash'] = df.apply(generate_hash, axis=1)
    return df

def ingest_incremental_pib():
    print("Iniciando ingestão incremental do PostgreSQL...")
    engine = get_db_engine()
    
    last_watermark = get_last_watermark()
    print(f"Último watermark (checkpoint): {last_watermark}")
    
    query = f"""
        SELECT * FROM pib_municipal
        WHERE updated_at > '{last_watermark}'
        ORDER BY updated_at ASC
    """
    
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("Nenhum dado novo encontrado desde o último checkpoint. Idempotência garantida!")
        return
        
    print(f"Extraídos {len(df)} registros novos/atualizados.")
    
    new_watermark = df['updated_at'].max()
    
    df = add_bronze_metadata(df)
    
    output_dir = "data/bronze/pib_municipal"
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"pib_incremental_{timestamp}.parquet")
    
    df.to_parquet(output_path, engine='fastparquet', index=False)
    
    update_watermark(new_watermark)
    
    print(f"Ingestão concluída. Dados salvos em {output_path}")
    print(f"Novo watermark atualizado para: {new_watermark}")

if __name__ == "__main__":
    ingest_incremental_pib()
