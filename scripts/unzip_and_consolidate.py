"""
Script para descompactar os arquivos zip do SCR em data/
e consolidar todos os CSVs em um único arquivo .parquet.
"""

import os
import glob
import zipfile
import time
from datetime import datetime
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

def unzip_all_data(data_dir="data"):
    zip_files = sorted(glob.glob(os.path.join(data_dir, "scrdata_*.zip")))
    if not zip_files:
        print(f"Nenhum arquivo zip encontrado em '{data_dir}'.")
        return []
    
    print(f"Encontrados {len(zip_files)} arquivos .zip em '{data_dir}'. Iniciando descompactação...")
    extracted_files = []
    
    for zip_path in zip_files:
        filename = os.path.basename(zip_path)
        print(f"  -> Extraindo {filename}...")
        t0 = time.time()
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(data_dir)
            extracted_files.extend(zf.namelist())
        dt = time.time() - t0
        print(f"     Concluído em {dt:.1f}s.")
        
    print(f"Descompactação finalizada! Total de {len(extracted_files)} arquivos extraídos.\n")
    return extracted_files

def consolidate_to_parquet(data_dir="data", output_parquet="data/scrdata.parquet"):
    csv_files = sorted(glob.glob(os.path.join(data_dir, "scrdata_*.csv")))
    if not csv_files:
        print(f"Nenhum arquivo CSV encontrado em '{data_dir}'.")
        return None

    print(f"Iniciando consolidação de {len(csv_files)} arquivos CSV em '{output_parquet}'...")
    
    # Amostra inicial para capturar os nomes das colunas
    first_df = pl.read_csv(csv_files[0], separator=";", n_rows=5, truncate_ragged_lines=True)
    columns = first_df.columns
    string_schema = {col: pl.String for col in columns}
    
    writer = None
    total_rows = 0
    start_time = time.time()
    
    for idx, csv_path in enumerate(csv_files, 1):
        file_name = os.path.basename(csv_path)
        t0 = time.time()
        
        # Lê o CSV forçando schema String para preservar integridade dos dados brutos e compatibilidade com silver_scr
        df = pl.read_csv(
            csv_path,
            separator=";",
            schema_overrides=string_schema,
            truncate_ragged_lines=True
        )
        
        n_rows = len(df)
        total_rows += n_rows
        
        # Converte para PyArrow Table
        arrow_table = df.to_arrow()
        
        if writer is None:
            # Inicializa o writer com o schema da primeira tabela
            writer = pq.ParquetWriter(output_parquet, arrow_table.schema, compression='snappy')
            
        writer.write_table(arrow_table)
        dt = time.time() - t0
        print(f"  [{idx:02d}/{len(csv_files):02d}] {file_name}: {n_rows:,} linhas em {dt:.2f}s (Total acumulado: {total_rows:,})")
        
    if writer is not None:
        writer.close()
        
    total_time = time.time() - start_time
    file_size_mb = os.path.getsize(output_parquet) / (1024 * 1024)
    print("\n" + "="*60)
    print("CONSOLIDAÇÃO CONCLUÍDA COM SUCESSO!")
    print(f"Arquivo gerado: {output_parquet}")
    print(f"Tamanho do arquivo: {file_size_mb:.2f} MB")
    print(f"Total de registros: {total_rows:,}")
    print(f"Tempo total de processamento: {total_time:.1f}s")
    print("="*60)
    
    return output_parquet

if __name__ == "__main__":
    data_directory = "data"
    unzip_all_data(data_directory)
    output_path = os.path.join(data_directory, "scrdata.parquet")
    consolidate_to_parquet(data_directory, output_path)
