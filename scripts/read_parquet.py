import polars as pl
import argparse
import os

def read_parquet_sample(file_path, n_rows=50):
    if not os.path.exists(file_path):
        print(f"Erro: O arquivo '{file_path}' não foi encontrado.")
        return

    try:
        print(f"--- Lendo amostra ({n_rows} linhas) do arquivo: {file_path} ---")
        # Usamos scan_parquet e limit() para não ler o arquivo inteiro na memória se for gigante
        df = pl.scan_parquet(file_path).head(n_rows).collect()
        
        # Mostrando usando o display padrão do Polars (que já é bem formatado)
        # Ajustando configs de display para mostrar todas as colunas
        with pl.Config(tbl_cols=-1, tbl_rows=n_rows):
            print(df)
            
    except Exception as e:
        print(f"Erro ao ler o arquivo parquet: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lê uma amostra rápida de um arquivo .parquet usando Polars.")
    parser.add_argument("file_path", type=str, help="Caminho para o arquivo .parquet")
    parser.add_argument("--rows", "-n", type=int, default=20, help="Número de linhas para ler (padrão: 20)")
    
    args = parser.parse_args()
    read_parquet_sample(args.file_path, args.rows)
