import os
from pathlib import Path
from src.utils.metadata import adicionar_metadados_tecnicos
from config.settings import BRONZE_DIR

class BronzeLayer:
    @staticmethod
    def save_to_bronze(df, source_system, source_object):
        """Salva o DataFrame bruto na camada Bronze com metadados."""
        if df.empty:
            print(f"[{source_system}] Nenhum dado para salvar.")
            return

        print(f"[{source_system}] Processando {len(df)} registros para a Bronze...")
        
        # Adiciona _ingestion_ts, _source_system, _load_id, _record_hash
        df_bronze = adicionar_metadados_tecnicos(df, source_system, source_object)
        
        # Define caminho (em um projeto real, faríamos particionamento por data)
        target_path = BRONZE_DIR / source_system / f"{source_object}.parquet"
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Lógica simplificada de idempotência: se arquivo existe, anexa apenas hashes novos
        import pandas as pd
        if target_path.exists():
            df_existente = pd.read_parquet(target_path)
            hashes_antigos = set(df_existente['_record_hash'])
            df_novos = df_bronze[~df_bronze['_record_hash'].isin(hashes_antigos)]
            
            if df_novos.empty:
                print(f"[{source_system}] Dados já existentes. Nada novo para salvar.")
                return
                
            df_final = pd.concat([df_existente, df_novos], ignore_index=True)
            print(f"[{source_system}] Inserindo {len(df_novos)} registros novos na Bronze.")
        else:
            df_final = df_bronze
            print(f"[{source_system}] Criando novo arquivo Bronze com {len(df_final)} registros.")

        df_final.to_parquet(target_path, index=False)
