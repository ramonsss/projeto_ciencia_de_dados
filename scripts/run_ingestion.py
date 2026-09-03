import sys
from pathlib import Path

# Adiciona o diretório raiz ao PYTHONPATH para permitir imports do src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingestion.api_ingestor import BCB_API_Ingestor
from src.ingestion.file_ingestor import SIM_File_Ingestor
from src.pipeline.bronze import BronzeLayer

def executar():
    print("Iniciando Ingestão - BCB")
    bcb_ingestor = BCB_API_Ingestor()
    df_bcb = bcb_ingestor.ingest_data()
    BronzeLayer.save_to_bronze(df_bcb, "bcb", "series_economicas")
    
    print("\nIniciando Ingestão - OpenDataSUS (SIM)")
    sim_ingestor = SIM_File_Ingestor()
    df_sim = sim_ingestor.ingest_data()
    BronzeLayer.save_to_bronze(df_sim, "sim", "mortalidade")

if __name__ == "__main__":
    executar()
