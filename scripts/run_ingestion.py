import sys
from pathlib import Path

# Adiciona o diretório raiz ao PYTHONPATH para permitir imports do src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingestion.api_ingestor import BCB_API_Ingestor, IBGE_API_Ingestor
from src.ingestion.file_ingestor import SIM_File_Ingestor
from src.pipeline.bronze import BronzeLayer
from src.pipeline.silver import build_mortalidade_por_estado
from src.pipeline.gold import build_gold_summary

def executar():
    print("Iniciando Ingestão - BCB")
    bcb_ingestor = BCB_API_Ingestor()
    df_bcb = bcb_ingestor.ingest_data()
    BronzeLayer.save_to_bronze(df_bcb, "bcb", "series_economicas")
    
    print("\nIniciando Ingestão - IBGE")
    ibge_ingestor = IBGE_API_Ingestor()
    df_ibge = ibge_ingestor.ingest_data()
    BronzeLayer.save_to_bronze(df_ibge, "ibge", "populacao_estimada")

    print("\nIniciando Ingestão - OpenDataSUS (SIM)")
    sim_ingestor = SIM_File_Ingestor()
    df_sim = sim_ingestor.ingest_data()
    BronzeLayer.save_to_bronze(df_sim, "sim", "mortalidade")

    print("\nGerando camada Silver...")
    build_mortalidade_por_estado()

    print("\nGerando camada Gold...")
    resultado = build_gold_summary()
    print(resultado["resumo"])

if __name__ == "__main__":
    executar()
