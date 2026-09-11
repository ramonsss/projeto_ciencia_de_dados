import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import os
import hashlib
from datetime import datetime

BASE_URL = "https://apisidra.ibge.gov.br/values/t/4709/n6/all/v/93/p/last%201/f/n"

def get_session():
    """Configura uma sessão HTTP com Retry e Backoff Exponencial."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def process_record(row):
    """Adiciona metadados técnicos obrigatórios no registro (Camada Bronze)."""
    row_str = json.dumps(row, sort_keys=True)
    row_hash = hashlib.sha256(row_str.encode('utf-8')).hexdigest()
    
    row['_metadata_ingestion_time'] = datetime.now().isoformat()
    row['_metadata_source_system'] = "API_IBGE_SIDRA"
    row['_metadata_record_hash'] = row_hash
    return row

def ingest_ibge_data():
    session = get_session()
    
    print("Iniciando extração da API do IBGE (Tabela 4709 - População Residente Estimada)")
    print(f"URL: {BASE_URL}")
    
    try:
        response = session.get(BASE_URL, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        print(f"Resposta recebida com sucesso! {len(data)} registros (incluindo cabeçalho).")
        
        all_data = []
        for row in data:
            processed_row = process_record(row)
            all_data.append(processed_row)
        
        output_dir = "data/bronze/ibge_populacao"
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(output_dir, f"ibge_4709_{timestamp}.json")
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
            
        print(f"Extração concluída com sucesso! {len(all_data)-1} registros de municípios salvos em {output_path}")
        
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar dados da API do IBGE: {e}")
        raise

if __name__ == "__main__":
    ingest_ibge_data()
