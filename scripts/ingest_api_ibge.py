import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import os
import hashlib
from datetime import datetime

# API IBGE servicodados v3 — população residente estimada por município (tabela 4709, variável 93)
BASE_URL = (
    "https://servicodados.ibge.gov.br/api/v3/agregados/4709/periodos/2022"
    "/variaveis/93?localidades=N6[all]"
)

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
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
    })
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

        # A API v3 retorna: [{ id, variavel, resultados: [{ series: [{ localidade, serie }] }] }]
        all_data = []
        for variavel in data:
            for resultado in variavel.get("resultados", []):
                for serie in resultado.get("series", []):
                    localidade = serie.get("localidade", {})
                    valores = serie.get("serie", {})
                    for periodo, valor in valores.items():
                        row = {
                            "variavel_id": variavel["id"],
                            "variavel_nome": variavel["variavel"],
                            "unidade": variavel.get("unidade", ""),
                            "municipio_id": localidade.get("id", ""),
                            "municipio_nome": localidade.get("nome", ""),
                            "periodo": periodo,
                            "valor": valor,
                        }
                        all_data.append(process_record(row))

        print(f"Resposta recebida com sucesso! {len(all_data)} registros de municípios.")

        output_dir = "data/bronze/ibge_populacao"
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(output_dir, f"ibge_4709_{timestamp}.json")

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)

        print(f"Extração concluída com sucesso! {len(all_data)} registros salvos em {output_path}")

    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar dados da API do IBGE: {e}")
        raise

if __name__ == "__main__":
    ingest_ibge_data()
