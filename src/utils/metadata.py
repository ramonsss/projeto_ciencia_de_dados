import hashlib
import json
import uuid
from datetime import datetime

def gerar_hash_registro(registro: dict) -> str:
    conteudo = json.dumps(registro, sort_keys=True, default=str)
    return hashlib.sha256(conteudo.encode("utf-8")).hexdigest()

def adicionar_metadados_tecnicos(df, sistema_origem, objeto_origem):
    """Adiciona as colunas obrigatorias para a camada Bronze."""
    load_id = str(uuid.uuid4())
    df = df.copy()
    df["_ingestion_ts"] = datetime.now().isoformat()
    df["_source_system"] = sistema_origem
    df["_source_object"] = objeto_origem
    df["_load_id"] = load_id
    
    # Gera o hash do registro para controle de idempotencia
    df["_record_hash"] = df.apply(
        lambda row: gerar_hash_registro({k: v for k, v in row.items() if not str(k).startswith("_")}),
        axis=1
    )
    return df
