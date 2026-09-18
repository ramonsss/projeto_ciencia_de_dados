import requests
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Carrega as variáveis de ambiente
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "projeto_cd")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "tuavodecueca123@")
DB_PORT = os.getenv("DB_PORT", "5432")

import urllib.parse

def get_db_engine():
    encoded_password = urllib.parse.quote_plus(DB_PASSWORD)
    connection_string = f"postgresql+psycopg2://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def populate_pib():
    print("Baixando dados do PIB Municipal (Tabela 5938) do IBGE servicodados v3...")

    # Usa API v3 do IBGE (servicodados) — tabela 5938, variável 37 (PIB a preços correntes), período 2021
    url = (
        "https://servicodados.ibge.gov.br/api/v3/agregados/5938"
        "/periodos/2021/variaveis/37?localidades=N6[all]"
    )

    response = requests.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    data = response.json()

    # Parse do formato v3: [{ id, variavel, resultados: [{ series: [{ localidade, serie }] }] }]
    rows = []
    for variavel in data:
        for resultado in variavel.get("resultados", []):
            for serie in resultado.get("series", []):
                localidade = serie.get("localidade", {})
                for periodo, valor in serie.get("serie", {}).items():
                    rows.append({
                        "codigo_municipio": localidade.get("id", ""),
                        "nome_municipio": localidade.get("nome", ""),
                        "ano": periodo,
                        "pib_corrente_mil_reais": valor,
                    })

    df = pd.DataFrame(rows)
    df['pib_corrente_mil_reais'] = pd.to_numeric(df['pib_corrente_mil_reais'], errors='coerce')
    df['updated_at'] = pd.Timestamp.now()
    
    print("Dados baixados com sucesso. Conectando ao PostgreSQL...")
    engine = get_db_engine()
    
    print("Criando tabela 'pib_municipal' com Chave Primária (Primary Key)...")
    
    create_table_sql = """
    DROP TABLE IF EXISTS pib_municipal;
    CREATE TABLE pib_municipal (
        codigo_municipio VARCHAR(10) PRIMARY KEY,
        nome_municipio VARCHAR(255),
        ano VARCHAR(4),
        pib_corrente_mil_reais FLOAT,
        updated_at TIMESTAMP
    );
    """
    
    from sqlalchemy import text
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
        
    print("Inserindo os dados na tabela...")
    df.to_sql('pib_municipal', engine, if_exists='append', index=False)
    
    print("Banco de dados populado com sucesso e estruturado corretamente!")

if __name__ == "__main__":
    populate_pib()
