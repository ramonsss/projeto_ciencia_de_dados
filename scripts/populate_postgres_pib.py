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
    print("Baixando dados do PIB Municipal (Tabela 5938) do IBGE SIDRA...")
    
    url = "https://apisidra.ibge.gov.br/values/t/5938/n6/all/v/37/p/last%201"
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    columns = data[0]
    df = pd.DataFrame(data[1:])
    df.columns = columns.keys()
    
    df = df.rename(columns={
        "D1C": "codigo_municipio",
        "D1N": "nome_municipio",
        "D2C": "ano",
        "V": "pib_corrente_mil_reais"
    })
    
    df = df[["codigo_municipio", "nome_municipio", "ano", "pib_corrente_mil_reais"]]
    
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
