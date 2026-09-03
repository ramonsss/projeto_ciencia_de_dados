import requests
import zipfile
import io
import pandas as pd
from src.ingestion.api_ingestor import BaseIngestor

class SIM_File_Ingestor(BaseIngestor):
    def __init__(self):
        super().__init__("OpenDataSUS_SIM")
        self.base_url = 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIM/csv/DO{ano}OPEN_csv.zip'
        self.anos = ["22", "23"] # 2022 e 2023 para teste inicial

    def _download_and_extract(self, ano):
        url = self.base_url.format(ano=ano)
        print(f"Baixando arquivo do SIM ({url})...")
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            csv_filename = [f for f in z.namelist() if f.endswith('.csv')][0]
            with z.open(csv_filename) as f:
                # Carrega colunas chaves, tudo como string para evitar conversões indesejadas na Bronze
                colunas = ['DTOBITO', 'CODMUNRES', 'CAUSABAS']
                df = pd.read_csv(f, sep=';', encoding='latin-1', usecols=colunas, dtype=str)
                df['ano_referencia'] = f"20{ano}"
                return df

    def ingest_data(self):
        df_list = []
        for ano in self.anos:
            df_list.append(self._download_and_extract(ano))
        
        return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()
