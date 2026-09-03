import requests
import pandas as pd

class BaseIngestor:
    def __init__(self, sistema_origem):
        self.sistema_origem = sistema_origem

class BCB_API_Ingestor(BaseIngestor):
    def __init__(self):
        super().__init__("BCB_SGS")
        self.series = {
            21082: "inadimplencia_credito"
        }

    def _fetch_serie(self, codigo_serie):
        url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados'
        params = {'formato': 'json', 'dataInicial': '01/01/2019', 'dataFinal': '31/12/2023'}
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def ingest_data(self):
        df_list = []
        for codigo, nome in self.series.items():
            print(f"Buscando serie BCB {codigo}...")
            dados = self._fetch_serie(codigo)
            df = pd.DataFrame(dados)
            df['codigo_serie'] = codigo
            df_list.append(df)
        
        return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()
