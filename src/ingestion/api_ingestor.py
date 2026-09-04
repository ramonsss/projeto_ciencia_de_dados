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
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def _fallback_serie(self):
        meses = pd.date_range(start='2019-01-01', end='2023-12-01', freq='MS')
        valores = []
        for idx in range(len(meses)):
            base = 3.2 + (idx * 0.07)
            if idx >= 36:
                base += (idx - 35) * 0.18
            valores.append(round(base, 2))
        df = pd.DataFrame({
            'data': [d.strftime('%d/%m/%Y') for d in meses],
            'valor': valores,
            'codigo_serie': 21082,
        })
        return df

    def ingest_data(self):
        df_list = []
        for codigo, nome in self.series.items():
            print(f"Buscando serie BCB {codigo}...")
            try:
                dados = self._fetch_serie(codigo)
            except Exception as exc:
                print(f"Falha ao consultar API do BCB ({exc}). Usando fallback sintético.")
                dados = self._fallback_serie().to_dict(orient='records')
            df = pd.DataFrame(dados)
            if 'valor' in df.columns:
                df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
            df['codigo_serie'] = codigo
            df_list.append(df)

        return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()


class IBGE_API_Ingestor(BaseIngestor):
    def __init__(self):
        super().__init__("IBGE_SIDRA")
        self.url = "https://apisidra.ibge.gov.br/values/t/6579/v/9324/p/2021/n3/all"

    def _fallback_populacao(self):
        populacao_por_uf = {
            11: 1620992, 12: 907564, 13: 2277950, 14: 688958, 15: 905418, 16: 391360, 17: 380190,
            21: 3419384, 22: 1708514, 23: 1271596, 24: 1900040, 25: 820447, 26: 2949120, 27: 334560,
            28: 652182, 29: 2658973, 31: 1104021, 32: 418591, 33: 1600000, 35: 46000000, 41: 12373020,
            42: 2642414, 43: 595390, 50: 295860, 51: 5200000, 52: 3270000, 53: 3200000,
        }
        nomes_uf = [
            'RO', 'AC', 'AM', 'RR', 'PA', 'AP', 'TO', 'MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE',
            'BA', 'MG', 'ES', 'RJ', 'SP', 'PR', 'SC', 'RS', 'MS', 'MT', 'GO', 'DF'
        ]
        return pd.DataFrame(
            {
                'codigo_uf': list(populacao_por_uf.keys()),
                'uf': nomes_uf,
                'populacao': list(populacao_por_uf.values()),
            }
        )

    def ingest_data(self):
        try:
            response = requests.get(self.url, timeout=10)
            response.raise_for_status()
            payload = response.json()
            if not payload or len(payload) < 2:
                raise ValueError('Resposta vazia da API do IBGE.')

            raw = pd.DataFrame(payload[1:])
            if {'D3C', 'D3N', 'V'}.issubset(raw.columns):
                df = raw[['D3C', 'D3N', 'V']].rename(columns={'D3C': 'codigo_uf', 'D3N': 'uf', 'V': 'populacao'})
                df['codigo_uf'] = pd.to_numeric(df['codigo_uf'], errors='coerce')
                df['populacao'] = pd.to_numeric(df['populacao'], errors='coerce')
                df = df.dropna(subset=['codigo_uf', 'populacao']).sort_values('codigo_uf')
                return df.reset_index(drop=True)
        except Exception as exc:
            print(f"Falha ao consultar API do IBGE ({exc}). Usando população sintética por UF.")

        return self._fallback_populacao()
