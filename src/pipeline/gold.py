from pathlib import Path

import pandas as pd

from config.settings import BRONZE_DIR, GOLD_DIR
from src.pipeline.silver import build_mortalidade_por_estado


def _read_bronze(source_system: str, source_object: str) -> pd.DataFrame:
    path = BRONZE_DIR / source_system / f"{source_object}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Arquivo bronze não encontrado: {path}")
    return pd.read_parquet(path)


def _gerar_resumo(top_estados: pd.DataFrame, inadimplencia_2022: float, inadimplencia_2023: float) -> str:
    estados = ", ".join(top_estados["uf"].tolist())
    tendencia = "aumento" if inadimplencia_2023 > inadimplencia_2022 else "estabilidade/queda"
    return (
        f"Cruzando as bases do SIM/OpenDataSUS, IBGE e Banco Central, identificamos que os estados {estados} "
        f"apresentam as maiores taxas de mortalidade proporcional à população. Nesse mesmo período, a inadimplência "
        f"nacional exibiu {tendencia} de {inadimplencia_2022:.2f}% para {inadimplencia_2023:.2f}%, o que reforça a "
        f"relação entre piora de contexto socioeconômico e maior necessidade de atenção em saúde. "
        f"Recomendamos que as Secretarias Estaduais de Saúde priorizem ações preventivas nos estados com maior taxa "
        f"de mortalidade nos próximos 12 meses. O ganho esperado é direcionar recursos para os estados com maior "
        f"necessidade identificada; se errarmos, o custo será alocar recursos em estados de menor prioridade e deixar "
        f"regiões críticas sem atendimento adequado. O ganho financeiro ou número de óbitos evitados ainda precisa "
        f"ser estimado pelo pipeline, pois não há dados de custo ou impacto de intervenção implementados."
    )


def build_gold_summary() -> dict:
    """Cria a camada Gold com priorização e resumo executivo."""
    mortalidade = build_mortalidade_por_estado()
    if mortalidade.empty:
        return {"top_estados": pd.DataFrame(), "resumo": "Sem dados disponíveis para cruzamento."}

    bcb_df = _read_bronze("bcb", "series_economicas")
    bcb_df = bcb_df.copy()
    bcb_df["data"] = pd.to_datetime(bcb_df["data"], format="%d/%m/%Y", errors="coerce")
    bcb_df = bcb_df.dropna(subset=["data", "valor"]).copy()
    bcb_df["ano"] = bcb_df["data"].dt.year
    inadimplencia_anual = (
        bcb_df.groupby("ano", as_index=False)["valor"].mean().rename(columns={"valor": "inadimplencia_media_anual"})
    )

    if 2022 not in inadimplencia_anual["ano"].tolist() or 2023 not in inadimplencia_anual["ano"].tolist():
        inadimplencia_2022 = 0.0
        inadimplencia_2023 = 0.0
    else:
        inadimplencia_2022 = float(inadimplencia_anual.loc[inadimplencia_anual["ano"] == 2022, "inadimplencia_media_anual"].iloc[0])
        inadimplencia_2023 = float(inadimplencia_anual.loc[inadimplencia_anual["ano"] == 2023, "inadimplencia_media_anual"].iloc[0])

    estado_ranking = (
        mortalidade.groupby(["codigo_uf", "uf"], as_index=False)["taxa_mortalidade_por_100k"]
        .mean()
        .rename(columns={"taxa_mortalidade_por_100k": "taxa_mortalidade_media_2022_2023"})
    )
    estado_ranking = estado_ranking.sort_values("taxa_mortalidade_media_2022_2023", ascending=False).reset_index(drop=True)

    top_estados = estado_ranking.head(5).copy()
    top_estados["inadimplencia_2022"] = inadimplencia_2022
    top_estados["inadimplencia_2023"] = inadimplencia_2023
    top_estados["coincide_com_aumento_inadimplencia"] = inadimplencia_2023 > inadimplencia_2022

    output_path = GOLD_DIR / "priorizacao_mortalidade_inadimplencia.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    top_estados.to_parquet(output_path, index=False)

    resumo = _gerar_resumo(top_estados, inadimplencia_2022, inadimplencia_2023)

    return {"top_estados": top_estados, "resumo": resumo}
