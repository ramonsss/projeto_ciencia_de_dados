"""
Camada Silver — Tratamento, Separação de Colunas e Modelagem do PIB Municipal.
Aplica regras estritas de negócio:
  - Triagem de Escopo: Apenas os municípios do Estado do Pará (PA) avançam na Silver.
  - Quarentena: Municípios fora do Pará (outros 26 estados) são direcionados para a Quarentena.
  - Decomposição: 'nome_municipio' em nome limpo, UF, Estado e Região.
  - Decomposição: 'codigo_municipio' em código UF, código de 6 dígitos e dígito verificador.
  - Conversão do valor financeiro para Reais (R$).
  - Modelagem: dim_municipio (dimensão geral), fato_pib_municipal e visão consolidada Silver.
"""
import os
from datetime import datetime
import polars as pl

from base import print_banner, print_funnel_report, get_latest_file, MAPA_UF_BRASIL


BRONZE_DIR = "data/bronze/pib_municipal"
SILVER_DIR = "data/silver/pib_municipal"
DIMENSOES_DIR = "data/silver/dimensoes"
QUARENTENA_DIR = "data/silver/quarentena"


def process_silver_pib():
    print_banner("Tratamento & Modelagem Dimensional: PIB Municipal (PostgreSQL)")

    # 1. Leitura do arquivo Bronze mais recente
    bronze_path = get_latest_file(BRONZE_DIR, "parquet")
    print(f"[+] Lendo arquivo Bronze: {bronze_path}")

    df_raw = pl.read_parquet(bronze_path)
    total_entered = len(df_raw)
    print(f"[+] Registros de entrada lidos da Bronze: {total_entered:,}")

    # 2. Decomposição e enriquecimento de colunas
    df = (
        df_raw
        .with_columns([
            pl.col("codigo_municipio").str.strip_chars().alias("codigo_municipio"),
            pl.col("codigo_municipio").str.slice(0, 2).alias("codigo_uf"),
            pl.col("codigo_municipio").str.slice(0, 6).alias("codigo_municipio_6"),
            pl.col("codigo_municipio").str.slice(-1, 1).alias("digito_verificador"),
            pl.col("nome_municipio")
              .str.split(" - ")
              .list.slice(0, 1)
              .list.get(0)
              .str.strip_chars()
              .alias("nome_municipio_limpo"),
            pl.col("nome_municipio")
              .str.split(" - ")
              .list.slice(-1)
              .list.get(0)
              .str.strip_chars()
              .str.to_uppercase()
              .alias("sigla_uf"),
            pl.col("ano").cast(pl.Int32, strict=False).alias("ano"),
            pl.col("pib_corrente_mil_reais").cast(pl.Float64, strict=False).alias("pib_corrente_mil_reais"),
        ])
        .with_columns([
            (pl.col("pib_corrente_mil_reais") * 1000.0).alias("pib_total_reais"),
            (pl.col("sigla_uf") == "PA").alias("flag_estado_para"),
        ])
    )

    # 3. Enriquecimento com Nome do Estado e Região
    df_uf_map = pl.DataFrame([
        {
            "codigo_uf": cod,
            "sigla_uf_oficial": info[0],
            "nome_uf": info[1],
            "regiao": info[2],
        }
        for cod, info in MAPA_UF_BRASIL.items()
    ])
    df = df.join(df_uf_map, on="codigo_uf", how="left")

    # 4. Regras Estritas de Quarentena e Filtro de Escopo do Projeto:
    # O projeto é focado no Pará (PA). Outros municípios são retidos na quarentena como FORA_DO_ESCOPO_REGIONAL_PA!
    df = df.with_columns(
        pl.when(pl.col("sigla_uf") != "PA")
          .then(pl.lit("FORA_DO_ESCOPO_REGIONAL_PA"))
          .when(pl.col("pib_corrente_mil_reais").is_null() | (pl.col("pib_corrente_mil_reais") <= 0))
          .then(pl.lit("PIB_NULO_OU_NEGATIVO"))
          .when(pl.col("codigo_municipio").is_null() | (pl.col("codigo_municipio").str.len_chars() != 7))
          .then(pl.lit("CODIGO_IBGE_INVALIDO"))
          .when(pl.col("ano").is_null() | (pl.col("ano") < 1900))
          .then(pl.lit("ANO_INVALIDO"))
          .otherwise(pl.lit(None))
          .alias("motivo_quarentena")
    )

    # 5. Separação: Quem Passou (Pará Tratado) vs Quem NÃO Passou (Quarentena / Outros Estados)
    quarentena_df = df.filter(pl.col("motivo_quarentena").is_not_null())
    limpos_df = df.filter(pl.col("motivo_quarentena").is_null())

    total_quarantine = len(quarentena_df)

    rejection_reasons = {}
    if total_quarantine > 0:
        counts = quarentena_df["motivo_quarentena"].value_counts()
        for row in counts.to_dicts():
            rejection_reasons[row["motivo_quarentena"]] = row["count"]

    # 6. Deduplicação nos registros limpos
    total_antes_dedup = len(limpos_df)
    limpos_df = limpos_df.unique(subset=["codigo_municipio", "ano"], keep="first")
    total_duplicates = total_antes_dedup - len(limpos_df)
    total_passed = len(limpos_df)

    # 7. Modelagem em Múltiplas Tabelas:
    # A) Dimensão Município Geral (dim_municipio) - lookup de apoio
    dim_municipio = (
        df
        .select([
            "codigo_municipio",
            "codigo_municipio_6",
            "digito_verificador",
            "codigo_uf",
            "sigla_uf",
            "nome_uf",
            "regiao",
            "nome_municipio_limpo",
            "flag_estado_para",
        ])
        .rename({"nome_municipio_limpo": "nome_municipio"})
        .unique(subset=["codigo_municipio"])
        .sort(["sigla_uf", "nome_municipio"])
    )

    # B) Tabela Fato PIB Municipal do Pará (fato_pib_municipal)
    fato_pib = (
        limpos_df
        .select([
            "codigo_municipio",
            "ano",
            "pib_corrente_mil_reais",
            "pib_total_reais",
        ])
        .sort(["codigo_municipio", "ano"])
    )

    # C) Visão Consolidada Silver do Pará
    silver_consolidada = (
        limpos_df
        .select([
            "codigo_municipio",
            "codigo_municipio_6",
            "nome_municipio_limpo",
            "sigla_uf",
            "nome_uf",
            "regiao",
            "ano",
            "pib_corrente_mil_reais",
            "pib_total_reais",
        ])
        .rename({
            "nome_municipio_limpo": "nome_municipio",
            "sigla_uf": "uf",
        })
        .sort(["uf", "nome_municipio", "ano"])
    )

    # 8. Salvamento
    os.makedirs(SILVER_DIR, exist_ok=True)
    os.makedirs(DIMENSOES_DIR, exist_ok=True)
    os.makedirs(QUARENTENA_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path_dim = os.path.join(DIMENSOES_DIR, f"dim_municipio.parquet")
    path_fato = os.path.join(SILVER_DIR, f"fato_pib_municipal_{timestamp}.parquet")
    path_silver = os.path.join(SILVER_DIR, f"pib_municipal_silver_{timestamp}.parquet")
    path_quarantine = os.path.join(QUARENTENA_DIR, f"pib_quarentena_{timestamp}.parquet")

    dim_municipio.write_parquet(path_dim, compression="snappy")
    fato_pib.write_parquet(path_fato, compression="snappy")
    silver_consolidada.write_parquet(path_silver, compression="snappy")
    quarentena_df.write_parquet(path_quarantine, compression="snappy")

    # 9. Relatório do Funil
    print_funnel_report(
        name="PIB Municipal (PostgreSQL)",
        total_entered=total_entered,
        total_passed=total_passed,
        total_quarantine=total_quarantine,
        total_duplicates=total_duplicates,
        rejection_reasons=rejection_reasons,
        output_files=[
            f"Fato PIB (Para)    : {path_fato} ({len(fato_pib):,} municipios aprovados)",
            f"Silver Consolidada : {path_silver} ({len(silver_consolidada):,} municipios)",
            f"Quarentena         : {path_quarantine} ({len(quarentena_df):,} registros fora de escopo)",
        ]
    )

    return path_silver


if __name__ == "__main__":
    process_silver_pib()
