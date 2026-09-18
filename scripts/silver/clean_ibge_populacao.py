"""
Camada Silver — Tratamento, Separação de Colunas e Modelagem da População (IBGE).
Aplica regras estritas de negócio:
  - Triagem de Escopo: Apenas os municípios do Estado do Pará (PA) avançam na Silver.
  - Quarentena: Municípios fora do Pará (outros 26 estados) e dados inconsistentes vão para Quarentena.
  - Decomposição: 'municipio_nome' em nome limpo, UF, Estado e Região.
  - Decomposição: 'municipio_id' em código UF, código de 6 dígitos e dígito verificador.
  - Conversão e sanitização do número de habitantes para Int64.
  - Criação da Fato População Municipal (fato_populacao_municipal) e Consolidada Silver.
"""
import json
import os
from datetime import datetime
import polars as pl

from base import print_banner, print_funnel_report, get_latest_file, MAPA_UF_BRASIL


BRONZE_DIR = "data/bronze/ibge_populacao"
SILVER_DIR = "data/silver/ibge_populacao"
QUARENTENA_DIR = "data/silver/quarentena"


def process_silver_ibge():
    print_banner("Tratamento & Modelagem Dimensional: IBGE População Residente (API)")

    # 1. Leitura do arquivo Bronze mais recente
    bronze_path = get_latest_file(BRONZE_DIR, "json")
    print(f"[+] Lendo arquivo Bronze: {bronze_path}")

    with open(bronze_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    total_entered = len(raw_data)
    print(f"[+] Registros de entrada lidos da Bronze: {total_entered:,}")

    df_raw = pl.DataFrame(raw_data)

    # 2. Decomposição de colunas
    df = (
        df_raw
        .with_columns([
            pl.col("municipio_id").str.strip_chars().alias("codigo_municipio"),
            pl.col("municipio_id").str.slice(0, 2).alias("codigo_uf"),
            pl.col("municipio_id").str.slice(0, 6).alias("codigo_municipio_6"),
            pl.col("municipio_id").str.slice(-1, 1).alias("digito_verificador"),
            pl.col("municipio_nome")
              .str.split(" - ")
              .list.slice(0, 1)
              .list.get(0)
              .str.strip_chars()
              .alias("nome_municipio_limpo"),
            pl.col("municipio_nome")
              .str.split(" - ")
              .list.slice(-1)
              .list.get(0)
              .str.strip_chars()
              .str.to_uppercase()
              .alias("sigla_uf"),
            pl.col("periodo").cast(pl.Int32, strict=False).alias("ano"),
            pl.col("valor")
              .str.replace_all(r"\.", "")
              .str.replace(",", ".")
              .cast(pl.Int64, strict=False)
              .alias("populacao"),
        ])
        .with_columns([
            (pl.col("sigla_uf") == "PA").alias("flag_estado_para"),
        ])
    )

    # 3. Enriquecimento territorial
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

    # 4. Regras Estritas de Quarentena e Filtro de Escopo:
    df = df.with_columns(
        pl.when(pl.col("sigla_uf") != "PA")
          .then(pl.lit("FORA_DO_ESCOPO_REGIONAL_PA"))
          .when(pl.col("populacao").is_null() | (pl.col("populacao") <= 0))
          .then(pl.lit("POPULACAO_NULA_OU_NEGATIVA"))
          .when(pl.col("codigo_municipio").is_null() | (pl.col("codigo_municipio").str.len_chars() != 7))
          .then(pl.lit("CODIGO_IBGE_INVALIDO"))
          .when(pl.col("ano").is_null() | (pl.col("ano") < 1900))
          .then(pl.lit("ANO_INVALIDO"))
          .otherwise(pl.lit(None))
          .alias("motivo_quarentena")
    )

    # 5. Separação: Quem Passou (Pará Tratado) vs Quem NÃO Passou (Quarentena)
    quarentena_df = df.filter(pl.col("motivo_quarentena").is_not_null())
    limpos_df = df.filter(pl.col("motivo_quarentena").is_null())

    total_quarantine = len(quarentena_df)

    rejection_reasons = {}
    if total_quarantine > 0:
        counts = quarentena_df["motivo_quarentena"].value_counts()
        for row in counts.to_dicts():
            rejection_reasons[row["motivo_quarentena"]] = row["count"]

    # 6. Deduplicação
    total_antes_dedup = len(limpos_df)
    limpos_df = limpos_df.unique(subset=["codigo_municipio", "ano"], keep="first")
    total_duplicates = total_antes_dedup - len(limpos_df)
    total_passed = len(limpos_df)

    # 7. Modelagem em Múltiplas Tabelas:
    # A) Tabela Fato População Municipal (fato_populacao_municipal)
    fato_populacao = (
        limpos_df
        .select([
            "codigo_municipio",
            "ano",
            "populacao",
        ])
        .sort(["codigo_municipio", "ano"])
    )

    # B) Visão Consolidada Silver do Pará
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
            "populacao",
        ])
        .rename({
            "nome_municipio_limpo": "nome_municipio",
            "sigla_uf": "uf",
        })
        .sort(["uf", "nome_municipio", "ano"])
    )

    # 8. Salvamento
    os.makedirs(SILVER_DIR, exist_ok=True)
    os.makedirs(QUARENTENA_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path_fato = os.path.join(SILVER_DIR, f"fato_populacao_municipal_{timestamp}.parquet")
    path_silver = os.path.join(SILVER_DIR, f"ibge_populacao_silver_{timestamp}.parquet")
    path_quarantine = os.path.join(QUARENTENA_DIR, f"ibge_quarentena_{timestamp}.parquet")

    fato_populacao.write_parquet(path_fato, compression="snappy")
    silver_consolidada.write_parquet(path_silver, compression="snappy")
    quarentena_df.write_parquet(path_quarantine, compression="snappy")

    # 9. Relatório do Funil
    print_funnel_report(
        name="IBGE População Residente (API)",
        total_entered=total_entered,
        total_passed=total_passed,
        total_quarantine=total_quarantine,
        total_duplicates=total_duplicates,
        rejection_reasons=rejection_reasons,
        output_files=[
            f"Fato Populacao (Para) : {path_fato} ({len(fato_populacao):,} municipios aprovados)",
            f"Silver Consolidada     : {path_silver} ({len(silver_consolidada):,} municipios)",
            f"Quarentena             : {path_quarantine} ({len(quarentena_df):,} registros fora de escopo)",
        ]
    )

    return path_silver


if __name__ == "__main__":
    process_silver_ibge()
