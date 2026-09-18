"""
Camada Silver — Tratamento, Separação de Colunas e Modelagem Dimensional do SCR (BACEN).
Aplica regras estritas de qualidade de dados:
  - Quarentena: Registros com porte 'INDISPONÍVEL' (66.480 registros incompletos) são direcionados para a Quarentena.
  - Decomposição: 'data_base' em múltiplos grãos temporais (ano, mês, trimestre, semestre, ano_mês).
  - Decomposição: 'porte' em tipo de porte (PF salários mínimos vs PJ empresarial) e ordem de grandeza.
  - Decomposição: 'cnae_ocupacao' em macro setor econômico.
  - Agregação por buckets de vencimento financeiro (Curto, Médio e Longo Prazo, Atraso Crítico).
  - Modelagem: dim_tempo_scr, dim_modalidade_credito, fato_scr_credito e visão consolidada.
"""
import os
from datetime import datetime
import polars as pl

from base import print_banner, print_funnel_report, get_latest_file


BRONZE_DIR = "data/bronze/scr"
SILVER_DIR = "data/silver/scr"
DIMENSOES_DIR = "data/silver/dimensoes"
QUARENTENA_DIR = "data/silver/quarentena"

COLUNAS_MONETARIAS = [
    "a_vencer_ate_90_dias",
    "a_vencer_de_91_ate_360_dias",
    "a_vencer_de_361_ate_1080_dias",
    "a_vencer_de_1081_ate_1800_dias",
    "a_vencer_de_1801_ate_5400_dias",
    "a_vencer_acima_de_5400_dias",
    "carteira_a_vencer",
    "vencido_de_15_ate_90_dias",
    "vencido_acima_de_90_dias",
    "carteira_vencida",
    "carteira_ativa",
    "carteira_inadimplencia",
    "ativo_problematico",
]

COLUNAS_CATEGORICAS = [
    "segmento",
    "cliente",
    "cnae_ocupacao",
    "porte",
    "modalidade",
    "submodalidade",
    "origem",
    "indexador",
]

CHAVES_DEDUP = [
    "data_base",
    "uf",
    "segmento",
    "cliente",
    "cnae_ocupacao",
    "porte",
    "modalidade",
    "submodalidade",
    "origem",
    "indexador",
]


def process_silver_scr():
    print_banner("Tratamento & Modelagem Dimensional: SCR BACEN (Crédito - PA)")

    # 1. Leitura Bronze
    bronze_path = get_latest_file(BRONZE_DIR, "parquet")
    print(f"[+] Lendo arquivo Bronze: {bronze_path}")

    lf = pl.scan_parquet(bronze_path)
    total_entered = lf.select(pl.len()).collect().item()
    print(f"[+] Registros de entrada lidos da Bronze: {total_entered:,}")

    # 2. Conversão de valores monetários
    exprs_monetarias = [
        pl.col(c)
          .str.replace_all(r"\.", "")
          .str.replace(",", ".")
          .cast(pl.Float64, strict=False)
          .fill_null(0.0)
          .alias(c)
        for c in COLUNAS_MONETARIAS
    ]

    # 3. Tratamento e normalização textual
    exprs_categoricas = [
        pl.col(c).str.strip_chars().str.to_uppercase().alias(c)
        for c in COLUNAS_CATEGORICAS
    ]

    lf = lf.with_columns(
        exprs_monetarias +
        exprs_categoricas +
        [
            pl.col("data_base").str.to_date("%Y-%m-%d", strict=False).alias("data_base"),
            pl.col("uf").str.strip_chars().str.to_uppercase().alias("uf"),
            pl.col("numero_de_operacoes").cast(pl.Int64, strict=False).alias("numero_de_operacoes"),
        ]
    )

    # 4. Decomposição de Colunas & Engenharia de Atributos:
    # A) Decomposição temporal
    lf = lf.with_columns([
        pl.col("data_base").dt.year().cast(pl.Int32).alias("ano"),
        pl.col("data_base").dt.month().cast(pl.Int32).alias("mes"),
        pl.col("data_base").dt.quarter().cast(pl.Int32).alias("trimestre"),
        ((pl.col("data_base").dt.month() - 1) // 6 + 1).cast(pl.Int32).alias("semestre"),
        pl.col("data_base").dt.strftime("%Y-%m").alias("ano_mes"),
        (pl.col("numero_de_operacoes") == -1).alias("flag_sigilo_estatistico"),
    ])

    # B) Decomposição do Porte
    lf = lf.with_columns([
        pl.when(pl.col("cliente") == "PF")
          .then(pl.lit("FAIXA_SALARIAL_PF"))
          .when(pl.col("porte").str.contains("SALÁRIOS|SALARIOS"))
          .then(pl.lit("FAIXA_SALARIAL_PF"))
          .when(pl.col("porte").is_in(["MICRO", "PEQUENO", "MÉDIO", "MEDIO", "GRANDE"]))
          .then(pl.lit("PORTE_EMPRESARIAL_PJ"))
          .otherwise(pl.lit("INDISPONIVEL"))
          .alias("tipo_porte"),
        
        pl.when(pl.col("porte").str.contains("ATÉ 1 SALÁRIO|ATE 1 SALARIO")).then(1)
          .when(pl.col("porte").str.contains("1 A 2")).then(2)
          .when(pl.col("porte").str.contains("2 A 3")).then(3)
          .when(pl.col("porte").str.contains("3 A 5")).then(4)
          .when(pl.col("porte").str.contains("5 A 10")).then(5)
          .when(pl.col("porte").str.contains("10 A 20")).then(6)
          .when(pl.col("porte").str.contains("ACIMA DE 20")).then(7)
          .when(pl.col("porte") == "MICRO").then(2)
          .when(pl.col("porte") == "PEQUENO").then(4)
          .when(pl.col("porte").is_in(["MÉDIO", "MEDIO"])).then(6)
          .when(pl.col("porte") == "GRANDE").then(8)
          .otherwise(0)
          .cast(pl.Int32)
          .alias("ordem_porte"),
    ])

    # C) Decomposição de CNAE / Ocupação em Macro Setor Econômico
    lf = lf.with_columns(
        pl.when(pl.col("cliente") == "PF")
          .then(pl.lit("PESSOA_FISICA"))
          .when(pl.col("cnae_ocupacao").str.contains("AGRO|RURAL|AGRIC|PECUAR"))
          .then(pl.lit("AGROPECUARIA"))
          .when(pl.col("cnae_ocupacao").str.contains("INDÚSTRIA|INDUSTRIA|FABRICA"))
          .then(pl.lit("INDUSTRIA"))
          .when(pl.col("cnae_ocupacao").str.contains("COMÉRCIO|COMERCIO|VAREJO|ATACADO"))
          .then(pl.lit("COMERCIO"))
          .when(pl.col("cnae_ocupacao").str.contains("SERVIÇO|SERVICO|TRANSPORTE|FINANC"))
          .then(pl.lit("SERVICOS"))
          .when(pl.col("cnae_ocupacao").str.contains("CONSTRUÇÃO|CONSTRUCAO"))
          .then(pl.lit("CONSTRUCAO_CIVIL"))
          .otherwise(pl.lit("OUTROS_SETORES"))
          .alias("macro_setor")
    )

    # D) Decomposição dos Buckets de Prazos
    lf = lf.with_columns([
        (pl.col("a_vencer_ate_90_dias") + pl.col("a_vencer_de_91_ate_360_dias")).alias("carteira_curto_prazo"),
        (pl.col("a_vencer_de_361_ate_1080_dias") + pl.col("a_vencer_de_1081_ate_1800_dias")).alias("carteira_medio_prazo"),
        (pl.col("a_vencer_de_1801_ate_5400_dias") + pl.col("a_vencer_acima_de_5400_dias")).alias("carteira_longo_prazo"),
        pl.col("vencido_de_15_ate_90_dias").alias("carteira_atraso_curto"),
        pl.col("vencido_acima_de_90_dias").alias("carteira_atraso_critico"),
    ])

    # E) Taxas Financeiras de Risco
    lf = lf.with_columns([
        pl.when(pl.col("carteira_ativa") > 0)
          .then(pl.col("carteira_inadimplencia") / pl.col("carteira_ativa"))
          .otherwise(0.0)
          .alias("taxa_inadimplencia"),
        pl.when(pl.col("carteira_ativa") > 0)
          .then(pl.col("ativo_problematico") / pl.col("carteira_ativa"))
          .otherwise(0.0)
          .alias("taxa_ativo_problematico"),
    ])

    # 5. Regras Estritas de Quarentena:
    # Registros com porte 'INDISPONÍVEL' são dados incompletos que enviesam a análise regional -> Quarentena!
    lf = lf.with_columns(
        pl.when(pl.col("porte").str.to_uppercase() == "INDISPONÍVEL")
          .then(pl.lit("DADO_INCOMPLETO_PORTE_INDISPONIVEL"))
          .when(pl.col("data_base").is_null())
          .then(pl.lit("DATA_BASE_NULA_OU_INVALIDA"))
          .when(pl.col("carteira_ativa") < 0)
          .then(pl.lit("CARTEIRA_ATIVA_NEGATIVA"))
          .when(pl.col("carteira_inadimplencia") < 0)
          .then(pl.lit("CARTEIRA_INADIMPLENCIA_NEGATIVA"))
          .otherwise(pl.lit(None))
          .alias("motivo_quarentena")
    )

    # Remove metadados da Bronze
    colunas_metadata = [c for c in lf.collect_schema().names() if c.startswith("_metadata")]
    if colunas_metadata:
        lf = lf.drop(colunas_metadata)

    print("[*] Coletando e processando transformações da base...")
    df = lf.collect()

    # 6. Separação: Aprovados vs Quarentena
    quarentena_df = df.filter(pl.col("motivo_quarentena").is_not_null())
    limpos_df = df.filter(pl.col("motivo_quarentena").is_null())

    total_quarantine = len(quarentena_df)

    rejection_reasons = {}
    if total_quarantine > 0:
        counts = quarentena_df["motivo_quarentena"].value_counts()
        for row in counts.to_dicts():
            rejection_reasons[row["motivo_quarentena"]] = row["count"]

    # 7. Deduplicação nos registros limpos
    total_antes_dedup = len(limpos_df)
    limpos_df = limpos_df.unique(subset=CHAVES_DEDUP, keep="first")
    total_duplicates = total_antes_dedup - len(limpos_df)
    total_passed = len(limpos_df)

    limpos_df = limpos_df.drop("motivo_quarentena")

    # 8. Modelagem em Múltiplas Tabelas:
    # A) Dimensão Tempo
    dim_tempo = (
        limpos_df
        .select(["data_base", "ano", "mes", "trimestre", "semestre", "ano_mes"])
        .unique(subset=["data_base"])
        .sort("data_base")
    )

    # B) Dimensão Modalidade
    dim_modalidade = (
        limpos_df
        .select(["modalidade", "submodalidade"])
        .unique(subset=["modalidade", "submodalidade"])
        .sort(["modalidade", "submodalidade"])
    )

    # C) Tabela Fato de Crédito
    fato_scr = (
        limpos_df
        .select([
            "data_base",
            "uf",
            "segmento",
            "cliente",
            "macro_setor",
            "tipo_porte",
            "ordem_porte",
            "modalidade",
            "submodalidade",
            "origem",
            "indexador",
            "numero_de_operacoes",
            "flag_sigilo_estatistico",
            "carteira_curto_prazo",
            "carteira_medio_prazo",
            "carteira_longo_prazo",
            "carteira_atraso_curto",
            "carteira_atraso_critico",
            "carteira_a_vencer",
            "carteira_vencida",
            "carteira_ativa",
            "carteira_inadimplencia",
            "ativo_problematico",
            "taxa_inadimplencia",
            "taxa_ativo_problematico",
        ])
    )

    # 9. Salvamento
    os.makedirs(SILVER_DIR, exist_ok=True)
    os.makedirs(DIMENSOES_DIR, exist_ok=True)
    os.makedirs(QUARENTENA_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path_dim_tempo = os.path.join(DIMENSOES_DIR, "dim_tempo_scr.parquet")
    path_dim_modalidade = os.path.join(DIMENSOES_DIR, "dim_modalidade_credito.parquet")
    path_fato = os.path.join(SILVER_DIR, f"fato_scr_credito_{timestamp}.parquet")
    path_silver = os.path.join(SILVER_DIR, f"scr_credito_silver_{timestamp}.parquet")
    path_quarantine = os.path.join(QUARENTENA_DIR, f"scr_quarentena_{timestamp}.parquet")

    dim_tempo.write_parquet(path_dim_tempo, compression="snappy")
    dim_modalidade.write_parquet(path_dim_modalidade, compression="snappy")
    fato_scr.write_parquet(path_fato, compression="snappy")
    limpos_df.write_parquet(path_silver, compression="snappy")
    quarentena_df.write_parquet(path_quarantine, compression="snappy")

    # 10. Relatório do Funil
    print_funnel_report(
        name="SCR BACEN (Crédito - Pará)",
        total_entered=total_entered,
        total_passed=total_passed,
        total_quarantine=total_quarantine,
        total_duplicates=total_duplicates,
        rejection_reasons=rejection_reasons,
        output_files=[
            f"Fato SCR Credito    : {path_fato} ({len(fato_scr):,} registros aprovados)",
            f"Consolidada Silver  : {path_silver} ({len(limpos_df):,} registros)",
            f"Quarentena          : {path_quarantine} ({len(quarentena_df):,} registros incompletos)",
        ]
    )

    return path_silver


if __name__ == "__main__":
    process_silver_scr()
