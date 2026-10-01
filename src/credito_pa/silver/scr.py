from __future__ import annotations

import polars as pl

from credito_pa.config import Settings
from credito_pa.silver.base import (
    SilverStats,
    _clean,
    br_number,
    finalize,
    latest_by_key,
    parse_typed,
    read_bronze,
    to_quarantine,
)
from credito_pa.silver.contracts import SCR_DIMS, SCR_PA_MES, SCR_VALORES

PARQUET_ALIASES = {
    "ocupacao": "cnae_ocupacao",
    "carteira_inadimplida_arrastada": "carteira_inadimplencia",
    "vencido_acima_de_15_dias": "carteira_vencida",
    "tcb": "segmento",
}
OPERACOES_SIGILO = ["<= 15", "-1"]


def build_scr(settings: Settings) -> tuple[pl.DataFrame, SilverStats]:
    c = SCR_PA_MES
    stats = SilverStats(c.name, c.granularity)
    raw = read_bronze(settings, "scr_data")
    stats.linhas_bronze = raw.height
    needed = set(SCR_DIMS) | set(SCR_VALORES) | set(PARQUET_ALIASES.values()) | {"submodalidade", "segmento", "numero_de_operacoes"}
    raw = raw.with_columns([pl.lit(None, dtype=pl.Utf8).alias(col) for col in needed if col not in raw.columns])

    is_parquet = pl.col("segmento").is_not_null()
    df = raw.with_columns(
        pl.when(is_parquet).then(pl.lit("parquet_local")).otherwise(pl.lit("csv_oficial")).alias("layout_origem"),
        *[pl.coalesce(pl.col(t), pl.col(s)).alias(t) for t, s in PARQUET_ALIASES.items()],
    ).with_columns(
        pl.when(is_parquet & pl.col("submodalidade").is_not_null())
        .then(pl.col("modalidade") + " - " + pl.col("submodalidade")).otherwise(pl.col("modalidade")).alias("modalidade"),
    )
    df = df.with_columns([pl.coalesce(_clean(d), pl.lit("NAO_INFORMADO")).alias(d) for d in SCR_DIMS if d not in ("cnae_secao", "cnae_subclasse", "ocupacao")])
    df = df.with_columns([pl.coalesce(pl.col(d).cast(pl.Utf8).str.strip_chars(), pl.lit("NAO_INFORMADO")).alias(d)
                          for d in ("cnae_secao", "cnae_subclasse", "ocupacao")])

    nop = pl.col("numero_de_operacoes").str.strip_chars()
    df = df.with_columns(nop.is_in(OPERACOES_SIGILO).fill_null(False).alias("operacoes_ate_15"),
                         pl.when(nop.is_in(OPERACOES_SIGILO)).then(None).otherwise(nop).alias("__nop"))
    specs = {v: (v, br_number(v)) for v in SCR_VALORES}
    specs["numero_de_operacoes"] = ("__nop", pl.col("__nop").cast(pl.Int64, strict=False))
    specs["data_competencia"] = ("data_base", pl.col("data_base").str.strip_chars().str.to_date("%Y-%m-%d", strict=False).dt.month_end())
    df, cast_reason = parse_typed(df, specs)
    df = df.with_columns(cast_reason.alias("motivo"))
    bad = df.filter(pl.col("motivo").is_not_null())
    stats.add_quarantine(bad["motivo"])
    raw_cols = ["data_base", "uf", *SCR_DIMS, "numero_de_operacoes", "carteira_ativa"]
    quarantined = [to_quarantine(bad, "motivo", c.name, raw_cols)]
    df = df.filter(pl.col("motivo").is_null())
    df, stats.duplicatas_removidas = latest_by_key(df, c.primary_key, ["ingestion_timestamp"])
    stats.observacoes.append(
        f"{df.filter(pl.col('operacoes_ate_15')).height} linhas com quantidade de operações omitida pela fonte ('<= 15')."
    )
    return finalize(settings, df, c, stats, quarantined), stats
