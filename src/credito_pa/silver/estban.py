from __future__ import annotations

import re

import polars as pl

from credito_pa.config import Settings
from credito_pa.silver.base import (
    SilverStats,
    _clean,
    br_number,
    finalize,
    int_number,
    latest_by_key,
    month_end_from_yyyymm,
    parse_typed,
    read_bronze,
    to_quarantine,
)
from credito_pa.silver.contracts import ESTBAN_MUNICIPIO_INSTITUICAO_MES, ESTBAN_VERBETES

VERBETE_RE = re.compile(r"^VERBETE_(\d{3})")


def verbete_sources(columns: list[str]) -> dict[str, list[str]]:
    by_code: dict[str, list[str]] = {}
    for col in columns:
        m = VERBETE_RE.match(col)
        if m:
            by_code.setdefault(m.group(1), []).append(col)
    out = {}
    for target in ESTBAN_VERBETES:
        code = target.split("_")[1]
        out[target] = by_code.get(code, [])
    return out


def build_estban(settings: Settings) -> tuple[pl.DataFrame, SilverStats]:
    c = ESTBAN_MUNICIPIO_INSTITUICAO_MES
    stats = SilverStats(c.name, c.granularity)
    raw = read_bronze(settings, "estban_municipio")
    stats.linhas_bronze = raw.height
    sources = verbete_sources(raw.columns)
    stats.observacoes.append("Mapeamento verbete -> colunas da Bronze: " + "; ".join(
        f"{k} <- {[s[:40] for s in v]}" for k, v in sources.items() if len(v) != 1))

    df = raw.with_columns([
        (pl.coalesce([_clean(s) for s in srcs]) if srcs else pl.lit(None, dtype=pl.Utf8)).alias(f"__{t}")
        for t, srcs in sources.items()
    ])

    nao_proc = (pl.col("AGEN_PROCESSADAS").str.strip_chars() == "0") & pl.col("__verbete_160").is_null()
    q1 = df.filter(nao_proc).with_columns(pl.lit("agencia_nao_processada").alias("motivo"))
    stats.quarantine_add("agencia_nao_processada", q1.height)
    raw_cols = ["#DATA_BASE", "UF", "CODMUN", "MUNICIPIO", "CNPJ", "NOME_INSTITUICAO", "AGEN_PROCESSADAS", "CODMUN_IBGE"]
    quarantined = [to_quarantine(q1, "motivo", c.name, raw_cols)]
    df = df.filter(~nao_proc)

    specs = {t: (f"__{t}", br_number(f"__{t}")) for t in sources}
    specs["agencias_processadas"] = ("AGEN_PROCESSADAS", int_number("AGEN_PROCESSADAS"))
    specs["data_competencia"] = ("#DATA_BASE", month_end_from_yyyymm("#DATA_BASE"))
    df, cast_reason = parse_typed(df, specs)
    df = df.with_columns(cast_reason.alias("motivo"))
    bad = df.filter(pl.col("motivo").is_not_null())
    stats.add_quarantine(bad["motivo"])
    quarantined.append(to_quarantine(bad, "motivo", c.name, raw_cols))
    df = df.filter(pl.col("motivo").is_null()).with_columns(
        pl.col("CODMUN_IBGE").str.strip_chars().alias("cod_ibge_municipio"),
        pl.col("CNPJ").str.strip_chars().str.zfill(8).alias("cnpj_raiz"),
        pl.col("NOME_INSTITUICAO").str.strip_chars().alias("nome_instituicao"),
        pl.col("CODMUN").str.strip_chars().alias("codmun_bcb"),
    )
    df, stats.duplicatas_removidas = latest_by_key(df, c.primary_key, ["ingestion_timestamp"])
    return finalize(settings, df, c, stats, quarantined), stats
