from __future__ import annotations

import polars as pl

from credito_pa.config import Settings
from credito_pa.silver.base import (
    SilverStats,
    finalize,
    int_number,
    latest_by_key,
    normalize_column,
    parse_typed,
    plain_number,
    read_bronze,
    to_quarantine,
)
from credito_pa.silver.contracts import DIM_MUNICIPIO, IBGE_PIB_ANO, IBGE_POPULACAO_ANO

PIB_VARIAVEIS = {
    "37": "pib_mil_reais",
    "498": "vab_total_mil_reais",
    "513": "vab_agropecuaria_mil_reais",
    "517": "vab_industria_mil_reais",
    "6575": "vab_servicos_mil_reais",
    "525": "vab_adm_publica_mil_reais",
}


def build_dim_municipio(settings: Settings) -> tuple[pl.DataFrame, SilverStats]:
    c = DIM_MUNICIPIO
    stats = SilverStats(c.name, c.granularity)
    raw = read_bronze(settings, "ibge_municipios")
    stats.linhas_bronze = raw.height
    df, stats.duplicatas_removidas = latest_by_key(raw, ["id"], ["ingestion_timestamp"])
    df = df.with_columns(
        pl.col("id").str.strip_chars().alias("cod_ibge_municipio"),
        pl.col("nome").str.strip_chars().alias("nome_municipio"),
        pl.col("microrregiao_nome").alias("microrregiao"),
        pl.col("mesorregiao_nome").alias("mesorregiao"),
        pl.col("regiao_imediata_nome").alias("regiao_imediata"),
        pl.col("regiao_intermediaria_nome").alias("regiao_intermediaria"),
        pl.col("uf_sigla").str.strip_chars().str.to_uppercase(),
    )
    df = normalize_column(df, "nome_municipio", "nome_normalizado")
    return finalize(settings, df, c, stats, []), stats


def interpolate_missing_years(df: pl.DataFrame) -> pl.DataFrame:
    full = (
        df.group_by("cod_ibge_municipio")
        .agg(pl.col("ano").min().alias("a0"), pl.col("ano").max().alias("a1"))
        .with_columns(pl.int_ranges("a0", pl.col("a1") + 1).alias("ano"))
        .explode("ano", empty_as_null=True)
        .select("cod_ibge_municipio", pl.col("ano").cast(pl.Int32))
    )
    out = full.join(df, on=["cod_ibge_municipio", "ano"], how="left").sort("cod_ibge_municipio", "ano")
    out = out.with_columns(
        pl.col("populacao").cast(pl.Float64).interpolate().over("cod_ibge_municipio").round(0).cast(pl.Int64).alias("pop_interp"),
    )
    return out.with_columns(
        pl.when(pl.col("populacao").is_null()).then(pl.lit("interpolada")).otherwise(pl.col("fonte_populacao")).alias("fonte_populacao"),
        pl.col("pop_interp").alias("populacao"),
    ).drop("pop_interp")


def build_populacao(settings: Settings) -> tuple[pl.DataFrame, SilverStats]:
    c = IBGE_POPULACAO_ANO
    stats = SilverStats(c.name, c.granularity)
    raw = read_bronze(settings, "ibge_populacao")
    stats.linhas_bronze = raw.height
    df, cast_reason = parse_typed(raw, {"populacao": ("V", int_number("V")), "ano_num": ("D3C", int_number("D3C"))})
    df = df.with_columns(cast_reason.alias("motivo"))
    bad = df.filter(pl.col("motivo").is_not_null())
    stats.add_quarantine(bad["motivo"])
    quarantined = [to_quarantine(bad, "motivo", c.name, ["D1C", "D3C", "V"])]
    df = df.filter(pl.col("motivo").is_null()).with_columns(
        pl.col("D1C").str.strip_chars().alias("cod_ibge_municipio"),
        pl.col("ano_num").cast(pl.Int32).alias("ano"),
        pl.when(pl.col("source_object").str.contains("/t4709/")).then(pl.lit("censo")).otherwise(pl.lit("estimativa"))
        .alias("fonte_populacao"),
        pl.when(pl.col("source_object").str.contains("/t4709/")).then(1).otherwise(0).alias("prioridade"),
    )
    df, stats.duplicatas_removidas = latest_by_key(df, ["cod_ibge_municipio", "ano"], ["prioridade", "ingestion_timestamp"])
    df = df.select("cod_ibge_municipio", "ano", "populacao", "fonte_populacao")
    df = interpolate_missing_years(df)
    n_interp = df.filter(pl.col("fonte_populacao") == "interpolada").height
    stats.observacoes.append(f"{n_interp} linhas com população interpolada (anos sem estimativa nem censo).")
    return finalize(settings, df, c, stats, quarantined), stats


def build_pib(settings: Settings, populacao: pl.DataFrame) -> tuple[pl.DataFrame, SilverStats, dict]:
    c = IBGE_PIB_ANO
    stats = SilverStats(c.name, c.granularity)
    raw = read_bronze(settings, "source_db_pib_municipal")
    stats.linhas_bronze = raw.height
    df, stats.duplicatas_removidas = latest_by_key(
        raw, ["cod_municipio", "ano", "variavel_codigo"], ["updated_at", "ingestion_timestamp"]
    )
    df, cast_reason = parse_typed(df, {"valor_num": ("valor", plain_number("valor")), "ano_num": ("ano", int_number("ano"))})
    df = df.with_columns(cast_reason.alias("motivo"))
    bad = df.filter(pl.col("motivo").is_not_null())
    stats.add_quarantine(bad["motivo"])
    quarantined = [to_quarantine(bad, "motivo", c.name, ["cod_municipio", "ano", "variavel_codigo", "valor"])]
    df = df.filter(pl.col("motivo").is_null() & pl.col("variavel_codigo").is_in(list(PIB_VARIAVEIS)))
    wide = (
        df.with_columns(pl.col("variavel_codigo").replace_strict(PIB_VARIAVEIS).alias("variavel"),
                        pl.col("updated_at").str.to_datetime(strict=False).alias("upd"))
        .pivot(on="variavel", index=["cod_municipio", "ano_num"], values="valor_num", aggregate_function="first")
    )
    upd = df.with_columns(pl.col("updated_at").str.to_datetime(strict=False).alias("upd")).group_by(
        "cod_municipio", "ano_num").agg(pl.col("upd").max().alias("updated_at_origem"))
    wide = wide.join(upd, on=["cod_municipio", "ano_num"], how="left").rename(
        {"cod_municipio": "cod_ibge_municipio", "ano_num": "ano"}).with_columns(pl.col("ano").cast(pl.Int32))
    for col in PIB_VARIAVEIS.values():
        if col not in wide.columns:
            wide = wide.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))

    joined = wide.join(populacao.select("cod_ibge_municipio", "ano", "populacao"), on=["cod_ibge_municipio", "ano"], how="left")
    integrity = {
        "join": "ibge_pib_ano x ibge_populacao_ano (cod_ibge_municipio, ano)",
        "lado_fonte_total": wide.height,
        "casados": joined.filter(pl.col("populacao").is_not_null()).height,
        "orfaos_fonte": joined.filter(pl.col("populacao").is_null()).height,
        "exemplos_orfaos_fonte": joined.filter(pl.col("populacao").is_null()).select("cod_ibge_municipio", "ano").head(5).to_dicts(),
        "tratamento": "Mantidos na Silver com populacao e pib_per_capita_reais nulos (não se inventa população).",
    }
    joined = joined.with_columns((pl.col("pib_mil_reais") * 1000 / pl.col("populacao")).alias("pib_per_capita_reais"))
    return finalize(settings, joined, c, stats, quarantined), stats, integrity
