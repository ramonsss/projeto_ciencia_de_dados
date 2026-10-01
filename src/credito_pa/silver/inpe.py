from __future__ import annotations

import polars as pl

from credito_pa.config import Settings
from credito_pa.silver.base import (
    SilverStats,
    finalize,
    latest_by_key,
    normalize_column,
    parse_typed,
    plain_number,
    read_bronze,
    to_quarantine,
)
from credito_pa.silver.contracts import INPE_FOCOS_PA

NAME_EXCEPTIONS: dict[str, str] = {}


def build_inpe(settings: Settings, dim: pl.DataFrame) -> tuple[pl.DataFrame, SilverStats, dict]:
    c = INPE_FOCOS_PA
    stats = SilverStats(c.name, c.granularity)
    raw = read_bronze(settings, "inpe_focos")
    stats.linhas_bronze = raw.height
    df, cast_reason = parse_typed(raw, {
        "latitude": ("lat", plain_number("lat")),
        "longitude": ("lon", plain_number("lon")),
        "data_hora_utc": ("data_pas", pl.col("data_pas").str.strip_chars().str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False)),
    })
    df = df.with_columns(cast_reason.alias("motivo"))
    raw_cols = ["foco_id", "lat", "lon", "data_pas", "municipio", "estado", "bioma"]
    bad = df.filter(pl.col("motivo").is_not_null())
    stats.add_quarantine(bad["motivo"])
    quarantined = [to_quarantine(bad, "motivo", c.name, raw_cols)]
    df = df.filter(pl.col("motivo").is_null())
    df, stats.duplicatas_removidas = latest_by_key(df, ["foco_id"], ["ingestion_timestamp"])

    df = normalize_column(df, "municipio", "nome_normalizado")
    df = df.with_columns(pl.col("nome_normalizado").replace(NAME_EXCEPTIONS))
    joined = df.join(dim.select("nome_normalizado", "cod_ibge_municipio"), on="nome_normalizado", how="left")
    orfaos = joined.filter(pl.col("cod_ibge_municipio").is_null()).with_columns(pl.lit("municipio_nao_encontrado").alias("motivo"))
    stats.quarantine_add("municipio_nao_encontrado", orfaos.height)
    quarantined.append(to_quarantine(orfaos, "motivo", c.name, raw_cols))
    focos_por_mun = joined.filter(pl.col("cod_ibge_municipio").is_not_null()).select("cod_ibge_municipio").unique()
    integrity = {
        "join": "inpe_focos (nome do município normalizado) x dim_municipio",
        "lado_fonte_total": joined.height,
        "casados": joined.height - orfaos.height,
        "orfaos_fonte": orfaos.height,
        "exemplos_orfaos_fonte": orfaos["municipio"].unique().head(10).to_list(),
        "orfaos_dimensao": dim.height - focos_por_mun.height,
        "exemplos_orfaos_dimensao": dim.join(focos_por_mun, on="cod_ibge_municipio", how="anti")["nome_municipio"].head(10).to_list(),
        "tratamento": "Focos sem município casado vão para a quarentena ('municipio_nao_encontrado'). Municípios sem "
                      "foco no período ficam com 0 focos na Gold (ausência de foco é informação, não dado faltante).",
    }
    joined = joined.filter(pl.col("cod_ibge_municipio").is_not_null()).with_columns(
        pl.col("data_hora_utc").dt.date().dt.month_end().alias("data_competencia"),
        pl.col("municipio").alias("municipio_inpe"),
        pl.col("bioma").str.strip_chars(),
    )
    return finalize(settings, joined, c, stats, quarantined), stats, integrity
