from __future__ import annotations

import polars as pl

from credito_pa.common.io import write_parquet_atomic
from credito_pa.config import Settings
from credito_pa.quality.contracts import TableContract
from credito_pa.silver.base import SilverStats, to_quarantine

KEY = "cod_ibge_municipio"


def check_vs_dim(name: str, df: pl.DataFrame, dim: pl.DataFrame, *, panel_col: str | None = None,
                 tratamento: str) -> dict:
    dim_keys = dim.select(KEY)
    matched = df.join(dim_keys, on=KEY, how="semi")
    orphans = df.join(dim_keys, on=KEY, how="anti")
    present = df.select(KEY).unique()
    dim_orphans = dim.join(present, on=KEY, how="anti")
    result = {
        "join": f"{name} x dim_municipio ({KEY})",
        "lado_fonte_total": df.height,
        "casados": matched.height,
        "orfaos_fonte": orphans.height,
        "exemplos_orfaos_fonte": orphans.select(KEY).unique().head(5)[KEY].to_list(),
        "municipios_dim": dim.height,
        "municipios_com_dado": present.join(dim_keys, on=KEY, how="semi").height,
        "orfaos_dimensao": dim_orphans.height,
        "exemplos_orfaos_dimensao": dim_orphans["nome_municipio"].head(10).to_list(),
        "tratamento": tratamento,
    }
    if panel_col:
        periods = df.select(panel_col).unique()
        grid = dim_keys.join(periods, how="cross")
        have = df.select(KEY, panel_col).unique()
        missing = grid.join(have, on=[KEY, panel_col], how="anti")
        top = (missing.group_by(KEY).agg(pl.len().alias("periodos_ausentes"))
               .join(dim.select(KEY, "nome_municipio"), on=KEY).sort("periodos_ausentes", descending=True).head(5))
        result.update({
            "periodos": periods.height,
            "celulas_painel_esperadas": grid.height,
            "celulas_painel_ausentes": missing.height,
            "municipios_com_ausencia": missing[KEY].n_unique(),
            "maiores_ausencias": top.to_dicts(),
        })
    return result


def enforce_dim(settings: Settings, df: pl.DataFrame, contract: TableContract, dim: pl.DataFrame,
                stats: SilverStats) -> pl.DataFrame:
    orphans = df.join(dim.select(KEY), on=KEY, how="anti")
    if orphans.height == 0:
        return df
    kept = df.join(dim.select(KEY), on=KEY, how="semi")
    write_parquet_atomic(kept, settings.layer_dir("silver") / f"{contract.name}.parquet")
    q = to_quarantine(orphans.with_columns(pl.lit("municipio_nao_encontrado").alias("motivo")), "motivo",
                      contract.name, contract.column_names)
    qpath = settings.layer_dir("quarantine") / "silver" / f"{contract.name}.parquet"
    if qpath.exists():
        q = pl.concat([pl.read_parquet(qpath), q])
    write_parquet_atomic(q, qpath)
    stats.quarantine_add("municipio_nao_encontrado", orphans.height)
    stats.linhas_silver = kept.height
    return kept
