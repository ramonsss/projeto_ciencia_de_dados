from __future__ import annotations

from datetime import date

import polars as pl

from credito_pa.common.io import write_parquet_atomic
from credito_pa.common.logging import get_logger
from credito_pa.config import Settings
from credito_pa.quality.checks import ContractError, assert_primary_key, domain_violations, validate_schema
from credito_pa.quality.contracts import TableContract

log = get_logger("gold")


def finalize_gold(settings: Settings, df: pl.DataFrame, contract: TableContract) -> dict:
    df = df.select(contract.column_names).cast(contract.schema).sort(contract.primary_key)
    validate_schema(df, contract)
    motivos = domain_violations(df, contract).drop_nulls()
    if motivos.len():
        raise ContractError(f"[{contract.name}] Gold violou o contrato ({motivos.value_counts().to_dicts()[:5]}). "
                            "Corrija na Silver: a Gold não limpa dados.")
    pk = assert_primary_key(df, contract)
    write_parquet_atomic(df, settings.layer_dir("gold") / f"{contract.name}.parquet")
    log.info("gold.%s: %d linhas, PK única=%s", contract.name, df.height, pk["pk_unica"])
    return {"tabela": contract.name, "granularidade": contract.granularity, **pk}


def read_gold(settings: Settings, table: str) -> pl.DataFrame:
    return pl.read_parquet(settings.layer_dir("gold") / f"{table}.parquet")


def parse_month(value: str) -> date:
    y, m = (int(v) for v in value.split("-"))
    return pl.select(pl.date(y, m, 1).dt.month_end()).item()


def month_grid(inicio: str, fim: str) -> pl.Series:
    return pl.date_range(parse_month(inicio).replace(day=1), parse_month(fim), interval="1mo", eager=True).dt.month_end()
