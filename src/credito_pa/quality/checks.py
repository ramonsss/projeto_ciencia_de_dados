from __future__ import annotations

import polars as pl

from credito_pa.quality.contracts import TableContract


class ContractError(Exception):
    pass


def validate_schema(df: pl.DataFrame, contract: TableContract) -> None:
    problems = []
    missing = [c for c in contract.column_names if c not in df.columns]
    extra = [c for c in df.columns if c not in contract.column_names]
    if missing:
        problems.append(f"colunas ausentes: {missing}")
    if extra:
        problems.append(f"colunas fora do contrato: {extra}")
    for col in contract.columns:
        if col.name in df.columns and df.schema[col.name] != col.dtype:
            problems.append(f"{col.name}: tipo {df.schema[col.name]} != contrato {col.dtype}")
    if not problems and df.columns != contract.column_names:
        problems.append("ordem das colunas difere do contrato")
    if problems:
        raise ContractError(f"[{contract.name}] " + "; ".join(problems))


def domain_violations(df: pl.DataFrame, contract: TableContract) -> pl.Series:
    reason = pl.lit(None, dtype=pl.Utf8)
    for col in reversed(contract.columns):
        if col.name not in df.columns:
            continue
        if not col.nullable:
            reason = pl.when(pl.col(col.name).is_null()).then(pl.lit(f"null_nao_permitido:{col.name}")).otherwise(reason)
        if col.check is not None:
            invalid = pl.col(col.name).is_not_null() & ~col.check.fill_null(False)
            reason = pl.when(invalid).then(pl.lit(f"dominio_invalido:{col.name}")).otherwise(reason)
    return df.select(reason.alias("motivo"))["motivo"]


def split_valid(df: pl.DataFrame, contract: TableContract) -> tuple[pl.DataFrame, pl.DataFrame]:
    motivo = domain_violations(df, contract)
    df = df.with_columns(motivo.alias("quarantine_reason"))
    return df.filter(pl.col("quarantine_reason").is_null()).drop("quarantine_reason"), df.filter(
        pl.col("quarantine_reason").is_not_null()
    )


def check_primary_key(df: pl.DataFrame, pk: list[str]) -> dict:
    duplicates = df.height - df.select(pk).unique().height
    nulls = df.select(pl.any_horizontal([pl.col(c).is_null() for c in pk]).sum()).item() if df.height else 0
    return {"pk": pk, "linhas": df.height, "duplicatas_na_pk": int(duplicates), "linhas_com_pk_nula": int(nulls),
            "pk_unica": duplicates == 0, "pk_sem_nulos": nulls == 0}


def assert_primary_key(df: pl.DataFrame, contract: TableContract) -> dict:
    result = check_primary_key(df, contract.primary_key)
    if not (result["pk_unica"] and result["pk_sem_nulos"]):
        raise ContractError(f"[{contract.name}] chave primária violada: {result}")
    return result
