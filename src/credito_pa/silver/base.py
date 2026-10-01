from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass, field

import polars as pl

from credito_pa.common.io import scan_parquet_dir, write_parquet_atomic
from credito_pa.common.logging import get_logger
from credito_pa.config import Settings
from credito_pa.quality.checks import assert_primary_key, split_valid, validate_schema
from credito_pa.quality.contracts import TableContract

log = get_logger("silver")

NULL_TOKENS = ["", "-", "...", "..", "X", "x"]


def _clean(col: str) -> pl.Expr:
    s = pl.col(col).cast(pl.Utf8).str.strip_chars()
    return pl.when(s.is_in(NULL_TOKENS)).then(None).otherwise(s).alias(col)


def br_number(col: str) -> pl.Expr:
    return (_clean(col).str.replace_all(".", "", literal=True).str.replace(",", ".", literal=True)
            .cast(pl.Float64, strict=False).alias(col))


def plain_number(col: str) -> pl.Expr:
    return _clean(col).cast(pl.Float64, strict=False).alias(col)


def int_number(col: str) -> pl.Expr:
    return _clean(col).cast(pl.Int64, strict=False).alias(col)


def parse_typed(df: pl.DataFrame, specs: dict[str, tuple[str, pl.Expr]]) -> tuple[pl.DataFrame, pl.Series]:
    reason = pl.lit(None, dtype=pl.Utf8)
    exprs = []
    for target, (src, expr) in reversed(list(specs.items())):
        failed = _clean(src).is_not_null() & expr.is_null()
        reason = pl.when(failed).then(pl.lit(f"cast_error:{target}")).otherwise(reason)
        exprs.append(expr.alias(target))
    out = df.with_columns(reason.alias("__cast_reason"))
    out = out.with_columns(exprs[::-1])
    return out.drop("__cast_reason"), out["__cast_reason"]


def latest_by_key(df: pl.DataFrame, keys: list[str], order: list[str]) -> tuple[pl.DataFrame, int]:
    deduped = df.sort(order).unique(subset=keys, keep="last", maintain_order=True)
    return deduped, df.height - deduped.height


def normalize_name(text: str | None) -> str | None:
    if text is None:
        return None
    ascii_text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").upper()
    return " ".join(re.sub(r"[^A-Z0-9 ]", " ", ascii_text).split())


def normalize_column(df: pl.DataFrame, col: str, alias: str) -> pl.DataFrame:
    mapping = {v: normalize_name(v) for v in df[col].unique().to_list() if v is not None}
    return df.with_columns(pl.col(col).replace_strict(mapping, default=None, return_dtype=pl.Utf8).alias(alias))


def month_end_from_yyyymm(col: str) -> pl.Expr:
    return pl.col(col).str.strip_chars().str.to_date("%Y%m", strict=False).dt.month_end()


def read_bronze(settings: Settings, dataset: str) -> pl.DataFrame:
    lf = scan_parquet_dir(settings.layer_dir("bronze") / dataset)
    if lf is None:
        raise FileNotFoundError(f"Bronze vazia para '{dataset}'. Rode a etapa bronze antes da silver.")
    return lf.collect()


@dataclass
class SilverStats:
    tabela: str
    granularidade: str
    linhas_bronze: int = 0
    quarentena: dict = field(default_factory=dict)
    duplicatas_removidas: int = 0
    linhas_silver: int = 0
    pk: dict = field(default_factory=dict)
    observacoes: list = field(default_factory=list)

    def add_quarantine(self, reasons: pl.Series) -> None:
        for reason, n in reasons.value_counts().iter_rows():
            self.quarantine_add(reason, n)

    def quarantine_add(self, reason: str, n: int) -> None:
        if n:
            self.quarentena[reason] = self.quarentena.get(reason, 0) + int(n)

    def as_dict(self) -> dict:
        return {**self.__dict__, "linhas_quarentena": sum(self.quarentena.values())}


def to_quarantine(df: pl.DataFrame, reason_col: str, table: str, raw_cols: list[str]) -> pl.DataFrame:
    if df.height == 0:
        return pl.DataFrame(schema={"tabela": pl.Utf8, "quarantine_reason": pl.Utf8, "source_object": pl.Utf8,
                                    "record_hash": pl.Utf8, "raw_record": pl.Utf8})
    raw = [json.dumps(r, ensure_ascii=False, default=str) for r in df.select(raw_cols).iter_rows(named=True)]
    return pl.DataFrame({
        "tabela": [table] * df.height,
        "quarantine_reason": df[reason_col].cast(pl.Utf8),
        "source_object": df["source_object"] if "source_object" in df.columns else [None] * df.height,
        "record_hash": df["record_hash"] if "record_hash" in df.columns else [None] * df.height,
        "raw_record": raw,
    }, schema={"tabela": pl.Utf8, "quarantine_reason": pl.Utf8, "source_object": pl.Utf8,
               "record_hash": pl.Utf8, "raw_record": pl.Utf8})


def finalize(settings: Settings, df: pl.DataFrame, contract: TableContract, stats: SilverStats,
             quarantined: list[pl.DataFrame], raw_cols: list[str] | None = None) -> pl.DataFrame:
    typed = df.select(contract.column_names).cast(contract.schema)
    valid, invalid = split_valid(typed, contract)
    if invalid.height:
        stats.add_quarantine(invalid["quarantine_reason"])
        quarantined.append(to_quarantine(invalid, "quarantine_reason", contract.name, contract.column_names))
    valid = valid.sort(contract.primary_key)
    validate_schema(valid, contract)
    stats.pk = assert_primary_key(valid, contract)
    stats.linhas_silver = valid.height

    write_parquet_atomic(valid, settings.layer_dir("silver") / f"{contract.name}.parquet")
    q = pl.concat([x for x in quarantined if x.height], how="vertical") if any(x.height for x in quarantined) else None
    qpath = settings.layer_dir("quarantine") / "silver" / f"{contract.name}.parquet"
    if q is not None:
        write_parquet_atomic(q, qpath)
    elif qpath.exists():
        qpath.unlink()
    log.info("silver.%s: bronze=%d silver=%d quarentena=%d duplicatas=%d", contract.name, stats.linhas_bronze,
             stats.linhas_silver, sum(stats.quarentena.values()), stats.duplicatas_removidas)
    return valid


def read_silver(settings: Settings, table: str) -> pl.DataFrame:
    return pl.read_parquet(settings.layer_dir("silver") / f"{table}.parquet")
