from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl


@dataclass(frozen=True)
class Col:
    name: str
    dtype: pl.DataType
    description: str
    origem: str
    domain: str = ""
    nullable: bool = False
    check: pl.Expr | None = None


@dataclass(frozen=True)
class TableContract:
    name: str
    layer: str
    granularity: str
    primary_key: list[str]
    columns: list[Col]
    description: str = ""
    notes: list[str] = field(default_factory=list)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def schema(self) -> dict[str, pl.DataType]:
        return {c.name: c.dtype for c in self.columns}

    def col(self, name: str) -> Col:
        return next(c for c in self.columns if c.name == name)
