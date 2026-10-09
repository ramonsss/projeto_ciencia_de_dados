from __future__ import annotations

import polars as pl
from sqlalchemy import (
    BigInteger, Boolean, Column, Date, Float, ForeignKey, Integer, MetaData, Table, Text, bindparam, func, insert, select,
    update,
)

from credito_pa.common.db import describe_url, warehouse_engine
from credito_pa.common.logging import get_logger
from credito_pa.config import Settings
from credito_pa.gold.contracts import GOLD_CONTRACTS
from credito_pa.quality.contracts import TableContract
from credito_pa.silver.contracts import DIM_MUNICIPIO

log = get_logger("gold.export")

CHAVE_MUNICIPIO = "cod_ibge_municipio"
LOTE = 5000


def _tipo_sql(dtype: pl.DataType):
    if dtype == pl.Boolean:
        return Boolean
    if dtype == pl.Date:
        return Date
    if dtype == pl.Int64:
        return BigInteger
    if dtype.is_integer():
        return Integer
    if dtype.is_float():
        return Float
    return Text


def _tabela(metadata: MetaData, contract: TableContract, com_fk: bool) -> Table:
    colunas = []
    for col in contract.columns:
        fk = [ForeignKey(f"{DIM_MUNICIPIO.name}.{CHAVE_MUNICIPIO}")] if com_fk and col.name == CHAVE_MUNICIPIO else []
        colunas.append(Column(col.name, _tipo_sql(col.dtype), *fk, primary_key=col.name in contract.primary_key,
                              nullable=col.nullable))
    return Table(contract.name, metadata, *colunas)


def _sincronizar_dim(conn, tabela: Table, df: pl.DataFrame) -> None:
    """A dimensão é atualizada no lugar: apagá-la quebraria as chaves estrangeiras das tabelas que já apontam para ela."""
    tabela.create(conn, checkfirst=True)
    existentes = set(conn.execute(select(tabela.c[CHAVE_MUNICIPIO])).scalars())
    linhas = df.to_dicts()
    novas = [r for r in linhas if r[CHAVE_MUNICIPIO] not in existentes]
    antigas = [{**{k: v for k, v in r.items() if k != CHAVE_MUNICIPIO}, "chave": r[CHAVE_MUNICIPIO]}
               for r in linhas if r[CHAVE_MUNICIPIO] in existentes]
    if novas:
        conn.execute(insert(tabela), novas)
    if antigas:
        conn.execute(update(tabela).where(tabela.c[CHAVE_MUNICIPIO] == bindparam("chave")), antigas)


def export_tables(settings: Settings, tables: list[str]) -> dict[str, int]:
    contratos = {c.name: c for c in GOLD_CONTRACTS}
    engine = warehouse_engine(settings)
    metadata = MetaData()
    dim_path = settings.layer_dir("silver") / f"{DIM_MUNICIPIO.name}.parquet"
    counts = {}
    with engine.begin() as conn:
        if dim_path.exists():
            _sincronizar_dim(conn, _tabela(metadata, DIM_MUNICIPIO, com_fk=False), pl.read_parquet(dim_path))
        for name in tables:
            df = pl.read_parquet(settings.layer_dir("gold") / f"{name}.parquet")
            tabela = _tabela(metadata, contratos[name], com_fk=dim_path.exists())
            tabela.drop(conn, checkfirst=True)
            tabela.create(conn)
            for inicio in range(0, df.height, LOTE):
                conn.execute(insert(tabela), df.slice(inicio, LOTE).to_dicts())
            counts[name] = conn.execute(select(func.count()).select_from(tabela)).scalar()
    log.info("Gold exportada para %s (com chaves primárias%s): %s", describe_url(engine),
             " e estrangeiras para dim_municipio" if dim_path.exists() else "", counts)
    return counts
