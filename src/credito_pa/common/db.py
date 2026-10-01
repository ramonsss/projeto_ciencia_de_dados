from __future__ import annotations

from pathlib import Path

from sqlalchemy import Column, DateTime, Float, MetaData, String, Integer, Table, create_engine
from sqlalchemy.engine import Engine, make_url

from credito_pa.config import Settings

metadata = MetaData()

pib_municipal = Table(
    "pib_municipal",
    metadata,
    Column("cod_municipio", String(7), primary_key=True),
    Column("ano", Integer, primary_key=True),
    Column("variavel_codigo", String(10), primary_key=True),
    Column("variavel_nome", String(300), nullable=False),
    Column("unidade", String(50), nullable=False),
    Column("valor", Float, nullable=True),
    Column("updated_at", DateTime, nullable=False, index=True),
)


def make_engine(url: str) -> Engine:
    parsed = make_url(url)
    if parsed.get_backend_name() == "sqlite" and parsed.database not in (None, "", ":memory:"):
        Path(parsed.database).parent.mkdir(parents=True, exist_ok=True)
    return create_engine(url, future=True)


def source_engine(settings: Settings) -> Engine:
    return make_engine(settings.source_db_url)


def warehouse_engine(settings: Settings) -> Engine:
    return make_engine(settings.warehouse_db_url)


def describe_url(engine: Engine) -> str:
    return engine.url.render_as_string(hide_password=True)
