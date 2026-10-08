from datetime import datetime, timezone

import polars as pl
import pytest
from sqlalchemy import func, select

from credito_pa.bronze import source_db
from credito_pa.bronze.source_db import WATERMARK_KEY, ingest_source_db, upsert_pib
from credito_pa.common.control import StateStore
from credito_pa.common.db import describe_url, make_engine, pib_municipal, source_engine
from credito_pa.common.io import read_parquet_dir

T1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
T2 = datetime(2026, 2, 1, tzinfo=timezone.utc)


def _rows(valor_belem=100.0):
    base = {"variavel_codigo": "37", "variavel_nome": "PIB", "unidade": "Mil Reais", "ano": 2021}
    return [
        {**base, "cod_municipio": "1501402", "valor": valor_belem},
        {**base, "cod_municipio": "1504208", "valor": 50.0},
    ]


def _count(engine):
    with engine.connect() as c:
        return c.execute(select(func.count()).select_from(pib_municipal)).scalar()


def test_engine_vem_da_variavel_de_ambiente(settings):
    eng = source_engine(settings)
    assert eng.url.get_backend_name() == "sqlite"
    assert "source.db" in describe_url(eng)


def test_engine_postgres_sem_alterar_codigo():
    pytest.importorskip("psycopg2")
    eng = make_engine("postgresql+psycopg2://u:segredo@host:5432/db")
    assert "segredo" not in describe_url(eng)


def test_seed_idempotente(settings):
    eng = source_engine(settings)
    s1 = upsert_pib(eng, _rows(), now=T1)
    s2 = upsert_pib(eng, _rows(), now=T2)
    assert s1["inserted"] == 2 and s2 == {"inserted": 0, "updated": 0, "unchanged": 2}
    assert _count(eng) == 2


def test_full_load_depois_incremental_com_versoes_preservadas(settings):
    eng = source_engine(settings)
    upsert_pib(eng, _rows(), now=T1)
    r1 = ingest_source_db(settings, eng)
    assert r1["mode"] == "full_load" and r1["rows_written"] == 2
    assert StateStore(settings.layer_dir("_control")).get(WATERMARK_KEY).startswith("2026-01-01")

    r_vazio = ingest_source_db(settings, eng)
    assert r_vazio["mode"] == "incremental" and r_vazio["rows_read"] == 0

    upsert_pib(eng, _rows(valor_belem=120.0), now=T2)
    r2 = ingest_source_db(settings, eng)
    assert r2["rows_read"] == 1 and r2["rows_written"] == 1
    bronze = read_parquet_dir(settings.layer_dir("bronze") / "source_db_pib_municipal")
    belem = bronze.filter(pl.col("cod_municipio") == "1501402").sort("updated_at")
    assert belem["valor"].to_list() == ["100.0", "120.0"]
    assert r2["watermark_after"].startswith("2026-02-01")


def test_watermark_nao_avanca_em_falha(settings, monkeypatch):
    eng = source_engine(settings)
    upsert_pib(eng, _rows(), now=T1)

    def explode(*a, **k):
        raise OSError("disco cheio")

    monkeypatch.setattr(source_db.BronzeWriter, "write_batch", explode)
    with pytest.raises(OSError):
        ingest_source_db(settings, eng)
    assert StateStore(settings.layer_dir("_control")).get(WATERMARK_KEY) is None
