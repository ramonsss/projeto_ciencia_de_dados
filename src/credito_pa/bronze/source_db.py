from __future__ import annotations

from datetime import datetime

import polars as pl
from sqlalchemy import select
from sqlalchemy.engine import Engine

from credito_pa.bronze.contract import BronzeWriter
from credito_pa.bronze.ibge_api import SidraQuery, available_years, fetch_sidra_pages, parse_sidra_rows
from credito_pa.common.control import StateStore, utc_now
from credito_pa.common.db import describe_url, metadata, pib_municipal, source_engine
from credito_pa.common.http import HttpClient
from credito_pa.common.logging import get_logger
from credito_pa.config import Settings

log = get_logger("bronze.source_db")

PIB_VARIAVEIS = ["37", "498", "513", "517", "6575", "525"]
PIB_QUERY = SidraQuery("5938", ",".join(PIB_VARIAVEIS), "pib_municipal")
WATERMARK_KEY = "source_db.pib_municipal.updated_at"
SOURCE_OBJECT = "pib_municipal"


def _to_float(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def upsert_pib(engine: Engine, rows: list[dict], now: datetime | None = None) -> dict:
    now = (now or utc_now()).replace(tzinfo=None)
    metadata.create_all(engine)
    stats = {"inserted": 0, "updated": 0, "unchanged": 0}
    with engine.begin() as conn:
        existing = {
            (r.cod_municipio, r.ano, r.variavel_codigo): r.valor
            for r in conn.execute(select(pib_municipal.c.cod_municipio, pib_municipal.c.ano,
                                         pib_municipal.c.variavel_codigo, pib_municipal.c.valor))
        }
        for row in rows:
            key = (row["cod_municipio"], row["ano"], row["variavel_codigo"])
            if key not in existing:
                conn.execute(pib_municipal.insert().values(**row, updated_at=now))
                stats["inserted"] += 1
            elif existing[key] != row["valor"]:
                conn.execute(
                    pib_municipal.update()
                    .where(pib_municipal.c.cod_municipio == key[0])
                    .where(pib_municipal.c.ano == key[1])
                    .where(pib_municipal.c.variavel_codigo == key[2])
                    .values(valor=row["valor"], variavel_nome=row["variavel_nome"], unidade=row["unidade"], updated_at=now)
                )
                stats["updated"] += 1
            else:
                stats["unchanged"] += 1
    return stats


def seed_source_db(settings: Settings, client: HttpClient | None = None, engine: Engine | None = None) -> dict:
    client = client or HttpClient.from_settings(settings)
    engine = engine or source_engine(settings)
    anos = available_years(client, PIB_QUERY.tabela, settings.years("IBGE_PIB_ANOS"))
    rows: list[dict] = []
    for source_object, url, data, err in fetch_sidra_pages(settings, client, PIB_QUERY, anos):
        if err is not None:
            raise RuntimeError(f"Falha ao buscar {source_object}: {err}") from err
        validas, invalidas = parse_sidra_rows(data)
        if invalidas:
            log.warning("%d linha(s) inválidas ignoradas no seed (%s)", len(invalidas), source_object)
        rows.extend(
            {
                "cod_municipio": r["D1C"],
                "ano": int(r["D3C"]),
                "variavel_codigo": r["D2C"],
                "variavel_nome": r["D2N"],
                "unidade": r["MN"],
                "valor": _to_float(r["V"]),
            }
            for r in validas
        )
    stats = upsert_pib(engine, rows)
    log.info("Seed do banco de origem (%s): %s", describe_url(engine), stats)
    return stats


def ingest_source_db(settings: Settings, engine: Engine | None = None) -> dict:
    engine = engine or source_engine(settings)
    state = StateStore(settings.layer_dir("_control"))
    watermark = state.get(WATERMARK_KEY)
    started = utc_now()

    stmt = select(pib_municipal).order_by(pib_municipal.c.updated_at)
    if watermark:
        stmt = stmt.where(pib_municipal.c.updated_at > datetime.fromisoformat(watermark))
    with engine.connect() as conn:
        records = [dict(r._mapping) for r in conn.execute(stmt)]

    mode = "incremental" if watermark else "full_load"
    writer = BronzeWriter(settings, "source_db_pib_municipal", f"RDBMS:{describe_url(engine)}")
    columns = [c.name for c in pib_municipal.columns]
    payload = pl.DataFrame(
        [{k: (v.isoformat() if isinstance(v, datetime) else (None if v is None else str(v))) for k, v in r.items()}
         for r in records],
        schema={c: pl.Utf8 for c in columns},
    )
    result = writer.write_batch(payload, SOURCE_OBJECT, started_at=started,
                                message=f"modo={mode}; watermark_anterior={watermark}")
    if records:
        new_wm = max(r["updated_at"] for r in records).isoformat()
        state.set(WATERMARK_KEY, new_wm)
    else:
        new_wm = watermark
    log.info("Origem relacional: modo=%s lidas=%d watermark %s -> %s", mode, len(records), watermark, new_wm)
    return {"mode": mode, "rows_read": len(records), "rows_written": result.rows_written,
            "watermark_before": watermark, "watermark_after": new_wm}
