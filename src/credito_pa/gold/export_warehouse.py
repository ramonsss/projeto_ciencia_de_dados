from __future__ import annotations

import polars as pl
from sqlalchemy import func, select, table

from credito_pa.common.db import describe_url, warehouse_engine
from credito_pa.common.logging import get_logger
from credito_pa.config import Settings

log = get_logger("gold.export")


def export_tables(settings: Settings, tables: list[str]) -> dict[str, int]:
    engine = warehouse_engine(settings)
    counts = {}
    with engine.begin() as conn:
        for name in tables:
            df = pl.read_parquet(settings.layer_dir("gold") / f"{name}.parquet")
            df.to_pandas().to_sql(name, conn, if_exists="replace", index=False, chunksize=5000)
            counts[name] = conn.execute(select(func.count()).select_from(table(name))).scalar()
    log.info("Gold exportada para %s: %s", describe_url(engine), counts)
    return counts
