from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from credito_pa.common.io import write_json_atomic, write_parquet_atomic

LOAD_LOG_SCHEMA = {
    "load_id": pl.Utf8,
    "layer": pl.Utf8,
    "source_system": pl.Utf8,
    "dataset": pl.Utf8,
    "source_object": pl.Utf8,
    "started_at": pl.Utf8,
    "finished_at": pl.Utf8,
    "status": pl.Utf8,
    "rows_read": pl.Int64,
    "rows_written": pl.Int64,
    "rows_quarantined": pl.Int64,
    "rows_skipped_existing": pl.Int64,
    "message": pl.Utf8,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def new_load_id(now: datetime | None = None) -> str:
    now = now or utc_now()
    return f"{now.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"


class LoadLog:
    def __init__(self, control_dir: Path):
        self.path = Path(control_dir) / "load_log.parquet"

    def read(self) -> pl.DataFrame:
        if not self.path.exists():
            return pl.DataFrame(schema=LOAD_LOG_SCHEMA)
        return pl.read_parquet(self.path)

    def append(self, entry: dict) -> None:
        row = {key: entry.get(key) for key in LOAD_LOG_SCHEMA}
        new = pl.DataFrame([row], schema=LOAD_LOG_SCHEMA)
        df = pl.concat([self.read(), new], how="vertical")
        write_parquet_atomic(df, self.path)


class StateStore:
    def __init__(self, control_dir: Path):
        self.path = Path(control_dir) / "state.json"

    def _read_all(self) -> dict:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def get(self, key: str, default=None):
        return self._read_all().get(key, default)

    def set(self, key: str, value) -> None:
        state = self._read_all()
        state[key] = value
        write_json_atomic(state, self.path)
