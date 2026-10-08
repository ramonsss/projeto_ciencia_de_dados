from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import polars as pl

from credito_pa.common.control import LoadLog, new_load_id, utc_now
from credito_pa.common.hashing import hash_frame
from credito_pa.common.io import list_parquet_files, write_json_atomic, write_parquet_atomic
from credito_pa.common.logging import get_logger
from credito_pa.config import Settings

METADATA_COLUMNS = ["ingestion_timestamp", "source_system", "source_object", "load_id", "record_hash"]
QUARANTINE_COLUMNS = [
    "raw_record",
    "quarantine_reason",
    "stage",
    "source_system",
    "dataset",
    "source_object",
    "load_id",
    "ingestion_timestamp",
]

log = get_logger("bronze")


@dataclass
class BatchResult:
    source_object: str
    rows_read: int
    rows_written: int
    rows_quarantined: int
    rows_skipped_existing: int
    path: Path | None = None


class BronzeWriter:
    def __init__(
        self,
        settings: Settings,
        dataset: str,
        source_system: str,
        *,
        load_id: str | None = None,
        now: datetime | None = None,
    ):
        self.settings = settings
        self.dataset = dataset
        self.source_system = source_system
        self.now = now or utc_now()
        self.load_id = load_id or new_load_id(self.now)
        self.ingestion_timestamp = self.now.isoformat(timespec="seconds")
        self.ingestion_date = self.now.strftime("%Y-%m-%d")
        self.dataset_dir = settings.layer_dir("bronze") / dataset
        self.quarantine_dir = settings.layer_dir("quarantine")
        self.load_log = LoadLog(settings.layer_dir("_control"))
        self._existing: set[str] | None = None
        self._seq = 0

    @property
    def partition_dir(self) -> Path:
        return self.dataset_dir / f"ingestion_date={self.ingestion_date}"

    def existing_hashes(self) -> set[str]:
        if self._existing is None:
            files = list_parquet_files(self.dataset_dir)
            if files:
                hashes = pl.concat([pl.scan_parquet(f).select("record_hash") for f in files]).collect()
                self._existing = set(hashes["record_hash"].to_list())
            else:
                self._existing = set()
        return self._existing

    def _next_part(self, directory: Path) -> Path:
        while True:
            self._seq += 1
            path = directory / f"part-{self.load_id}-{self._seq:05d}.parquet"
            if not path.exists():
                return path

    def quarantine_records(self, records: list[dict], source_object: str, stage: str = "bronze") -> int:
        if not records:
            return 0
        rows = [
            {
                "raw_record": r["raw_record"] if isinstance(r["raw_record"], str) else json.dumps(r["raw_record"], ensure_ascii=False, default=str),
                "quarantine_reason": r["quarantine_reason"],
                "stage": stage,
                "source_system": self.source_system,
                "dataset": self.dataset,
                "source_object": source_object,
                "load_id": self.load_id,
                "ingestion_timestamp": self.ingestion_timestamp,
            }
            for r in records
        ]
        df = pl.DataFrame(rows, schema={c: pl.Utf8 for c in QUARANTINE_COLUMNS})
        directory = self.quarantine_dir / stage / self.dataset / f"ingestion_date={self.ingestion_date}"
        write_parquet_atomic(df, self._next_part(directory), overwrite=False)
        log.warning("%s: %d registro(s) em quarentena (%s)", self.dataset, len(rows), source_object)
        return len(rows)

    def quarantine_file(self, file_path: Path | None, reason: str, source_object: str, detail: str = "") -> Path:
        directory = self.quarantine_dir / "files" / self.dataset / f"ingestion_date={self.ingestion_date}"
        directory.mkdir(parents=True, exist_ok=True)
        stem = f"{self.load_id}__{Path(source_object).name or 'objeto'}"
        target = directory / stem
        if file_path is not None and Path(file_path).exists():
            shutil.copy2(file_path, target)
        write_json_atomic(
            {
                "source_system": self.source_system,
                "dataset": self.dataset,
                "source_object": source_object,
                "load_id": self.load_id,
                "ingestion_timestamp": self.ingestion_timestamp,
                "quarantine_reason": reason,
                "detail": detail,
            },
            directory / f"{stem}.reason.json",
        )
        self.log_object(source_object, status="quarantined_file", rows_read=0, rows_written=0,
                        rows_quarantined=0, rows_skipped=0, message=f"{reason}: {detail}"[:500])
        log.warning("%s: arquivo em quarentena (%s) -> %s", self.dataset, reason, source_object)
        return target

    def write_batch(
        self,
        payload: pl.DataFrame,
        source_object: str,
        *,
        quarantined: list[dict] | None = None,
        started_at: datetime | None = None,
        message: str = "",
    ) -> BatchResult:
        started_at = started_at or utc_now()
        quarantined = quarantined or []
        reserved = set(payload.columns) & set(METADATA_COLUMNS)
        if reserved:
            raise ValueError(f"Payload não pode conter colunas de metadados: {sorted(reserved)}")

        payload = payload.with_columns(pl.all().cast(pl.Utf8))
        n_quarantined = self.quarantine_records(quarantined, source_object)

        hashes = hash_frame(payload)
        existing = self.existing_hashes()
        is_new = pl.Series([h not in existing for h in hashes], dtype=pl.Boolean)
        new_rows = payload.with_columns(hashes).filter(is_new)
        n_skipped = payload.height - new_rows.height

        path = None
        if new_rows.height:
            out = new_rows.with_columns(
                pl.lit(self.ingestion_timestamp).alias("ingestion_timestamp"),
                pl.lit(self.source_system).alias("source_system"),
                pl.lit(source_object).alias("source_object"),
                pl.lit(self.load_id).alias("load_id"),
            ).select([*payload.columns, *METADATA_COLUMNS])
            path = write_parquet_atomic(out, self._next_part(self.partition_dir), overwrite=False)
            existing.update(out["record_hash"].to_list())

        result = BatchResult(
            source_object=source_object,
            rows_read=payload.height + n_quarantined,
            rows_written=new_rows.height,
            rows_quarantined=n_quarantined,
            rows_skipped_existing=n_skipped,
            path=path,
        )
        self.log_object(source_object, status="success", rows_read=result.rows_read,
                        rows_written=result.rows_written, rows_quarantined=n_quarantined,
                        rows_skipped=n_skipped, started_at=started_at, message=message)
        log.info(
            "%s | %s | lidas=%d gravadas=%d quarentena=%d ja_existentes=%d",
            self.dataset, source_object, result.rows_read, result.rows_written, n_quarantined, n_skipped,
        )
        return result

    def log_object(self, source_object: str, *, status: str, rows_read: int, rows_written: int,
                   rows_quarantined: int, rows_skipped: int, started_at: datetime | None = None,
                   message: str = "") -> None:
        self.load_log.append(
            {
                "load_id": self.load_id,
                "layer": "bronze",
                "source_system": self.source_system,
                "dataset": self.dataset,
                "source_object": source_object,
                "started_at": (started_at or self.now).isoformat(timespec="seconds"),
                "finished_at": utc_now().isoformat(timespec="seconds"),
                "status": status,
                "rows_read": rows_read,
                "rows_written": rows_written,
                "rows_quarantined": rows_quarantined,
                "rows_skipped_existing": rows_skipped,
                "message": message,
            }
        )
