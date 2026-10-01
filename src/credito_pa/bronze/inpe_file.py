from __future__ import annotations

import csv
import io
import zipfile

import polars as pl

from credito_pa.bronze.contract import BronzeWriter
from credito_pa.common.control import utc_now
from credito_pa.common.download import InvalidFileError, download_file
from credito_pa.common.http import HttpClient, HttpRequestError
from credito_pa.common.logging import get_logger
from credito_pa.config import Settings

log = get_logger("bronze.inpe")

SOURCE_SYSTEM = "INPE_QUEIMADAS"
DATASET = "inpe_focos"
ENCODING = "utf-8"
SEPARATOR = ","


def parse_inpe_csv(raw: bytes) -> tuple[list[str], list[list[str]], list[dict]]:
    text = raw.decode(ENCODING).lstrip("﻿")
    reader = csv.reader(io.StringIO(text), delimiter=SEPARATOR)
    header = [h.strip() for h in next(reader)]
    rows, bad = [], []
    for line_no, fields in enumerate(reader, start=2):
        if not fields:
            continue
        if len(fields) != len(header):
            bad.append({"raw_record": SEPARATOR.join(fields),
                        "quarantine_reason": f"field_count_mismatch:esperado={len(header)},obtido={len(fields)},linha={line_no}"})
            continue
        rows.append([f.strip() for f in fields])
    return header, rows, bad


def ingest_inpe(settings: Settings, client: HttpClient | None = None, anos: list[int] | None = None) -> dict:
    client = client or HttpClient.from_settings(settings)
    anos = anos or settings.years("INPE_ANOS")
    uf = settings.uf_sigla
    base = settings.get("INPE_BASE_URL").format(uf_sigla=uf)
    writer = BronzeWriter(settings, DATASET, SOURCE_SYSTEM)
    landing = settings.layer_dir("landing") / DATASET
    resumo = {}
    for ano in anos:
        name = f"focos_br_{uf.lower()}_ref_{ano}.zip"
        started = utc_now()
        try:
            path, _ = download_file(client, base + name, landing / name)
        except (HttpRequestError, InvalidFileError) as exc:
            writer.quarantine_file(getattr(exc, "path", None), exc.reason, name, str(exc)[:300])
            resumo[ano] = exc.reason
            continue
        try:
            with zipfile.ZipFile(path) as zf:
                member = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
                header, rows, bad = parse_inpe_csv(zf.read(member))
        except (StopIteration, UnicodeDecodeError, csv.Error, zipfile.BadZipFile) as exc:
            writer.quarantine_file(path, "csv_parse_error", name, str(exc)[:300])
            resumo[ano] = "csv_parse_error"
            continue
        payload = pl.DataFrame(rows, schema={h: pl.Utf8 for h in header}, orient="row")
        r = writer.write_batch(payload, f"{name}/{member}", quarantined=bad, started_at=started,
                               message=f"encoding={ENCODING}; separador='{SEPARATOR}'; focos={len(rows)}")
        resumo[ano] = r.rows_read
    return resumo
