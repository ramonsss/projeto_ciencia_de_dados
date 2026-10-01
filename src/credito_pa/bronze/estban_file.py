from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

import polars as pl

from credito_pa.bronze.contract import BronzeWriter
from credito_pa.common.control import StateStore, utc_now
from credito_pa.common.download import ZIP_SIGNATURE, InvalidFileError, download_file
from credito_pa.common.http import HttpClient, HttpRequestError
from credito_pa.common.logging import get_logger
from credito_pa.config import Settings

log = get_logger("bronze.estban")

SOURCE_SYSTEM = "BCB_ESTBAN"
DATASET = "estban_municipio"
CHECKPOINT_KEY = "estban.last_competencia"
FILE_SUFFIXES = ["_ESTBAN.ZIP", "_ESTBAN.zip", "_ESTBAN.csv.zip", "_ESTBAN.csv"]
ENCODING = "latin-1"
SEPARATOR = ";"
HEADER_PREFIX = "#DATA_BASE"


def month_range(inicio: str, fim: str) -> list[str]:
    y, m = (int(v) for v in inicio.split("-"))
    y_end, m_end = (int(v) for v in fim.split("-"))
    out = []
    while (y, m) <= (y_end, m_end):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def pending_competencias(inicio: str, fim: str, checkpoint: str | None) -> list[str]:
    return [c for c in month_range(inicio, fim) if checkpoint is None or c > checkpoint]


def parse_estban_csv(raw: bytes, uf: str) -> tuple[list[str], list[list[str]], list[dict], int]:
    text = raw.decode(ENCODING)
    lines = text.splitlines()
    try:
        header_idx = next(i for i, line in enumerate(lines[:20]) if line.startswith(HEADER_PREFIX))
    except StopIteration as exc:
        raise ValueError("cabeçalho #DATA_BASE não encontrado") from exc
    reader = csv.reader(io.StringIO("\n".join(lines[header_idx:])), delimiter=SEPARATOR)
    header = [h.strip() for h in next(reader)]
    uf_idx = header.index("UF")
    rows, bad, total = [], [], 0
    for line_no, fields in enumerate(reader, start=header_idx + 2):
        if not fields or all(not f.strip() for f in fields):
            continue
        total += 1
        if len(fields) != len(header):
            bad.append({"raw_record": SEPARATOR.join(fields),
                        "quarantine_reason": f"field_count_mismatch:esperado={len(header)},obtido={len(fields)},linha={line_no}"})
            continue
        if fields[uf_idx].strip().upper() == uf:
            rows.append(fields)
    return header, rows, bad, total


def read_estban_bytes(path: Path) -> tuple[bytes, str]:
    with open(path, "rb") as fh:
        head = fh.read(4096)
    if head.startswith(ZIP_SIGNATURE):
        with zipfile.ZipFile(path) as zf:
            member = next(n for n in zf.namelist() if n.upper().endswith(".CSV"))
            return zf.read(member), member
    if HEADER_PREFIX.encode(ENCODING) in head:
        return Path(path).read_bytes(), Path(path).name
    raise InvalidFileError(Path(path), "invalid_format", "conteúdo não é ZIP nem CSV do ESTBAN")


def _download_competencia(settings: Settings, client: HttpClient, writer: BronzeWriter, comp: str):
    yyyymm = comp.replace("-", "")
    base = settings.get("ESTBAN_BASE_URL")
    landing = settings.layer_dir("landing") / DATASET
    for suffix in FILE_SUFFIXES:
        name = f"{yyyymm}{suffix}"
        try:
            path, _ = download_file(client, base + name, landing / name, expect_zip=False)
            return path, name
        except HttpRequestError as exc:
            if exc.reason == "not_found":
                continue
            writer.quarantine_file(None, exc.reason, name, str(exc)[:300])
            return None, "invalid"
        except InvalidFileError as exc:
            writer.quarantine_file(exc.path, exc.reason, name, exc.detail)
            return None, "invalid"
    return None, "not_published"


def ingest_estban(settings: Settings, client: HttpClient | None = None, *, inicio: str | None = None,
                  fim: str | None = None) -> dict:
    client = client or HttpClient.from_settings(settings)
    state = StateStore(settings.layer_dir("_control"))
    checkpoint = state.get(CHECKPOINT_KEY)
    inicio = inicio or settings.get("ESTBAN_INICIO")
    fim = fim or settings.get("ESTBAN_FIM")
    writer = BronzeWriter(settings, DATASET, SOURCE_SYSTEM)
    pendentes = pending_competencias(inicio, fim, checkpoint)
    log.info("ESTBAN: checkpoint=%s, %d competência(s) pendente(s) até %s", checkpoint, len(pendentes), fim)

    contiguous = True
    processed, failed = [], []
    for comp in pendentes:
        started = utc_now()
        path, name = _download_competencia(settings, client, writer, comp)
        if path is None and name == "not_published":
            log.info("ESTBAN %s ainda não publicado. Encerrando a varredura.", comp)
            writer.log_object(comp, status="not_published", rows_read=0, rows_written=0, rows_quarantined=0,
                              rows_skipped=0, started_at=started, message="404 em todas as variações de nome")
            break
        if path is None:
            failed.append(comp)
            contiguous = False
            continue
        try:
            raw, member = read_estban_bytes(path)
            header, rows, bad, total = parse_estban_csv(raw, settings.uf_sigla)
        except InvalidFileError as exc:
            writer.quarantine_file(path, exc.reason, name, exc.detail)
            path.unlink(missing_ok=True)
            failed.append(comp)
            contiguous = False
            continue
        except (StopIteration, ValueError, UnicodeDecodeError, zipfile.BadZipFile, csv.Error) as exc:
            writer.quarantine_file(path, "csv_parse_error", name, str(exc)[:300])
            failed.append(comp)
            contiguous = False
            continue
        payload = pl.DataFrame(rows, schema={h: pl.Utf8 for h in header}, orient="row")
        writer.write_batch(payload, f"{name}/{member}", quarantined=bad, started_at=started,
                           message=f"linhas_arquivo_todas_ufs={total}; linhas_{settings.uf_sigla}={len(rows)}; encoding={ENCODING}")
        processed.append(comp)
        if contiguous:
            state.set(CHECKPOINT_KEY, comp)
    return {"checkpoint_antes": checkpoint, "checkpoint_depois": state.get(CHECKPOINT_KEY),
            "processadas": processed, "falhas": failed}
