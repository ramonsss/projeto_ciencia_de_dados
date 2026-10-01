from __future__ import annotations

import os
import zipfile
from pathlib import Path

import polars as pl

from credito_pa.bronze.contract import BronzeWriter
from credito_pa.common.control import StateStore, utc_now
from credito_pa.common.download import InvalidFileError, download_file
from credito_pa.common.http import HttpClient, HttpRequestError
from credito_pa.common.logging import get_logger
from credito_pa.config import Settings

log = get_logger("bronze.scr")

SOURCE_SYSTEM = "BCB_SCR_DATA"
DATASET = "scr_data"
SEPARATOR = ";"
FINGERPRINT_KEY = "scr.processed_objects"
CHUNK = 1 << 22


def detect_encoding(sample: bytes) -> str:
    try:
        sample.decode("utf-8")
        return "utf8"
    except UnicodeDecodeError as exc:
        if exc.reason == "unexpected end of data" and exc.start >= len(sample) - 3:
            return "utf8"
        return "latin-1"


def extract_member_utf8(zf: zipfile.ZipFile, member: str, dest: Path) -> tuple[str, int]:
    with zf.open(member) as src:
        sample = src.read(1 << 16)
    encoding = detect_encoding(sample)
    newlines = 0
    with zf.open(member) as src, open(dest, "wb") as out:
        while True:
            chunk = src.read(CHUNK)
            if not chunk:
                break
            newlines += chunk.count(b"\n")
            out.write(chunk if encoding == "utf8" else chunk.decode("latin-1").encode("utf-8"))
    return encoding, max(newlines - 1, 0)


def scan_uf(csv_path: Path, uf: str) -> pl.DataFrame:
    lf = pl.scan_csv(csv_path, separator=SEPARATOR, infer_schema=False, quote_char='"', encoding="utf8")
    return lf.filter(pl.col("uf").str.strip_chars() == uf).collect(engine="streaming")


def _fingerprint(zip_path: Path, info: zipfile.ZipInfo) -> str:
    return f"{zip_path.stat().st_size}:{info.CRC}:{info.file_size}"


def _ingest_zip_year(settings: Settings, writer: BronzeWriter, zip_path: Path, state: StateStore,
                     force: bool) -> list[str]:
    processed = state.get(FINGERPRINT_KEY, {}) or {}
    tmp_dir = settings.layer_dir("landing") / DATASET / "_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    done = []
    with zipfile.ZipFile(zip_path) as zf:
        members = sorted((i for i in zf.infolist() if i.filename.lower().endswith(".csv")), key=lambda i: i.filename)
        for info in members:
            source_object = f"{zip_path.name}/{info.filename}"
            fp = _fingerprint(zip_path, info)
            if not force and processed.get(source_object) == fp:
                writer.log_object(source_object, status="skipped_unchanged", rows_read=0, rows_written=0,
                                  rows_quarantined=0, rows_skipped=0, message=f"fingerprint={fp}")
                log.info("SCR %s inalterado (fingerprint). Pulando.", source_object)
                continue
            started = utc_now()
            tmp = tmp_dir / f"{Path(info.filename).stem}.utf8.csv"
            try:
                encoding, total = extract_member_utf8(zf, info.filename, tmp)
                df = scan_uf(tmp, settings.uf_sigla)
            except (pl.exceptions.PolarsError, UnicodeError, zipfile.BadZipFile, OSError) as exc:
                writer.quarantine_file(None, "csv_parse_error", source_object, f"{type(exc).__name__}: {exc}"[:300])
                continue
            finally:
                if tmp.exists():
                    os.remove(tmp)
            writer.write_batch(df, source_object, started_at=started,
                               message=f"modo=csv; encoding={encoding}; linhas_arquivo_todas_ufs={total}; "
                                       f"linhas_{settings.uf_sigla}={df.height}")
            processed[source_object] = fp
            state.set(FINGERPRINT_KEY, processed)
            done.append(source_object)
    return done


def ingest_scr_parquet(settings: Settings, writer: BronzeWriter, anos: list[int]) -> list[str]:
    path = settings.scr_local_parquet
    lf = pl.scan_parquet(path).filter(
        (pl.col("uf") == settings.uf_sigla) & pl.col("data_base").str.slice(0, 4).cast(pl.Int32).is_in(anos)
    )
    df = lf.collect(engine="streaming")
    done = []
    for (data_base,), part in df.sort("data_base").group_by(["data_base"], maintain_order=True):
        source_object = f"{path.name}#data_base={data_base}"
        writer.write_batch(part, source_object, message=f"modo=parquet; linhas_{settings.uf_sigla}={part.height}")
        done.append(source_object)
    return done


def ingest_scr(settings: Settings, client: HttpClient | None = None, *, anos: list[int] | None = None,
               mode: str | None = None, force: bool = False) -> dict:
    mode = (mode or settings.get("SCR_SOURCE_MODE")).lower()
    anos = anos or settings.years("SCR_ANOS")
    writer = BronzeWriter(settings, DATASET, SOURCE_SYSTEM)
    state = StateStore(settings.layer_dir("_control"))
    if mode == "parquet":
        return {"mode": "parquet", "objetos": ingest_scr_parquet(settings, writer, anos)}

    client = client or HttpClient.from_settings(settings)
    landing = settings.layer_dir("landing") / DATASET
    objetos, fallback_anos = [], []
    for ano in anos:
        url = settings.get("SCR_BASE_URL").format(ano=ano)
        try:
            zip_path, _ = download_file(client, url, landing / f"planilha_{ano}.zip")
        except (HttpRequestError, InvalidFileError) as exc:
            path = getattr(exc, "path", None)
            writer.quarantine_file(path, exc.reason, f"planilha_{ano}.zip", str(exc)[:300])
            if mode == "auto" and settings.scr_local_parquet.exists():
                log.warning("SCR %d: download falhou (%s). Usando fallback Parquet local.", ano, exc.reason)
                fallback_anos.append(ano)
            continue
        objetos += _ingest_zip_year(settings, writer, zip_path, state, force)
    if fallback_anos:
        objetos += ingest_scr_parquet(settings, writer, fallback_anos)
    return {"mode": mode, "objetos": objetos, "fallback_anos": fallback_anos}
