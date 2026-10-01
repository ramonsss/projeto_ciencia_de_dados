from __future__ import annotations

import os
import zipfile
from pathlib import Path

from credito_pa.common.http import HttpClient
from credito_pa.common.logging import get_logger

log = get_logger("download")

ZIP_SIGNATURE = b"PK\x03\x04"


class InvalidFileError(Exception):
    def __init__(self, path: Path, reason: str, detail: str = ""):
        self.path = path
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason}: {path} {detail}")


def is_valid_zip(path: Path) -> bool:
    path = Path(path)
    if not path.exists() or path.stat().st_size < 4:
        return False
    with path.open("rb") as fh:
        if fh.read(4) != ZIP_SIGNATURE:
            return False
    try:
        with zipfile.ZipFile(path) as zf:
            return zf.testzip() is None if path.stat().st_size < 50_000_000 else bool(zf.namelist())
    except zipfile.BadZipFile:
        return False


def download_file(client: HttpClient, url: str, dest: Path, *, force: bool = False,
                  expect_zip: bool = True) -> tuple[Path, bool]:
    dest = Path(dest)
    if not force and dest.exists() and (not expect_zip or is_valid_zip(dest)):
        log.info("Cache: %s", dest.name)
        return dest, False

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    resp = client.get(url, stream=True)
    try:
        with tmp.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                if chunk:
                    fh.write(chunk)
    finally:
        resp.close()

    if expect_zip and not is_valid_zip(tmp):
        invalid = dest.with_name(dest.name + ".invalid")
        os.replace(tmp, invalid)
        raise InvalidFileError(invalid, "invalid_zip", f"conteúdo baixado de {url} não é um ZIP válido")
    os.replace(tmp, dest)
    log.info("Baixado: %s (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
    return dest, True
