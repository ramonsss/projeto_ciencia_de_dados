from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import polars as pl


def _tmp_path(path: Path) -> Path:
    return path.with_name(f".{path.name}.{uuid.uuid4().hex[:8]}.tmp")


def write_parquet_atomic(df: pl.DataFrame, path: Path, *, overwrite: bool = True) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not overwrite and path.exists():
        raise FileExistsError(f"Arquivo imutável já existe: {path}")
    tmp = _tmp_path(path)
    try:
        df.write_parquet(tmp, compression="zstd")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()
    return path


def write_json_atomic(obj, path: Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(path)
    try:
        tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()
    return path


def write_text_atomic(text: str, path: Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(path)
    try:
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()
    return path


def list_parquet_files(directory: Path) -> list[Path]:
    directory = Path(directory)
    if not directory.exists():
        return []
    return sorted(p for p in directory.rglob("*.parquet") if not p.name.startswith("."))


def scan_parquet_dir(directory: Path) -> pl.LazyFrame | None:
    files = list_parquet_files(directory)
    if not files:
        return None
    return pl.concat([pl.scan_parquet(f) for f in files], how="diagonal_relaxed")


def read_parquet_dir(directory: Path) -> pl.DataFrame | None:
    lf = scan_parquet_dir(directory)
    return None if lf is None else lf.collect()
