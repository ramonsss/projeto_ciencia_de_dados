from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Iterable, Mapping

import polars as pl


def canonical_json(payload: Mapping) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def record_hash(payload: Mapping, occurrence: int = 1) -> str:
    text = canonical_json(payload)
    if occurrence > 1:
        text = f"{text}#{occurrence}"
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def hash_rows(rows: Iterable[Mapping]) -> list[str]:
    seen: Counter[str] = Counter()
    hashes: list[str] = []
    for row in rows:
        canon = canonical_json(row)
        seen[canon] += 1
        n = seen[canon]
        text = canon if n == 1 else f"{canon}#{n}"
        hashes.append(hashlib.sha256(text.encode("utf-8")).hexdigest())
    return hashes


def hash_frame(df: pl.DataFrame) -> pl.Series:
    return pl.Series("record_hash", hash_rows(df.iter_rows(named=True)), dtype=pl.Utf8)


def file_sha256(path, chunk_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()
