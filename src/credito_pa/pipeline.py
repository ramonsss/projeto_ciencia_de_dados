from __future__ import annotations

import importlib
import time

from credito_pa.common.logging import get_logger
from credito_pa.config import Settings

log = get_logger("pipeline")

BRONZE_SOURCES = {
    "ibge": [("credito_pa.bronze.ibge_api", "ingest_localidades"), ("credito_pa.bronze.ibge_api", "ingest_populacao")],
    "source_db": [("credito_pa.bronze.source_db", "ingest_source_db")],
    "estban": [("credito_pa.bronze.estban_file", "ingest_estban")],
    "scr": [("credito_pa.bronze.scr_file", "ingest_scr")],
    "inpe": [("credito_pa.bronze.inpe_file", "ingest_inpe")],
}

STAGES = {
    "seed": [("credito_pa.bronze.source_db", "seed_source_db")],
    "silver": [("credito_pa.silver.run", "run_silver")],
    "gold": [("credito_pa.gold.run", "run_gold")],
    "ml": [("credito_pa.ml.run", "run_ml")],
    "report": [("credito_pa.ml.run", "run_report")],
}

ORDER_ALL = ["seed", "bronze", "silver", "gold", "ml", "report"]


def _call(module: str, func: str, settings: Settings):
    fn = getattr(importlib.import_module(module), func)
    t0 = time.perf_counter()
    result = fn(settings)
    log.info("%s.%s concluído em %.1fs", module.rsplit(".", 1)[-1], func, time.perf_counter() - t0)
    return result


def run_bronze(settings: Settings, sources: list[str] | None = None) -> dict:
    results = {}
    for source in sources or list(BRONZE_SOURCES):
        if source not in BRONZE_SOURCES:
            raise ValueError(f"Fonte desconhecida: {source}. Opções: {sorted(BRONZE_SOURCES)}")
        for module, func in BRONZE_SOURCES[source]:
            results[f"{source}.{func}"] = _call(module, func, settings)
    return results


def run_stage(settings: Settings, stage: str, sources: list[str] | None = None):
    if stage == "bronze":
        return run_bronze(settings, sources)
    if stage == "all":
        return {s: run_stage(settings, s, sources) for s in ORDER_ALL}
    if stage not in STAGES:
        raise ValueError(f"Etapa desconhecida: {stage}. Opções: bronze, all, {', '.join(STAGES)}")
    return [_call(module, func, settings) for module, func in STAGES[stage]]
