import argparse

import polars as pl

from credito_pa.bronze.scr_file import ingest_scr
from credito_pa.common.control import LoadLog
from credito_pa.common.io import list_parquet_files
from credito_pa.config import load_settings
from credito_pa.pipeline import BRONZE_SOURCES, run_bronze


def bronze_counts(settings) -> dict[str, int]:
    root = settings.layer_dir("bronze")
    counts = {}
    for ds in sorted(p.name for p in root.iterdir() if p.is_dir()) if root.exists() else []:
        files = list_parquet_files(root / ds)
        counts[ds] = pl.concat([pl.scan_parquet(f).select(pl.len()) for f in files]).collect()["len"].sum() if files else 0
    return counts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-scr", action="store_true")
    args = parser.parse_args()
    settings = load_settings()
    fontes = [s for s in BRONZE_SOURCES if not (args.force_scr and s == "scr")]

    def rodada():
        run_bronze(settings, fontes)
        if args.force_scr:
            ingest_scr(settings, force=True)

    rodada()
    c1 = bronze_counts(settings)
    n_log = LoadLog(settings.layer_dir("_control")).read().height
    rodada()
    c2 = bronze_counts(settings)
    log2 = LoadLog(settings.layer_dir("_control")).read().slice(n_log)
    gravadas2 = log2.group_by("dataset").agg(pl.col("rows_written").sum()).to_dict(as_series=False)
    gravadas2 = dict(zip(gravadas2["dataset"], gravadas2["rows_written"]))

    print("\n=== DEMONSTRAÇÃO DE IDEMPOTÊNCIA (Bronze) ===")
    print(f"{'dataset':<28}{'após 1ª':>12}{'após 2ª':>12}{'gravadas na 2ª':>16}  ok")
    ok_geral = True
    for ds in sorted(c2):
        ok = c1.get(ds) == c2[ds] and gravadas2.get(ds, 0) == 0
        ok_geral &= ok
        print(f"{ds:<28}{c1.get(ds, 0):>12,}{c2[ds]:>12,}{gravadas2.get(ds, 0):>16,}  {'SIM' if ok else 'NAO'}")
    print(f"\nResultado: {'IDEMPOTENTE' if ok_geral else 'FALHOU'}")
    if not ok_geral:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
