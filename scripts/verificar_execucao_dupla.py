import hashlib
import subprocess
import sys
from datetime import datetime, timezone

import polars as pl

from credito_pa.common.control import LoadLog
from credito_pa.common.io import list_parquet_files, write_text_atomic
from credito_pa.config import PROJECT_ROOT, load_settings
from credito_pa.pipeline import run_stage


def snapshot(settings) -> dict:
    out = {}
    root = settings.layer_dir("bronze")
    for ds in sorted(p.name for p in root.iterdir() if p.is_dir()):
        files = list_parquet_files(root / ds)
        out[f"bronze.{ds}"] = (int(pl.concat([pl.scan_parquet(f).select(pl.len()) for f in files]).collect()["len"].sum()), None)
    for layer in ("silver", "gold"):
        for f in sorted(settings.layer_dir(layer).glob("*.parquet")):
            df = pl.read_parquet(f)
            digest = hashlib.sha256(df.sort(df.columns).write_csv().encode("utf-8")).hexdigest()[:16]
            out[f"{layer}.{f.stem}"] = (df.height, digest)
    return out


def main() -> None:
    settings = load_settings()
    run_stage(settings, "all")
    s1 = snapshot(settings)
    n_log = LoadLog(settings.layer_dir("_control")).read().height
    run_stage(settings, "all")
    s2 = snapshot(settings)
    log2 = LoadLog(settings.layer_dir("_control")).read().slice(n_log)
    gravadas2 = dict(log2.group_by("dataset").agg(pl.col("rows_written").sum()).iter_rows())

    pytest = subprocess.run([sys.executable, "-m", "pytest"], cwd=PROJECT_ROOT, capture_output=True, text=True)
    resumo_pytest = (pytest.stdout.strip().splitlines() or ["(sem saída)"])[-1]

    linhas = [
        "# Verificação integrada (execução dupla)", "",
        f"Gerado em {datetime.now(timezone.utc).isoformat(timespec='seconds')} por `python scripts/verificar_execucao_dupla.py`.", "",
        "`run_pipeline --stage all` foi executado duas vezes seguidas (seed → bronze → silver → gold → ml → report → resumo).", "",
        "| Tabela | Linhas (1ª) | Linhas (2ª) | Gravadas na 2ª (Bronze) | Hash do conteúdo (1ª = 2ª) | OK |",
        "|---|---:|---:|---:|---|:---:|",
    ]
    ok_geral = True
    for k in sorted(set(s1) | set(s2)):
        (n1, h1), (n2, h2) = s1.get(k, (None, None)), s2.get(k, (None, None))
        ds = k.split(".", 1)[1]
        grav = gravadas2.get(ds, 0) if k.startswith("bronze.") else ""
        ok = n1 == n2 and h1 == h2 and (grav in ("", 0))
        ok_geral &= ok
        linhas.append(f"| `{k}` | {n1:,} | {n2:,} | {grav} | {h2 or 'n/a'} | {'SIM' if ok else 'NÃO'} |")
    linhas += ["", f"**pytest (offline):** `{resumo_pytest}` (código de saída {pytest.returncode})", "",
               f"**Resultado:** {'APROVADO' if ok_geral and pytest.returncode == 0 else 'REPROVADO'}"]
    write_text_atomic("\n".join(linhas) + "\n", settings.reports_dir / "verificacao.md")
    print("\n".join(linhas))
    if not (ok_geral and pytest.returncode == 0):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
