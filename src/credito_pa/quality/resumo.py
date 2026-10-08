from __future__ import annotations

import html
from datetime import date
from pathlib import Path

import polars as pl
from sqlalchemy import func, inspect, select, table

from credito_pa.bronze.contract import METADATA_COLUMNS
from credito_pa.common.control import utc_now
from credito_pa.common.db import describe_url, warehouse_engine
from credito_pa.common.io import list_parquet_files, write_json_atomic, write_text_atomic
from credito_pa.common.logging import get_logger
from credito_pa.config import Settings
from credito_pa.gold.contracts import GOLD_CONTRACTS
from credito_pa.quality.checks import ContractError
from credito_pa.quality.validacao import (
    ALERTOU, FALHOU, PASSOU, WARNING, ResultadoRegra, ResultadoTabela, avaliar, perfilar, validar_contrato,
)
from credito_pa.silver.contracts import SILVER_CONTRACTS

log = get_logger("resumo")

DIAS_MAX_INGESTAO_PADRAO = 30
MARCA = {PASSOU: "[ok]", ALERTOU: "[alerta]", FALHOU: "[FALHA]"}


def _linhas(path: Path) -> int:
    files = list_parquet_files(path) if path.is_dir() else [path] if path.exists() else []
    return sum(pl.scan_parquet(f).select(pl.len()).collect().item() for f in files)


def resumo_bronze(settings: Settings, hoje: date | None = None) -> list[ResultadoTabela]:
    root, quarentena = settings.layer_dir("bronze"), settings.layer_dir("quarantine") / "bronze"
    dias_max = settings.section("qualidade").get("dias_max_ingestao", DIAS_MAX_INGESTAO_PADRAO)
    hoje = hoje or utc_now().date()
    tabelas = []
    for ds in sorted(p for p in root.iterdir() if p.is_dir()) if root.exists() else []:
        files = list_parquet_files(ds)
        presentes = [c for c in METADATA_COLUMNS if all(c in pl.scan_parquet(f).collect_schema().names() for f in files)]
        meta = pl.concat([pl.scan_parquet(f).select(presentes) for f in files]).collect() if files else pl.DataFrame()
        regras = [(f"completude::{c}", "completude", f"metadado técnico {c} preenchido em todo registro", pl.col(c).is_null())
                  for c in presentes]
        if "record_hash" in presentes:
            regras.append(("unicidade::record_hash", "unicidade", "nenhum registro repetido (idempotência da carga)",
                           pl.col("record_hash").is_duplicated()))
        resultados, reprovadas = avaliar(meta, regras)
        ausentes = [c for c in METADATA_COLUMNS if c not in presentes]
        resultados.insert(0, ResultadoRegra("consistencia::metadados_tecnicos", "consistencia", len(METADATA_COLUMNS),
                                            len(ausentes), f"colunas {METADATA_COLUMNS} presentes; ausentes: {ausentes}"))
        particoes = sorted(p.name.split("=", 1)[1] for p in ds.iterdir() if p.is_dir() and "=" in p.name)
        if particoes:
            idade = (hoje - date.fromisoformat(particoes[-1])).days
            resultados.append(ResultadoRegra(f"atualidade::ingestao_ate_{dias_max}d", "atualidade", 1, int(idade > dias_max),
                                             f"última ingestão em {particoes[-1]} ({idade} dia(s) atrás)", WARNING))
        tabelas.append(ResultadoTabela("bronze", ds.name, meta.height, _linhas(quarentena / ds.name), reprovadas, resultados,
                                       extras={"particoes": len(particoes), "ultima_ingestao": particoes[-1] if particoes else None}))
    return tabelas


def resumo_silver(settings: Settings) -> list[ResultadoTabela]:
    root, quarentena = settings.layer_dir("silver"), settings.layer_dir("quarantine") / "silver"
    dim = root / "dim_municipio.parquet"
    chaves_dim = pl.read_parquet(dim, columns=["cod_ibge_municipio"])["cod_ibge_municipio"].to_list() if dim.exists() else None
    tabelas = []
    for contract in SILVER_CONTRACTS:
        path = root / f"{contract.name}.parquet"
        if not path.exists():
            continue
        df = pl.read_parquet(path)
        resultados, reprovadas = validar_contrato(df, contract, chaves_dim)
        tabelas.append(ResultadoTabela("silver", contract.name, df.height, _linhas(quarentena / path.name), reprovadas,
                                       resultados, perfilar(df), {"granularidade": contract.granularity}))
    return tabelas


def resumo_gold(settings: Settings) -> list[ResultadoTabela]:
    root = settings.layer_dir("gold")
    presentes = [c for c in GOLD_CONTRACTS if (root / f"{c.name}.parquet").exists()]
    if not presentes:
        return []
    dim = settings.layer_dir("silver") / "dim_municipio.parquet"
    chaves_dim = pl.read_parquet(dim, columns=["cod_ibge_municipio"])["cod_ibge_municipio"].to_list() if dim.exists() else None
    engine = warehouse_engine(settings)
    no_banco = set(inspect(engine).get_table_names())
    tabelas = []
    with engine.connect() as conn:
        for contract in presentes:
            df = pl.read_parquet(root / f"{contract.name}.parquet")
            resultados, reprovadas = validar_contrato(df, contract, chaves_dim)
            n_banco = (conn.execute(select(func.count()).select_from(table(contract.name))).scalar()
                       if contract.name in no_banco else None)
            diferenca = df.height if n_banco is None else abs(df.height - n_banco)
            resultados.append(ResultadoRegra(
                "consistencia::linhas_no_warehouse", "consistencia", max(df.height, 1), diferenca,
                f"parquet com {df.height:,} linha(s); warehouse com {'tabela ausente' if n_banco is None else f'{n_banco:,}'}"))
            tabelas.append(ResultadoTabela("gold", contract.name, df.height, 0, reprovadas, resultados, perfilar(df),
                                           {"granularidade": contract.granularity, "linhas_warehouse": n_banco}))
    return tabelas


def decidir(tabelas: list[ResultadoTabela]) -> tuple[int, str]:
    if any(t.status == FALHOU for t in tabelas):
        return 1, "pipeline INTERROMPIDO: regra ERROR acima do limite"
    if any(t.status == ALERTOU for t in tabelas):
        return 0, "pipeline SEGUE com alertas: revisar as regras WARNING"
    return 0, "pipeline LIBERADO: todas as regras dentro do limite"


def _bloco(titulo: str, tabelas: list[ResultadoTabela], extra: tuple[str, str] | None = None) -> str:
    if not tabelas:
        return f"{titulo}\n  (vazia: a etapa ainda não rodou)"
    cab = ["tabela", "linhas", *([extra[1]] if extra else []), "quarentena", "regras", "erros", "score", "status"]
    corpo = []
    for t in tabelas:
        valor_extra = t.extras.get(extra[0]) if extra else None
        corpo.append([t.tabela, f"{t.linhas:,}", *(["-" if valor_extra is None else f"{valor_extra:,}"] if extra else []),
                      f"{t.quarentena:,}", str(len(t.resultados)), f"{sum(r.erros for r in t.resultados):,}",
                      f"{t.score:.2f}%", t.status])
    larguras = [max(len(c), *(len(linha[i]) for linha in corpo)) for i, c in enumerate(cab)]
    fmt = lambda linha: "  " + "  ".join(c.ljust(w) if i == 0 else c.rjust(w) for i, (c, w) in enumerate(zip(linha, larguras)))
    out = [titulo, fmt(cab), *(fmt(linha) for linha in corpo)]
    for t in tabelas:
        for r in t.resultados:
            if r.status != PASSOU:
                out.append(f"    {MARCA[r.status]} {t.tabela} | {r.regra} | {r.severidade} | {r.erros:,} erro(s) "
                           f"({r.erro_ratio * 100:.2f}%, limite {r.threshold * 100:.2f}%) | {r.descricao}")
    return "\n".join(out)


def render_console(resumo: dict[str, list[ResultadoTabela]], banco: str, mensagem: str) -> str:
    blocos = [
        _bloco("BRONZE (dado bruto, por dataset)", resumo["bronze"], ("particoes", "partições")),
        _bloco("SILVER (tipada, sob contrato)", resumo["silver"]),
        _bloco(f"GOLD (orientada à decisão) -> warehouse {banco}", resumo["gold"], ("linhas_warehouse", "no warehouse")),
    ]
    totais = " | ".join(f"{camada}={sum(t.linhas for t in tabelas):,}" for camada, tabelas in resumo.items())
    return "Linhas e qualidade por camada\n\n" + "\n\n".join(blocos) + f"\n\nTotal de linhas: {totais}\nVeredito: {mensagem}"


_CSS = """
:root{--fundo:#fcfcfb;--tinta:#0b0b0b;--tinta2:#52514e;--borda:#e1e0d9;--faixa:#f0efec}
@media (prefers-color-scheme:dark){:root{--fundo:#1a1a19;--tinta:#fff;--tinta2:#c3c2b7;--borda:#383835;--faixa:#2c2c2a}}
body{font-family:system-ui,-apple-system,"Segoe UI",sans-serif;margin:2rem auto;max-width:1100px;padding:0 16px;
background:var(--fundo);color:var(--tinta);line-height:1.5}
h1{font-size:1.6rem;margin-bottom:.2rem}h2{font-size:1.2rem;margin-top:2.2rem}h3{font-size:1rem;margin:0}
.sub{color:var(--tinta2);margin-top:0}
.rolagem{overflow-x:auto}
table{border-collapse:collapse;width:100%;margin:.6rem 0;font-size:.86rem}
th,td{border:1px solid var(--borda);padding:.4rem .55rem;text-align:left}
th{background:var(--faixa)}td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}
.pill{display:inline-block;white-space:nowrap;padding:.1rem .55rem;border-radius:99px;font-size:.78rem;font-weight:600;border:1px solid}
.PASSOU{color:#006300;border-color:#0ca30c}.ALERTOU{color:#8a5a06;border-color:#fab219}.FALHOU{color:#d03b3b;border-color:#d03b3b}
@media (prefers-color-scheme:dark){.PASSOU{color:#0ca30c}.ALERTOU{color:#fab219}.FALHOU{color:#ec835a}}
.cartao{border:1px solid var(--borda);border-radius:8px;padding:.9rem 1.1rem;margin:.8rem 0}
.score{font-size:1.9rem;font-weight:700;margin:.1rem 0}
details{margin:.4rem 0}summary{cursor:pointer;color:var(--tinta2)}
code{background:var(--faixa);padding:.1rem .3rem;border-radius:3px}
"""

_SIMBOLO = {PASSOU: "✓", ALERTOU: "!", FALHOU: "✗"}


def _pill(status: str) -> str:
    return f"<span class='pill {status}'>{_SIMBOLO[status]} {status}</span>"


def render_html(resumo: dict[str, list[ResultadoTabela]], banco: str, mensagem: str, momento: str) -> str:
    e = html.escape
    partes = ["<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'>",
              "<meta name='viewport' content='width=device-width, initial-scale=1'>",
              f"<title>Qualidade por camada</title><style>{_CSS}</style></head><body>",
              "<h1>Relatório de qualidade por camada</h1>",
              f"<p class='sub'>Gerado em {e(momento)} por <code>python scripts/run_pipeline.py --stage resumo</code>. "
              f"Warehouse: <code>{e(banco)}</code>.</p>",
              f"<p><b>Veredito:</b> {e(mensagem)}</p>",
              "<h2>Fluxo de linhas</h2><div class='rolagem'><table><tr><th>Camada</th><th>Tabela</th><th class='n'>Linhas</th>"
              "<th class='n'>Quarentena</th><th class='n'>Score</th><th>Status</th></tr>"]
    for camada, tabelas in resumo.items():
        for t in tabelas:
            partes.append(f"<tr><td>{camada}</td><td><code>{e(t.tabela)}</code></td><td class='n'>{t.linhas:,}</td>"
                          f"<td class='n'>{t.quarentena:,}</td><td class='n'>{t.score:.2f}%</td><td>{_pill(t.status)}</td></tr>")
    partes.append("</table></div>")
    for camada, tabelas in resumo.items():
        partes.append(f"<h2>{camada.capitalize()}</h2>")
        if not tabelas:
            partes.append("<p class='sub'>Vazia: a etapa ainda não rodou.</p>")
        for t in tabelas:
            partes.append(f"<div class='cartao'><h3><code>{e(t.tabela)}</code> {_pill(t.status)}</h3>"
                          f"<p class='score'>{t.score:.2f}%</p><p class='sub'>{t.linhas_reprovadas:,} de {t.linhas:,} linha(s) "
                          f"reprovada(s) em pelo menos uma regra. {e(t.extras.get('granularidade') or '')}</p>"
                          "<div class='rolagem'><table><tr><th>Regra</th><th>Dimensão</th><th>Severidade</th><th class='n'>Erros</th>"
                          "<th class='n'>Taxa</th><th class='n'>Limite</th><th>Status</th><th>Descrição</th></tr>")
            for r in sorted(t.resultados, key=lambda r: (-r.erros, r.regra)):
                partes.append(f"<tr><td><code>{e(r.regra)}</code></td><td>{r.dimensao}</td><td>{r.severidade}</td>"
                              f"<td class='n'>{r.erros:,}</td><td class='n'>{r.erro_ratio * 100:.2f}%</td>"
                              f"<td class='n'>{r.threshold * 100:.2f}%</td><td>{_pill(r.status)}</td><td>{e(r.descricao)}</td></tr>")
            partes.append("</table></div>")
            if t.perfil:
                partes.append(f"<details><summary>Perfil das {len(t.perfil)} colunas</summary><div class='rolagem'><table><tr>"
                              "<th>Coluna</th><th>Tipo</th><th class='n'>Nulos</th><th class='n'>Distintos</th><th>Faixa</th></tr>")
                for c in t.perfil:
                    faixa = f"{c['minimo']} .. {c['maximo']}" if c["minimo"] is not None else "-"
                    chave = " (chave candidata)" if c["chave_candidata"] else ""
                    partes.append(f"<tr><td><code>{e(c['coluna'])}</code>{chave}</td><td>{e(c['tipo'])}</td>"
                                  f"<td class='n'>{c['nulos']:,} ({c['nulo_ratio'] * 100:.1f}%)</td>"
                                  f"<td class='n'>{c['distintos']:,}</td><td>{e(faixa)}</td></tr>")
                partes.append("</table></div></details>")
            partes.append("</div>")
    partes.append("</body></html>")
    return "\n".join(partes)


def log_resumo(settings: Settings) -> dict:
    resumo = {"bronze": resumo_bronze(settings), "silver": resumo_silver(settings), "gold": resumo_gold(settings)}
    todas = [t for tabelas in resumo.values() for t in tabelas]
    codigo, mensagem = decidir(todas)
    banco = describe_url(warehouse_engine(settings))
    momento = utc_now().isoformat(timespec="seconds")
    saida = {"gerado_em": momento, "codigo_saida": codigo, "veredito": mensagem,
             "total_linhas": {camada: sum(t.linhas for t in tabelas) for camada, tabelas in resumo.items()},
             "camadas": {camada: [t.as_dict() for t in tabelas] for camada, tabelas in resumo.items()}}
    write_json_atomic(saida, settings.reports_dir / "qualidade.json")
    write_text_atomic(render_html(resumo, banco, mensagem, momento), settings.reports_dir / "qualidade.html")
    log.info("%s\nRelatórios: %s e qualidade.html", render_console(resumo, banco, mensagem), settings.reports_dir / "qualidade.json")
    if codigo:
        raise ContractError(f"{mensagem} (detalhes em {settings.reports_dir / 'qualidade.html'})")
    return saida
