from __future__ import annotations

from credito_pa.common.io import write_json_atomic, write_text_atomic
from credito_pa.config import Settings
from credito_pa.silver.contracts import (
    ESTBAN_MUNICIPIO_INSTITUICAO_MES,
    IBGE_PIB_ANO,
    IBGE_POPULACAO_ANO,
)
from credito_pa.silver.estban import build_estban
from credito_pa.silver.ibge import build_dim_municipio, build_pib, build_populacao
from credito_pa.silver.inpe import build_inpe
from credito_pa.silver.integrity import check_vs_dim, enforce_dim
from credito_pa.silver.scr import build_scr


def run_silver(settings: Settings) -> dict:
    dim, s_dim = build_dim_municipio(settings)
    pop, s_pop = build_populacao(settings)
    pop = enforce_dim(settings, pop, IBGE_POPULACAO_ANO, dim, s_pop)
    pib, s_pib, integ_pib_pop = build_pib(settings, pop)
    pib = enforce_dim(settings, pib, IBGE_PIB_ANO, dim, s_pib)
    estban, s_estban = build_estban(settings)
    estban = enforce_dim(settings, estban, ESTBAN_MUNICIPIO_INSTITUICAO_MES, dim, s_estban)
    scr, s_scr = build_scr(settings)
    inpe, s_inpe, integ_inpe = build_inpe(settings, dim)

    integridade = [
        check_vs_dim("estban_municipio_instituicao_mes", estban, dim, panel_col="data_competencia",
                     tratamento="Órfãos da fonte (código fora do cadastro) vão para a quarentena "
                                "('municipio_nao_encontrado'). Município-mês sem linha no ESTBAN NÃO é preenchido com "
                                "zero: fica ausente e a Gold o trata como sem dado (fora da coorte do t0 afetado)."),
        check_vs_dim("ibge_populacao_ano", pop, dim, panel_col="ano",
                     tratamento="Órfãos da fonte vão para a quarentena. Ano sem estimativa nem censo é interpolado "
                                "(marcado em fonte_populacao)."),
        check_vs_dim("ibge_pib_ano", pib, dim, panel_col="ano",
                     tratamento="Órfãos da fonte vão para a quarentena. Município sem PIB no ano fica sem PIB per capita."),
        integ_pib_pop,
        integ_inpe,
        {
            "join": "scr_pa_mes x dim_municipio",
            "lado_fonte_total": scr.height,
            "casados": None,
            "orfaos_fonte": None,
            "tratamento": "Não se aplica: o SCR.data é agregado por UF (sem município). Ele se junta à Gold só por "
                          f"data_competencia, como contexto regional. Competências cobertas: {scr['data_competencia'].n_unique()}.",
        },
    ]
    tabelas = [s.as_dict() for s in (s_dim, s_pop, s_pib, s_estban, s_scr, s_inpe)]
    report = {"tabelas": tabelas, "integridade_referencial": integridade}
    write_json_atomic(report, settings.reports_dir / "silver_relatorio.json")
    write_text_atomic(render_markdown(report), settings.reports_dir / "silver_relatorio.md")
    return report


def render_markdown(report: dict) -> str:
    lines = ["# Relatório da camada Silver", "",
             "Gerado automaticamente por `python scripts/run_pipeline.py --stage silver`.", "",
             "## Tabelas", "",
             "| Tabela | Granularidade | Linhas Bronze | Duplicatas removidas | Quarentena | Linhas Silver | PK única | PK sem nulos |",
             "|---|---|---:|---:|---:|---:|:---:|:---:|"]
    for t in report["tabelas"]:
        lines.append(f"| `{t['tabela']}` | {t['granularidade']} | {t['linhas_bronze']:,} | {t['duplicatas_removidas']:,} | "
                     f"{t['linhas_quarentena']:,} | {t['linhas_silver']:,} | {t['pk'].get('pk_unica')} | {t['pk'].get('pk_sem_nulos')} |")
    lines += ["", "### Motivos de quarentena e observações", ""]
    for t in report["tabelas"]:
        if t["quarentena"] or t["observacoes"]:
            lines.append(f"- **{t['tabela']}**")
            for motivo, n in sorted(t["quarentena"].items()):
                lines.append(f"  - quarentena `{motivo}`: {n:,}")
            for obs in t["observacoes"]:
                lines.append(f"  - {obs}")
    lines += ["", "## Integridade referencial", ""]
    for j in report["integridade_referencial"]:
        lines.append(f"### {j['join']}")
        lines.append("")
        for k, v in j.items():
            if k != "join":
                lines.append(f"- **{k}**: {v}")
        lines.append("")
    return "\n".join(lines)
