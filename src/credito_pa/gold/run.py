from __future__ import annotations

import polars as pl

from credito_pa.common.io import write_json_atomic, write_text_atomic
from credito_pa.config import Settings
from credito_pa.gold.base import finalize_gold
from credito_pa.gold.contracts import CONTEXTO_SCR_PA_MES, FATO_CREDITO_MUNICIPIO_MES, ML_BASE_RISCO_CREDITO
from credito_pa.gold.export_warehouse import export_tables
from credito_pa.gold.fato_mensal import build_contexto_scr, build_fato
from credito_pa.ml.dataset import build_ml_base


def validar_proxy(fato: pl.DataFrame, contexto: pl.DataFrame) -> dict:
    pa = (fato.filter(pl.col("possui_dado_estban")).group_by("data_competencia")
          .agg((pl.col("provisao_reais").sum() / pl.col("carteira_credito_reais").sum()).alias("razao_provisao_pa"))
          .join(contexto, on="data_competencia").sort("data_competencia"))
    d12 = pa.select([(pl.col(c) - pl.col(c).shift(12)).alias(c) for c in
                     ("razao_provisao_pa", "inadimplencia_pa", "ativo_problematico_pa")]).drop_nulls()

    def corr(df, a, b):
        return round(df.select(pl.corr(a, b)).item(), 3)

    return {
        "meses": pa.height,
        "corr_nivel_vs_inadimplencia_scr": corr(pa, "razao_provisao_pa", "inadimplencia_pa"),
        "corr_nivel_vs_ativo_problematico_scr": corr(pa, "razao_provisao_pa", "ativo_problematico_pa"),
        "corr_var12m_vs_inadimplencia_scr": corr(d12, "razao_provisao_pa", "inadimplencia_pa"),
        "corr_var12m_vs_ativo_problematico_scr": corr(d12, "razao_provisao_pa", "ativo_problematico_pa"),
        "razao_provisao_pa_inicio_fim": [round(pa["razao_provisao_pa"][0], 4), round(pa["razao_provisao_pa"][-1], 4)],
        "inadimplencia_scr_inicio_fim": [round(pa["inadimplencia_pa"][0], 4), round(pa["inadimplencia_pa"][-1], 4)],
    }


def run_gold(settings: Settings) -> dict:
    contexto = build_contexto_scr(settings)
    tabelas = [finalize_gold(settings, contexto, CONTEXTO_SCR_PA_MES)]
    fato, info = build_fato(settings, contexto)
    tabelas.append(finalize_gold(settings, fato, FATO_CREDITO_MUNICIPIO_MES))
    fato = pl.read_parquet(settings.layer_dir("gold") / "fato_credito_municipio_mes.parquet")
    ml_base, ml_resumo = build_ml_base(settings, fato)
    tabelas.append(finalize_gold(settings, ml_base, ML_BASE_RISCO_CREDITO))
    write_json_atomic(ml_resumo, settings.reports_dir / "ml_ready_resumo.json")
    report = {
        "tabelas": tabelas,
        "recorte_temporal": {**info, "competencia_fim_comparavel": settings.section("analise")["competencia_fim_comparavel"],
                             "motivo": "Res. CMN 4.966 (IFRS 9) altera a contabilização da provisão a partir de 2025-01."},
        "cobertura_fato": {
            "municipios": fato["cod_ibge_municipio"].n_unique(),
            "competencias": fato["data_competencia"].n_unique(),
            "linhas_com_dado_estban": fato.filter(pl.col("possui_dado_estban")).height,
            "linhas_sem_agencia": fato.filter(~pl.col("possui_dado_estban")).height,
        },
        "validacao_proxy": validar_proxy(fato, contexto),
    }
    report["warehouse"] = export_tables(settings, [t["tabela"] for t in tabelas])
    write_json_atomic(report, settings.reports_dir / "gold_relatorio.json")
    write_text_atomic(render_markdown(report), settings.reports_dir / "gold_relatorio.md")
    return report


def render_markdown(report: dict) -> str:
    lines = ["# Relatório da camada Gold", "",
             "Gerado automaticamente por `python scripts/run_pipeline.py --stage gold`.", "",
             "## Tabelas e verificação de chave primária", "",
             "| Tabela | Granularidade | Chave primária | Linhas | Duplicatas na PK | PK com nulo |",
             "|---|---|---|---:|---:|---:|"]
    for t in report["tabelas"]:
        lines.append(f"| `{t['tabela']}` | {t['granularidade']} | ({', '.join(t['pk'])}) | {t['linhas']:,} | "
                     f"{t['duplicatas_na_pk']} | {t['linhas_com_pk_nula']} |")
    rt, cob, vp = report["recorte_temporal"], report["cobertura_fato"], report["validacao_proxy"]
    lines += ["", "## Recorte temporal (quebra contábil)", "",
              f"- Última competência comparável: **{rt['competencia_fim_comparavel']}**. {rt['motivo']}",
              f"- Competências da Silver excluídas da Gold: {rt['competencias_excluidas_quebra_contabil']} "
              f"({rt['linhas_estban_excluidas_quebra_contabil']} linhas ESTBAN).",
              "", "## Cobertura do fato mensal", "",
              f"- {cob['municipios']} municípios × {cob['competencias']} competências; "
              f"{cob['linhas_com_dado_estban']:,} município-mês com agência e {cob['linhas_sem_agencia']:,} sem agência "
              "(medidas de crédito nulas, não zero).",
              "", "## Validação do proxy de risco (ESTBAN vs. SCR, PA agregado)", "",
              f"- Meses comparados: {vp['meses']}",
              f"- Correlação em nível, provisão/carteira (ESTBAN) × inadimplência (SCR): **{vp['corr_nivel_vs_inadimplencia_scr']}**",
              f"- Correlação em nível × ativo problemático (SCR): **{vp['corr_nivel_vs_ativo_problematico_scr']}**",
              f"- Correlação da variação em 12 meses × inadimplência: **{vp['corr_var12m_vs_inadimplencia_scr']}**",
              f"- Correlação da variação em 12 meses × ativo problemático: **{vp['corr_var12m_vs_ativo_problematico_scr']}**",
              f"- Razão provisão/carteira do PA (início → fim): {vp['razao_provisao_pa_inicio_fim']}; "
              f"inadimplência SCR-PA: {vp['inadimplencia_scr_inicio_fim']}",
              "", "## Exportação para o warehouse", "",
              *[f"- `{k}`: {v:,} linhas" for k, v in report["warehouse"].items()], ""]
    return "\n".join(lines)
