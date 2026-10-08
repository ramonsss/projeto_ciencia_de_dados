import json
from datetime import date

import polars as pl
import pytest

from credito_pa.common.io import write_parquet_atomic
from credito_pa.gold.base import month_grid
from credito_pa.gold.contracts import CONTEXTO_SCR_PA_MES
from credito_pa.gold.export_warehouse import export_tables
from credito_pa.pipeline import ORDER_ALL, run_stage
from credito_pa.quality.checks import ContractError
from credito_pa.quality.resumo import log_resumo, resumo_bronze
from credito_pa.quality.validacao import ALERTOU, FALHOU, PASSOU
from credito_pa.silver.contracts import DIM_MUNICIPIO


def _bronze(settings, n=3, hashes=None, data="2026-10-01"):
    df = pl.DataFrame({
        "valor": [str(i) for i in range(n)], "ingestion_timestamp": ["2026-10-01T00:00:00+00:00"] * n,
        "source_system": ["IBGE"] * n, "source_object": ["municipios"] * n, "load_id": ["x"] * n,
        "record_hash": hashes or [f"h{i}" for i in range(n)],
    })
    write_parquet_atomic(df, settings.layer_dir("bronze") / "ibge_municipios" / f"ingestion_date={data}" / "a.parquet")


def _dim(settings, codigos):
    linha = {c.name: "x" for c in DIM_MUNICIPIO.columns}
    df = pl.DataFrame([{**linha, "cod_ibge_municipio": c, "uf_sigla": "PA"} for c in codigos], schema=DIM_MUNICIPIO.schema)
    write_parquet_atomic(df, settings.layer_dir("silver") / "dim_municipio.parquet")


def _contexto(settings, n=3, inad=0.04, exportar=True):
    df = pl.DataFrame({
        "data_competencia": month_grid("2020-01", f"2020-{n:02d}"),
        "carteira_ativa_pa": [1e9] * n, "inadimplencia_pa": [inad] * n, "ativo_problematico_pa": [0.07] * n,
        "inadimplencia_pf_pa": [0.05] * n, "inadimplencia_pj_pa": [0.03] * n, "inadimplencia_rural_pa": [0.02] * n,
    }, schema=CONTEXTO_SCR_PA_MES.schema)
    write_parquet_atomic(df, settings.layer_dir("gold") / "contexto_scr_pa_mes.parquet")
    if exportar:
        export_tables(settings, ["contexto_scr_pa_mes"])


def _regra(tabela: dict, nome: str) -> dict:
    return next(r for r in tabela["resultados"] if r["regra"] == nome)


def test_resumo_conta_linhas_e_aprova_dado_limpo(settings):
    _bronze(settings, n=3)
    _dim(settings, ["1500107", "1500206"])
    write_parquet_atomic(pl.DataFrame({"x": [1]}), settings.layer_dir("quarantine") / "silver" / "dim_municipio.parquet")
    _contexto(settings, n=3)

    saida = log_resumo(settings)

    assert saida["codigo_saida"] == 0
    assert saida["total_linhas"] == {"bronze": 3, "silver": 2, "gold": 3}
    bronze, silver, gold = (saida["camadas"][c][0] for c in ("bronze", "silver", "gold"))
    assert (bronze["tabela"], bronze["linhas"], bronze["particoes"]) == ("ibge_municipios", 3, 1)
    assert (silver["linhas"], silver["quarentena"], silver["score"]) == (2, 1, 100.0)
    assert (gold["linhas"], gold["linhas_warehouse"], gold["status"]) == (3, 3, PASSOU)
    assert {r["dimensao"] for r in silver["resultados"]} >= {"completude", "unicidade", "validade", "consistencia"}
    assert json.loads((settings.reports_dir / "qualidade.json").read_text(encoding="utf-8"))["veredito"] == saida["veredito"]
    assert "ibge_municipios" in (settings.reports_dir / "qualidade.html").read_text(encoding="utf-8")


def test_resumo_falha_com_defeito_e_grava_relatorio(settings):
    _bronze(settings, n=3, hashes=["h0", "h0", "h2"])
    _dim(settings, ["1500107", "1500107", "9999999"])
    _contexto(settings, n=3, inad=1.7, exportar=False)

    with pytest.raises(ContractError, match="INTERROMPIDO"):
        log_resumo(settings)

    saida = json.loads((settings.reports_dir / "qualidade.json").read_text(encoding="utf-8"))
    bronze, silver, gold = (saida["camadas"][c][0] for c in ("bronze", "silver", "gold"))
    assert _regra(bronze, "unicidade::record_hash")["erros"] == 2
    assert _regra(silver, "unicidade::cod_ibge_municipio")["erros"] == 2
    assert _regra(silver, "validade::cod_ibge_municipio")["erros"] == 1
    assert (silver["linhas_reprovadas"], silver["score"], silver["status"]) == (3, 0.0, FALHOU)
    assert _regra(gold, "validade::inadimplencia_pa")["erros"] == 3
    assert _regra(gold, "consistencia::linhas_no_warehouse")["status"] == FALHOU


def test_ingestao_antiga_alerta_sem_bloquear(settings):
    _bronze(settings, data="2026-01-01")
    tabela = resumo_bronze(settings, hoje=date(2026, 10, 8))[0]
    atualidade = next(r for r in tabela.resultados if r.dimensao == "atualidade")
    assert (atualidade.status, tabela.status) == (ALERTOU, ALERTOU)
    assert resumo_bronze(settings, hoje=date(2026, 1, 15))[0].status == PASSOU


def test_resumo_sem_dados_nao_quebra_e_e_etapa_do_pipeline(settings):
    assert ORDER_ALL[-1] == "resumo"
    saida = run_stage(settings, "resumo")[0]
    assert saida["camadas"] == {"bronze": [], "silver": [], "gold": []}
    assert saida["codigo_saida"] == 0
