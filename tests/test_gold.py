from datetime import date

import polars as pl
import pytest

from credito_pa.gold.base import finalize_gold, month_grid, parse_month
from credito_pa.gold.contracts import CONTEXTO_SCR_PA_MES
from credito_pa.gold.export_warehouse import export_tables
from credito_pa.gold.fato_mensal import ano_ibge_referencia
from credito_pa.quality.checks import ContractError


@pytest.mark.parametrize("competencia,ano_pib", [
    (date(2023, 6, 30), 2020),
    (date(2023, 12, 31), 2021),
    (date(2024, 11, 30), 2021),
    (date(2024, 12, 31), 2022),
])
def test_pib_conhecido_na_data_respeita_defasagem(competencia, ano_pib):
    df = pl.DataFrame({"d": [competencia]})
    assert df.select(ano_ibge_referencia(pl.col("d"), 2, 12))["d"].item() == ano_pib


def test_grade_mensal():
    g = month_grid("2019-11", "2020-02")
    assert g.to_list() == [date(2019, 11, 30), date(2019, 12, 31), date(2020, 1, 31), date(2020, 2, 29)]
    assert parse_month("2024-12") == date(2024, 12, 31)


def _contexto(n=3, inad=0.04):
    return pl.DataFrame({
        "data_competencia": month_grid("2020-01", f"2020-{n:02d}"),
        "carteira_ativa_pa": [1e9] * n, "inadimplencia_pa": [inad] * n, "ativo_problematico_pa": [0.07] * n,
        "inadimplencia_pf_pa": [0.05] * n, "inadimplencia_pj_pa": [0.03] * n, "inadimplencia_rural_pa": [0.02] * n,
    })


def test_gold_nao_limpa_falha_em_violacao(settings):
    with pytest.raises(ContractError):
        finalize_gold(settings, _contexto(inad=1.7), CONTEXTO_SCR_PA_MES)


def test_gold_pk_verificada_e_exportacao_idempotente(settings):
    r = finalize_gold(settings, _contexto(), CONTEXTO_SCR_PA_MES)
    assert r["duplicatas_na_pk"] == 0 and r["pk_unica"]
    c1 = export_tables(settings, ["contexto_scr_pa_mes"])
    c2 = export_tables(settings, ["contexto_scr_pa_mes"])
    assert c1 == c2 == {"contexto_scr_pa_mes": 3}


def test_gold_rejeita_pk_duplicada(settings):
    dup = pl.concat([_contexto(1), _contexto(1)])
    with pytest.raises(ContractError):
        finalize_gold(settings, dup, CONTEXTO_SCR_PA_MES)
