from datetime import date

import polars as pl

from credito_pa.bronze.contract import BronzeWriter
from credito_pa.silver.base import br_number, latest_by_key, normalize_name, parse_typed, read_silver
from credito_pa.silver.estban import build_estban, verbete_sources
from credito_pa.silver.ibge import interpolate_missing_years
from credito_pa.silver.scr import build_scr


def test_numero_formato_brasileiro():
    df = pl.DataFrame({"v": ["1.234,56", "1234,56", "-9478177", "-", "", None, "abc"]})
    out = df.select(br_number("v"))["v"].to_list()
    assert out[:3] == [1234.56, 1234.56, -9478177.0]
    assert out[3:] == [None, None, None, None]


def test_cast_error_e_detectado_mas_marcador_de_ausencia_nao():
    df = pl.DataFrame({"v": ["10,5", "-", "N/D"]})
    out, reason = parse_typed(df, {"valor": ("v", br_number("v"))})
    assert reason.to_list() == [None, None, "cast_error:valor"]
    assert out["valor"].to_list() == [10.5, None, None]


def test_dedup_mantem_versao_mais_recente():
    df = pl.DataFrame({"k": ["a", "a", "b"], "v": [1, 2, 3], "ts": ["2026-01-01", "2026-02-01", "2026-01-01"]})
    out, removed = latest_by_key(df, ["k"], ["ts"])
    assert removed == 1
    assert out.filter(pl.col("k") == "a")["v"].item() == 2


def test_interpolacao_de_2023():
    df = pl.DataFrame({
        "cod_ibge_municipio": ["1501402"] * 3,
        "ano": pl.Series([2021, 2022, 2024], dtype=pl.Int32),
        "populacao": [1_500_000, 1_300_000, 1_400_000],
        "fonte_populacao": ["estimativa", "censo", "estimativa"],
    })
    out = interpolate_missing_years(df).sort("ano")
    row = out.filter(pl.col("ano") == 2023)
    assert row["populacao"].item() == 1_350_000
    assert row["fonte_populacao"].item() == "interpolada"
    assert out.filter(pl.col("ano") == 2022)["fonte_populacao"].item() == "censo"


def test_normalizacao_de_nomes():
    assert normalize_name("Santa Bárbara do Pará") == "SANTA BARBARA DO PARA"
    assert normalize_name("  Pau D'Arco ") == "PAU D ARCO"
    assert normalize_name("ELDORADO DO CARAJÁS") == normalize_name("Eldorado do Carajás")


def test_verbete_harmonizado_por_codigo_entre_layouts():
    cols = ["#DATA_BASE", "VERBETE_110_ENCAIXE", "VERBETE_110_DISPONIBILIDADES", "VERBETE_160_OPERACOES_DE_CREDITO",
            "VERBETE_401_SERVICOS_PUBLICOS + VERBETE_402_ATIVIDADES_EMPRESARIAIS"]
    src = verbete_sources(cols)
    assert src["verbete_160"] == ["VERBETE_160_OPERACOES_DE_CREDITO"]
    assert src["verbete_401_419"] == ["VERBETE_401_SERVICOS_PUBLICOS + VERBETE_402_ATIVIDADES_EMPRESARIAIS"]


def _estban_bronze(settings):
    base = {"UF": "PA", "CODMUN": "0427", "MUNICIPIO": "BELEM", "NOME_INSTITUICAO": "BANCO X", "AGEN_ESPERADAS": "1"}
    antigo = pl.DataFrame([{**base, "#DATA_BASE": "202001", "CNPJ": "1", "AGEN_PROCESSADAS": "1", "CODMUN_IBGE": "1501402",
                            "VERBETE_160_OPERACOES_DE_CREDITO": "1000", "VERBETE_174_PROV_P/_OPER_CREDITOS": "-40",
                            "VERBETE_167_FINANCIAMENTOS_AGROINDUSTRIAIS+VERBETE_168_RENDAS": "7"}])
    novo = pl.DataFrame([
        {**base, "#DATA_BASE": "202412", "CNPJ": "00000001", "AGEN_PROCESSADAS": "1", "CODMUN_IBGE": "1501402",
         "VERBETE_160_OPERACOES_DE_CREDITO": "2000", "VERBETE_174_PROV_P/_OPER_CREDITOS": "-80",
         "VERBETE_167_FINANCIAMENTOS_AGROINDUSTRIAIS": "9"},
        {**base, "#DATA_BASE": "202206", "CNPJ": "04913711", "AGEN_PROCESSADAS": "0", "CODMUN_IBGE": "",
         "VERBETE_160_OPERACOES_DE_CREDITO": "", "VERBETE_174_PROV_P/_OPER_CREDITOS": "",
         "VERBETE_167_FINANCIAMENTOS_AGROINDUSTRIAIS": ""},
    ])
    w = BronzeWriter(settings, "estban_municipio", "BCB_ESTBAN")
    w.write_batch(antigo, "202001_ESTBAN.ZIP")
    w.write_batch(novo, "202412_ESTBAN.csv.zip")


def test_silver_estban_harmoniza_tipa_e_quarentena(settings):
    _estban_bronze(settings)
    df, stats = build_estban(settings)
    assert df.height == 2
    assert df["data_competencia"].to_list() == [date(2020, 1, 31), date(2024, 12, 31)]
    assert df["verbete_167"].to_list() == [7.0, 9.0]
    assert df["cnpj_raiz"].to_list() == ["00000001", "00000001"]
    assert stats.quarentena == {"agencia_nao_processada": 1}
    assert stats.pk["pk_unica"] and read_silver(settings, "estban_municipio_instituicao_mes").height == 2


def test_silver_scr_tipagem_e_sigilo(settings):
    bronze = pl.DataFrame({
        "data_base": ["2020-01-31", "2020-01-31"], "uf": ["PA", "PA"], "tcb": ["Bancário", "Bancário"], "sr": ["S1", None],
        "cliente": ["PF", "PJ"], "ocupacao": ["PF - Outros", "-"], "cnae_secao": ["-", "PJ - Construção"],
        "cnae_subclasse": ["-", "PJ - X"], "porte": ["PF - Até 1 salário mínimo   ", "PJ - Micro"],
        "modalidade": ["PF - Cartão de crédito", "PJ - Capital de giro"], "origem": ["Sem destinação específica"] * 2,
        "indexador": ["Prefixado"] * 2, "numero_de_operacoes": ["<= 15", "120"],
        **{c: ["10,50", "0,00"] for c in [
            "a_vencer_ate_90_dias", "a_vencer_de_91_ate_360_dias", "a_vencer_de_361_ate_1080_dias",
            "a_vencer_de_1081_ate_1800_dias", "a_vencer_de_1801_ate_5400_dias", "a_vencer_acima_de_5400_dias",
            "vencido_acima_de_15_dias", "carteira_ativa", "carteira_inadimplida_arrastada", "ativo_problematico"]},
    })
    BronzeWriter(settings, "scr_data", "BCB_SCR_DATA").write_batch(bronze, "planilha_2020.zip/planilha_202001.csv")
    df, stats = build_scr(settings)
    df = df.sort("cliente")
    assert df.height == 2
    assert df["operacoes_ate_15"].to_list() == [True, False]
    assert df["numero_de_operacoes"].to_list() == [None, 120]
    assert df["carteira_ativa"].to_list() == [10.5, 0.0]
    assert "NAO_INFORMADO" in df["sr"].to_list()
    assert df["porte"].to_list()[0] == "PF - Até 1 salário mínimo"
