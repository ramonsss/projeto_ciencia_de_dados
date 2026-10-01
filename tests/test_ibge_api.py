from credito_pa.bronze.ibge_api import (
    SidraQuery,
    chunk,
    ingest_localidades,
    ingest_sidra,
    parse_sidra_rows,
)
from credito_pa.common.control import LoadLog
from credito_pa.common.http import HttpClient
from credito_pa.common.io import read_parquet_dir

HEADER = {"NC": "Nível", "NN": "x", "MC": "x", "MN": "x", "V": "Valor", "D1C": "x", "D1N": "x",
          "D2C": "x", "D2N": "x", "D3C": "x", "D3N": "x"}


def _row(mun, ano, valor):
    return {"NC": "6", "NN": "Município", "MC": "45", "MN": "Pessoas", "V": valor, "D1C": mun,
            "D1N": f"M{mun} - PA", "D2C": "9324", "D2N": "Pop", "D3C": str(ano), "D3N": str(ano)}


def _client(fake_session, by_url):
    return HttpClient(session=fake_session(by_url=by_url), sleep=lambda s: None)


def test_chunk():
    assert chunk([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]


def test_parse_descarta_cabecalho_e_quarentena_linha_invalida():
    validas, invalidas = parse_sidra_rows([HEADER, _row("1500107", 2020, "10"), {"V": "1"}])
    assert len(validas) == 1 and validas[0]["D1C"] == "1500107"
    assert invalidas[0]["quarantine_reason"] == "schema_mismatch"


def test_paginacao_por_lotes_de_anos(settings, fake_session, fake_response, monkeypatch):
    monkeypatch.setitem(settings.env, "IBGE_PAGE_SIZE_ANOS", "2")
    periodos = [{"id": str(a)} for a in (2018, 2019, 2020, 2021, 2024)]
    by_url = {
        "/periodos": fake_response(200, json_data=periodos),
        "/p/2018,2019": fake_response(200, json_data=[HEADER, _row("1", 2018, "5"), _row("1", 2019, "6")]),
        "/p/2020,2021": fake_response(200, json_data=[HEADER, _row("1", 2020, "7"), _row("1", 2021, "8")]),
        "/p/2024": fake_response(200, json_data=[HEADER, _row("1", 2024, "9")]),
    }
    client = _client(fake_session, by_url)
    ingest_sidra(settings, "pop", [SidraQuery("6579", "9324", "est")], list(range(2018, 2025)), client)
    df = read_parquet_dir(settings.layer_dir("bronze") / "pop")
    assert sorted(df["D3C"].to_list()) == ["2018", "2019", "2020", "2021", "2024"]
    assert df["source_object"].n_unique() == 3
    assert len(list((settings.layer_dir("landing") / "ibge" / "pop").rglob("*.json"))) == 3


def test_pagina_com_falha_vai_para_quarentena_e_job_continua(settings, fake_session, fake_response, monkeypatch):
    monkeypatch.setitem(settings.env, "IBGE_PAGE_SIZE_ANOS", "1")
    monkeypatch.setitem(settings.env, "HTTP_MAX_RETRIES", "1")
    by_url = {
        "/periodos": fake_response(200, json_data=[{"id": "2020"}, {"id": "2021"}]),
        "/p/2020": fake_response(200, text="<html>erro</html>"),
        "/p/2021": fake_response(200, json_data=[HEADER, _row("1", 2021, "8")]),
    }
    ingest_sidra(settings, "pop", [SidraQuery("6579", "9324", "est")], [2020, 2021], _client(fake_session, by_url))
    df = read_parquet_dir(settings.layer_dir("bronze") / "pop")
    assert df["D3C"].to_list() == ["2021"]
    q = read_parquet_dir(settings.layer_dir("quarantine") / "bronze" / "pop")
    assert q["quarantine_reason"].to_list() == ["invalid_json"]
    log = LoadLog(settings.layer_dir("_control")).read()
    assert "failed" in log["status"].to_list()


def test_execucao_dupla_api_nao_duplica(settings, fake_session, fake_response):
    def make():
        return _client(fake_session, {
            "/periodos": fake_response(200, json_data=[{"id": "2021"}]),
            "/p/2021": fake_response(200, json_data=[HEADER, _row("1", 2021, "8"), _row("2", 2021, "9")]),
        })
    ingest_sidra(settings, "pop", [SidraQuery("6579", "9324", "est")], [2021], make())
    ingest_sidra(settings, "pop", [SidraQuery("6579", "9324", "est")], [2021], make())
    assert read_parquet_dir(settings.layer_dir("bronze") / "pop").height == 2
    log = LoadLog(settings.layer_dir("_control")).read()
    assert log["rows_written"].to_list() == [2, 0]


def test_localidades_achata_json_aninhado(settings, fake_session, fake_response):
    mun = {"id": 1500107, "nome": "Abaetetuba",
           "microrregiao": {"id": 15011, "nome": "Cametá", "mesorregiao": {"id": 1504, "nome": "Nordeste Paraense",
                                                                          "UF": {"id": 15, "sigla": "PA"}}},
           "regiao-imediata": {"id": 150003, "nome": "Abaetetuba",
                               "regiao-intermediaria": {"id": 1501, "nome": "Belém"}}}
    quebrado = {"id": 1, "nome": "X"}
    client = _client(fake_session, {"municipios": fake_response(200, json_data=[mun, quebrado])})
    ingest_localidades(settings, client)
    df = read_parquet_dir(settings.layer_dir("bronze") / "ibge_municipios")
    assert df["id"].to_list() == ["1500107"]
    assert df["mesorregiao_nome"].item() == "Nordeste Paraense"
    q = read_parquet_dir(settings.layer_dir("quarantine") / "bronze" / "ibge_municipios")
    assert q.height == 1
