import polars as pl
import pytest
import requests

from credito_pa.common.control import LoadLog, StateStore, new_load_id
from credito_pa.common.hashing import hash_rows, record_hash
from credito_pa.common.http import HttpClient, HttpRequestError
from credito_pa.common.io import write_parquet_atomic


def test_hash_estavel_e_independente_da_ordem_das_chaves():
    a = {"municipio": "Belém", "valor": "10,5"}
    b = {"valor": "10,5", "municipio": "Belém"}
    assert record_hash(a) == record_hash(b)
    assert len(record_hash(a)) == 64


def test_hash_muda_com_o_valor():
    assert record_hash({"v": "1"}) != record_hash({"v": "2"})


def test_duplicatas_reais_no_lote_recebem_hash_distinto_e_deterministico():
    rows = [{"v": "1"}, {"v": "1"}, {"v": "2"}]
    h1 = hash_rows(rows)
    assert len(set(h1)) == 3
    assert h1[0] == record_hash({"v": "1"})
    assert hash_rows(rows) == h1


def test_escrita_atomica_sem_sobrescrever(tmp_path):
    path = tmp_path / "x.parquet"
    write_parquet_atomic(pl.DataFrame({"a": [1]}), path, overwrite=False)
    with pytest.raises(FileExistsError):
        write_parquet_atomic(pl.DataFrame({"a": [2]}), path, overwrite=False)
    assert pl.read_parquet(path)["a"].to_list() == [1]
    assert [p.name for p in tmp_path.iterdir()] == ["x.parquet"]


def test_state_store_ida_e_volta(tmp_path):
    st = StateStore(tmp_path)
    assert st.get("estban.last_competencia") is None
    st.set("estban.last_competencia", "2024-12")
    st.set("outro", 1)
    assert StateStore(tmp_path).get("estban.last_competencia") == "2024-12"


def test_load_log_acumula(tmp_path):
    ll = LoadLog(tmp_path)
    ll.append({"load_id": "a", "rows_read": 3, "status": "success"})
    ll.append({"load_id": "b", "rows_read": 0, "status": "success"})
    df = ll.read()
    assert df.height == 2
    assert df["rows_read"].to_list() == [3, 0]


def test_load_id_unico():
    assert new_load_id() != new_load_id()


def test_retry_503_depois_200(fake_session, fake_response):
    sess = fake_session([fake_response(503, text="indisponivel"), fake_response(200, json_data=[1, 2])])
    waits = []
    client = HttpClient(session=sess, max_retries=3, backoff_base_s=1.0, sleep=waits.append)
    assert client.get_json("http://x") == [1, 2]
    assert client.last_attempts == 2
    assert len(waits) == 1 and waits[0] >= 1.0


def test_backoff_exponencial():
    client = HttpClient(backoff_base_s=1.0, backoff_max_s=100, session=object())
    d1, d2, d3 = (client.backoff(i) for i in (1, 2, 3))
    assert 1.0 <= d1 < 1.2 and 2.0 <= d2 < 2.3 and 4.0 <= d3 < 4.5


def test_timeout_repetido_ate_esgotar(fake_session):
    sess = fake_session([requests.Timeout("t")] * 4)
    client = HttpClient(session=sess, max_retries=3, sleep=lambda s: None)
    with pytest.raises(HttpRequestError) as err:
        client.get("http://x")
    assert err.value.reason == "http_error"
    assert len(sess.calls) == 4


def test_404_nao_e_repetido(fake_session, fake_response):
    sess = fake_session([fake_response(404, text="nao existe")])
    client = HttpClient(session=sess, max_retries=5, sleep=lambda s: None)
    with pytest.raises(HttpRequestError) as err:
        client.get("http://x")
    assert err.value.reason == "not_found" and err.value.status == 404
    assert len(sess.calls) == 1


def test_html_no_lugar_de_json_vira_invalid_json(fake_session, fake_response):
    sess = fake_session([fake_response(200, text="<html>manutencao</html>")])
    client = HttpClient(session=sess, sleep=lambda s: None)
    with pytest.raises(HttpRequestError) as err:
        client.get_json("http://x")
    assert err.value.reason == "invalid_json"
