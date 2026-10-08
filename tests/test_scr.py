import io
import zipfile

import polars as pl

from credito_pa.bronze.scr_file import detect_encoding, ingest_scr, scan_uf
from credito_pa.common.control import LoadLog
from credito_pa.common.http import HttpClient
from credito_pa.common.io import read_parquet_dir

HEADER = "data_base;uf;cliente;modalidade;carteira_ativa;carteira_inadimplida_arrastada;ativo_problematico"


def scr_csv(mes="2020-01-31", encoding="utf-8-sig"):
    linhas = [
        HEADER,
        f'{mes};PA;PF;PF - Cartão de crédito;100,50;10,00;"-"',
        f"{mes};PA;PJ;PJ - Capital de giro;200,00;0,00;5,00",
        f"{mes};SP;PF;PF - Cartão de crédito;999,00;1,00;1,00",
        f"{mes};AM;PF;PF - Cartão de crédito;5,00;0,00;0,00",
    ]
    return ("\n".join(linhas) + "\n").encode(encoding)


def scr_zip(meses, encoding="utf-8-sig"):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for m in meses:
            zf.writestr(f"planilha_{m[:4]}{m[5:7]}.csv", scr_csv(m, encoding))
    return buf.getvalue()


def _client(fake_session, fake_response, content):
    return HttpClient(session=fake_session(by_url={"planilha_2020.zip": fake_response(200, content=content)}),
                      sleep=lambda s: None, max_retries=0)


def test_detect_encoding():
    assert detect_encoding("ação".encode("utf-8")) == "utf8"
    assert detect_encoding("ação".encode("latin-1")) == "latin-1"
    assert detect_encoding("ação".encode("utf-8")[:-1]) == "utf8"


def test_scan_lazy_materializa_so_pa(tmp_path):
    p = tmp_path / "x.csv"
    p.write_bytes(scr_csv())
    df = scan_uf(p, "PA")
    assert df["uf"].unique().to_list() == ["PA"] and df.height == 2
    assert df["carteira_ativa"].to_list() == ["100,50", "200,00"]


def test_ingestao_csv_zip_filtra_pa_e_registra_contagens(settings, fake_session, fake_response):
    content = scr_zip(["2020-01-31", "2020-02-29"])
    r = ingest_scr(settings, _client(fake_session, fake_response, content), anos=[2020], mode="csv")
    assert len(r["objetos"]) == 2
    df = read_parquet_dir(settings.layer_dir("bronze") / "scr_data")
    assert df.height == 4 and set(df["uf"].to_list()) == {"PA"}
    assert "Cartão de crédito" in df["modalidade"].to_list()[0]
    log = LoadLog(settings.layer_dir("_control")).read()
    assert all("linhas_arquivo_todas_ufs=4" in m for m in log["message"].to_list())


def test_csv_latin1_e_transcodificado(settings, fake_session, fake_response):
    content = scr_zip(["2020-01-31"], encoding="latin-1")
    ingest_scr(settings, _client(fake_session, fake_response, content), anos=[2020], mode="csv")
    df = read_parquet_dir(settings.layer_dir("bronze") / "scr_data")
    assert "PF - Cartão de crédito" in df["modalidade"].to_list()


def test_reexecucao_pula_inalterado_e_force_nao_duplica(settings, fake_session, fake_response):
    content = scr_zip(["2020-01-31"])
    ingest_scr(settings, _client(fake_session, fake_response, content), anos=[2020], mode="csv")
    r2 = ingest_scr(settings, _client(fake_session, fake_response, content), anos=[2020], mode="csv")
    assert r2["objetos"] == []
    r3 = ingest_scr(settings, _client(fake_session, fake_response, content), anos=[2020], mode="csv", force=True)
    assert len(r3["objetos"]) == 1
    assert read_parquet_dir(settings.layer_dir("bronze") / "scr_data").height == 2
    log = LoadLog(settings.layer_dir("_control")).read()
    assert log["status"].to_list() == ["success", "skipped_unchanged", "success"]
    assert log["rows_written"].to_list() == [2, 0, 0]


def test_fallback_parquet_quando_download_falha(settings, fake_session, fake_response, monkeypatch):
    local = settings.data_dir / "scr_local.parquet"
    local.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"data_base": ["2020-01-31", "2020-01-31", "2019-12-31"], "uf": ["PA", "SP", "PA"],
                  "carteira_ativa": ["1,0", "2,0", "3,0"]}).write_parquet(local)
    monkeypatch.setitem(settings.env, "SCR_LOCAL_PARQUET", str(local))
    client = HttpClient(session=fake_session(by_url={}), sleep=lambda s: None, max_retries=0)
    r = ingest_scr(settings, client, anos=[2020], mode="auto")
    assert r["fallback_anos"] == [2020]
    df = read_parquet_dir(settings.layer_dir("bronze") / "scr_data")
    assert df.height == 1 and df["source_object"].item().endswith("#data_base=2020-01-31")
