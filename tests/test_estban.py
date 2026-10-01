import io
import zipfile

import polars as pl

from credito_pa.bronze.estban_file import (
    CHECKPOINT_KEY,
    ingest_estban,
    month_range,
    parse_estban_csv,
    pending_competencias,
)
from credito_pa.common.control import StateStore
from credito_pa.common.http import HttpClient
from credito_pa.common.io import read_parquet_dir


def estban_csv(comp="202411", verbete110="VERBETE_110_ENCAIXE", extra_bad=True) -> bytes:
    linhas = [
        "ESTBAN (Documento 4500) por municipio ",
        "Data de geracao dos dados: 2025-08-01",
        f"#DATA_BASE;UF;CODMUN;MUNICIPIO;CNPJ;{verbete110};VERBETE_160_OPERACOES_DE_CREDITO;CODMUN_IBGE",
        f"{comp};PA;0427;BELÉM;00000000;10;1000;1501402",
        f"{comp};PA;0300;MARABÁ;00360305;5;200;1504208",
        f"{comp};SP;7107;SÃO PAULO;00000000;99;9999;3550308",
    ]
    if extra_bad:
        linhas.append(f"{comp};PA;0001;QUEBRADA;1")
    return "\n".join(linhas).encode("latin-1")


def zip_of(name, content: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(name, content)
    return buf.getvalue()


def test_month_range_e_pendentes():
    assert month_range("2019-11", "2020-02") == ["2019-11", "2019-12", "2020-01", "2020-02"]
    assert pending_competencias("2024-10", "2024-12", "2024-11") == ["2024-12"]


def test_parse_latin1_preambulo_filtro_uf_e_linha_quebrada():
    header, rows, bad, total = parse_estban_csv(estban_csv(), "PA")
    assert header[0] == "#DATA_BASE" and header[-1] == "CODMUN_IBGE"
    assert [r[3] for r in rows] == ["BELÉM", "MARABÁ"]
    assert total == 4 and len(bad) == 1
    assert bad[0]["quarantine_reason"].startswith("field_count_mismatch")


def _client(fake_session, fake_response, publicados: dict):
    by_url = {name: fake_response(200, content=content) for name, content in publicados.items()}
    return HttpClient(session=fake_session(by_url=by_url), sleep=lambda s: None, max_retries=0)


def test_incremental_com_checkpoint_e_404_encerra(settings, fake_session, fake_response):
    publicados = {
        "202411_ESTBAN.ZIP": zip_of("202411_ESTBAN.CSV", estban_csv("202411")),
        "202412_ESTBAN.csv.zip": zip_of("202412_ESTBAN.CSV", estban_csv("202412", "VERBETE_110_DISPONIBILIDADES")),
    }
    r1 = ingest_estban(settings, _client(fake_session, fake_response, publicados), inicio="2024-11", fim="2025-02")
    assert r1["processadas"] == ["2024-11", "2024-12"]
    assert r1["checkpoint_depois"] == "2024-12"
    df = read_parquet_dir(settings.layer_dir("bronze") / "estban_municipio")
    assert df.height == 4
    assert {"VERBETE_110_ENCAIXE", "VERBETE_110_DISPONIBILIDADES"} <= set(df.columns)
    q = read_parquet_dir(settings.layer_dir("quarantine") / "bronze" / "estban_municipio")
    assert q.height == 2

    publicados["202501_ESTBAN.csv.zip"] = zip_of("202501_ESTBAN.CSV", estban_csv("202501", extra_bad=False))
    r2 = ingest_estban(settings, _client(fake_session, fake_response, publicados), inicio="2024-11", fim="2025-02")
    assert r2["processadas"] == ["2025-01"] and r2["checkpoint_depois"] == "2025-01"
    assert read_parquet_dir(settings.layer_dir("bronze") / "estban_municipio").height == 6


def test_zip_invalido_vai_para_quarentena_e_checkpoint_nao_pula(settings, fake_session, fake_response):
    publicados = {
        "202411_ESTBAN.ZIP": fake_response(200, text="<html>erro</html>").content,
        "202412_ESTBAN.ZIP": zip_of("202412_ESTBAN.CSV", estban_csv("202412")),
    }
    r = ingest_estban(settings, _client(fake_session, fake_response, publicados), inicio="2024-11", fim="2024-12")
    assert r["falhas"] == ["2024-11"] and r["processadas"] == ["2024-12"]
    assert StateStore(settings.layer_dir("_control")).get(CHECKPOINT_KEY) is None
    files = list((settings.layer_dir("quarantine") / "files" / "estban_municipio").rglob("*.reason.json"))
    assert len(files) == 1
    assert '"invalid_format"' in files[0].read_text(encoding="utf-8")


def test_competencia_publicada_como_csv_puro(settings, fake_session, fake_response):
    publicados = {"202301_ESTBAN.csv": estban_csv("202301", extra_bad=False)}
    r = ingest_estban(settings, _client(fake_session, fake_response, publicados), inicio="2023-01", fim="2023-01")
    assert r["processadas"] == ["2023-01"]
    df = read_parquet_dir(settings.layer_dir("bronze") / "estban_municipio")
    assert df["source_object"].unique().to_list() == ["202301_ESTBAN.csv/202301_ESTBAN.csv"]
    assert df.height == 2
