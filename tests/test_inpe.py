import io
import zipfile

from credito_pa.bronze.inpe_file import ingest_inpe, parse_inpe_csv
from credito_pa.common.http import HttpClient
from credito_pa.common.io import read_parquet_dir

CSV = (
    "id_bdq,foco_id,lat,lon,data_pas,pais,estado,municipio,bioma\n"
    "1,abc,-1.4,-48.5,2023-08-01 17:00:00,Brasil,PARÁ,BELÉM,Amazônia\n"
    "2,def,-5.3,-49.1,2023-08-02 17:00:00,Brasil,PARÁ,MARABÁ,Amazônia\n"
    "3,ghi,-5.3\n"
)


def _zip(content: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("focos_br_pa_ref_2023.csv", content.encode("utf-8"))
    return buf.getvalue()


def test_parse_inpe():
    header, rows, bad = parse_inpe_csv(CSV.encode("utf-8"))
    assert header[1] == "foco_id" and len(rows) == 2 and len(bad) == 1
    assert rows[0][7] == "BELÉM"


def test_ingestao_inpe_e_idempotencia(settings, fake_session, fake_response):
    def client():
        return HttpClient(session=fake_session(by_url={"focos_br_pa_ref_2023.zip": fake_response(200, content=_zip(CSV))}),
                          sleep=lambda s: None, max_retries=0)
    r1 = ingest_inpe(settings, client(), anos=[2023])
    ingest_inpe(settings, client(), anos=[2023])
    assert r1 == {2023: 3}
    df = read_parquet_dir(settings.layer_dir("bronze") / "inpe_focos")
    assert df.height == 2 and df["foco_id"].to_list() == ["abc", "def"]
    q = read_parquet_dir(settings.layer_dir("quarantine") / "bronze" / "inpe_focos")
    assert q.height == 2
