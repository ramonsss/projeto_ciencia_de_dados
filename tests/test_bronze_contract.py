from datetime import datetime, timezone

import polars as pl

from credito_pa.bronze.contract import METADATA_COLUMNS, BronzeWriter
from credito_pa.common.control import LoadLog
from credito_pa.common.hashing import file_sha256
from credito_pa.common.io import list_parquet_files, read_parquet_dir

DIA1 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
DIA2 = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)


def _payload():
    return pl.DataFrame({"municipio": ["Belém", "Marabá", "Belém"], "valor": ["1256,22", "10", "1256,22"]})


def _bronze(settings):
    return read_parquet_dir(settings.layer_dir("bronze") / "teste")


def test_metadados_completos_e_payload_como_texto(settings):
    w = BronzeWriter(settings, "teste", "FONTE_X", now=DIA1)
    w.write_batch(_payload(), "objeto_1")
    df = _bronze(settings)
    assert set(METADATA_COLUMNS) <= set(df.columns)
    assert df.select(pl.col(METADATA_COLUMNS).null_count()).sum_horizontal().item() == 0
    assert df["valor"].dtype == pl.Utf8 and "1256,22" in df["valor"].to_list()
    assert df.height == 3


def test_particionamento_por_data_de_ingestao(settings):
    BronzeWriter(settings, "teste", "FONTE_X", now=DIA1).write_batch(_payload(), "o")
    files = list_parquet_files(settings.layer_dir("bronze") / "teste")
    assert all("ingestion_date=2026-10-01" in f.as_posix() for f in files)


def test_execucao_dupla_nao_duplica(settings):
    r1 = BronzeWriter(settings, "teste", "FONTE_X", now=DIA1).write_batch(_payload(), "o")
    n1 = _bronze(settings).height
    r2 = BronzeWriter(settings, "teste", "FONTE_X", now=DIA1).write_batch(_payload(), "o")
    n2 = _bronze(settings).height
    assert (r1.rows_written, r2.rows_written, r2.rows_skipped_existing) == (3, 0, 3)
    assert n1 == n2 == 3
    log = LoadLog(settings.layer_dir("_control")).read()
    assert log["rows_written"].to_list() == [3, 0]
    assert log["load_id"].n_unique() == 2


def test_imutabilidade_de_particoes_anteriores(settings):
    BronzeWriter(settings, "teste", "FONTE_X", now=DIA1).write_batch(_payload(), "o")
    antes = {f: file_sha256(f) for f in list_parquet_files(settings.layer_dir("bronze") / "teste")}
    novo = pl.DataFrame({"municipio": ["Santarém"], "valor": ["5"]})
    BronzeWriter(settings, "teste", "FONTE_X", now=DIA2).write_batch(pl.concat([_payload(), novo]), "o")
    for f, h in antes.items():
        assert file_sha256(f) == h
    df = _bronze(settings)
    assert df.height == 4
    assert sorted(df["ingestion_timestamp"].unique().to_list())[-1].startswith("2026-10-02")


def test_quarentena_e_conta_fechada(settings):
    w = BronzeWriter(settings, "teste", "FONTE_X", now=DIA1)
    ruins = [{"raw_record": "Belém;1;EXTRA", "quarantine_reason": "field_count_mismatch"}]
    r = w.write_batch(_payload(), "arquivo.csv", quarantined=ruins)
    assert r.rows_read == r.rows_written + r.rows_quarantined + r.rows_skipped_existing == 4
    q = read_parquet_dir(settings.layer_dir("quarantine") / "bronze" / "teste")
    assert q.height == 1
    assert q["quarantine_reason"].item() == "field_count_mismatch"
    assert q["source_object"].item() == "arquivo.csv"


def test_quarentena_de_arquivo(settings, tmp_path):
    ruim = tmp_path / "202412_ESTBAN.ZIP"
    ruim.write_text("<html>404</html>")
    w = BronzeWriter(settings, "teste", "FONTE_X", now=DIA1)
    target = w.quarantine_file(ruim, "invalid_zip", "202412_ESTBAN.ZIP", "assinatura PK ausente")
    assert target.exists()
    assert target.with_name(target.name + ".reason.json").exists()
    log = LoadLog(settings.layer_dir("_control")).read()
    assert log["status"].to_list() == ["quarantined_file"]
