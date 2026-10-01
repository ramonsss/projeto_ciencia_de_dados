import polars as pl
import pytest

from credito_pa.quality.checks import ContractError, assert_primary_key, check_primary_key, split_valid, validate_schema
from credito_pa.quality.contracts import Col, TableContract

C = TableContract(
    name="t", layer="silver", granularity="uma linha por id", primary_key=["id"],
    columns=[
        Col("id", pl.Utf8, "id", "teste"),
        Col("valor", pl.Float64, "valor", "teste", domain=">= 0", check=pl.col("valor") >= 0),
        Col("obs", pl.Utf8, "obs", "teste", nullable=True),
    ],
)


def test_schema_valido_e_invalido():
    ok = pl.DataFrame({"id": ["a"], "valor": [1.0], "obs": [None]}, schema=C.schema)
    validate_schema(ok, C)
    with pytest.raises(ContractError):
        validate_schema(ok.with_columns(pl.col("valor").cast(pl.Int64)), C)
    with pytest.raises(ContractError):
        validate_schema(ok.drop("obs"), C)


def test_violacao_de_dominio_e_nulo_vao_para_quarentena():
    df = pl.DataFrame({"id": ["a", "b", None], "valor": [1.0, -5.0, 2.0], "obs": [None, None, None]}, schema=C.schema)
    validas, invalidas = split_valid(df, C)
    assert validas["id"].to_list() == ["a"]
    assert sorted(invalidas["quarantine_reason"].to_list()) == ["dominio_invalido:valor", "null_nao_permitido:id"]


def test_pk_duplicada_detectada():
    df = pl.DataFrame({"id": ["a", "a", "b"], "valor": [1.0, 1.0, 2.0], "obs": [None] * 3}, schema=C.schema)
    r = check_primary_key(df, ["id"])
    assert r["duplicatas_na_pk"] == 1 and not r["pk_unica"]
    with pytest.raises(ContractError):
        assert_primary_key(df, C)
