from pathlib import Path

import pytest

from credito_pa.config import DEFAULTS, PROJECT_ROOT, load_settings, parse_year_range


@pytest.fixture
def clean_env(monkeypatch):
    for key in DEFAULTS:
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


def test_defaults_sem_env(clean_env, tmp_path):
    s = load_settings(env_file=tmp_path / "inexistente.env")
    assert s.seed == 42
    assert s.uf_sigla == "PA"
    assert s.source_db_url.startswith("sqlite:///")
    assert s.source_db_url.endswith("data/source_db/fonte_operacional.db")
    assert s.data_dir == PROJECT_ROOT / "data"


def test_override_por_variavel_de_ambiente(clean_env, tmp_path):
    clean_env.setenv("SEED", "7")
    clean_env.setenv("SOURCE_DB_URL", "postgresql+psycopg2://u:p@host:5432/db")
    s = load_settings(env_file=tmp_path / "inexistente.env")
    assert s.seed == 7
    assert s.source_db_url == "postgresql+psycopg2://u:p@host:5432/db"


def test_arquivo_env_e_lido(clean_env, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("SCR_ANOS=2021-2022\n", encoding="utf-8")
    s = load_settings(env_file=env_file)
    assert s.years("SCR_ANOS") == [2021, 2022]


def test_pipeline_toml_carregado(clean_env, tmp_path):
    s = load_settings(env_file=tmp_path / "x.env")
    ml = s.section("ml")
    assert ml["janela_observacao_meses"] == 12
    assert ml["janela_predicao_meses"] == 6
    assert s.section("pib")["defasagem_anos"] == 2


def test_todas_variaveis_documentadas_no_env_example():
    texto = (PROJECT_ROOT / ".env.example").read_text(encoding="utf-8")
    faltando = [k for k in DEFAULTS if f"{k}=" not in texto]
    assert faltando == []


@pytest.mark.parametrize("valor,esperado", [("2018-2020", [2018, 2019, 2020]), ("2024", [2024])])
def test_parse_year_range(valor, esperado):
    assert parse_year_range(valor) == esperado


def test_parse_year_range_invertido():
    with pytest.raises(ValueError):
        parse_year_range("2022-2020")
