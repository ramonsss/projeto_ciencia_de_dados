from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULTS: dict[str, str] = {
    "DATA_DIR": "data",
    "REPORTS_DIR": "reports",
    "SEED": "42",
    "UF_SIGLA": "PA",
    "UF_CODIGO_IBGE": "15",
    "SOURCE_DB_URL": "sqlite:///data/source_db/fonte_operacional.db",
    "WAREHOUSE_DB_URL": "sqlite:///data/warehouse/credito_pa.db",
    "HTTP_TIMEOUT_S": "120",
    "HTTP_MAX_RETRIES": "5",
    "HTTP_BACKOFF_BASE_S": "1.0",
    "HTTP_BACKOFF_MAX_S": "60",
    "IBGE_LOCALIDADES_URL": "https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf_codigo}/municipios",
    "IBGE_SIDRA_URL": "https://apisidra.ibge.gov.br/values",
    "IBGE_PAGE_SIZE_ANOS": "3",
    "IBGE_POP_ANOS": "2018-2025",
    "IBGE_PIB_ANOS": "2018-2023",
    "SCR_SOURCE_MODE": "auto",
    "SCR_BASE_URL": "https://www.bcb.gov.br/pda/desig/planilha_{ano}.zip",
    "SCR_ANOS": "2020-2024",
    "SCR_LOCAL_PARQUET": "data/scrdata.parquet",
    "ESTBAN_BASE_URL": "https://www.bcb.gov.br/content/estatisticas/estatistica_bancaria_estban/municipio/",
    "ESTBAN_INICIO": "2019-01",
    "ESTBAN_FIM": "2024-12",
    "INPE_BASE_URL": "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/anual/EstadosBr_sat_ref/{uf_sigla}/",
    "INPE_ANOS": "2019-2024",
}


def parse_year_range(value: str) -> list[int]:
    value = value.strip()
    if "-" in value:
        start, end = (int(v) for v in value.split("-", 1))
    else:
        start = end = int(value)
    if end < start:
        raise ValueError(f"Intervalo de anos invertido: {value!r}")
    return list(range(start, end + 1))


def _resolve(path_str: str) -> Path:
    path = Path(path_str)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _resolve_sqlite_url(url: str) -> str:
    prefix = "sqlite:///"
    if url.startswith(prefix) and not url.startswith(prefix + "/"):
        rel = url[len(prefix):]
        if rel and rel != ":memory:" and not Path(rel).is_absolute():
            return prefix + (PROJECT_ROOT / rel).as_posix()
    return url


@dataclass(frozen=True)
class Settings:
    env: dict[str, str]
    analytics: dict = field(default_factory=dict)

    def get(self, key: str) -> str:
        return self.env[key]

    @property
    def data_dir(self) -> Path:
        return _resolve(self.env["DATA_DIR"])

    @property
    def reports_dir(self) -> Path:
        return _resolve(self.env["REPORTS_DIR"])

    def layer_dir(self, layer: str) -> Path:
        return self.data_dir / layer

    @property
    def seed(self) -> int:
        return int(self.env["SEED"])

    @property
    def uf_sigla(self) -> str:
        return self.env["UF_SIGLA"].upper()

    @property
    def uf_codigo(self) -> int:
        return int(self.env["UF_CODIGO_IBGE"])

    @property
    def source_db_url(self) -> str:
        return _resolve_sqlite_url(self.env["SOURCE_DB_URL"])

    @property
    def warehouse_db_url(self) -> str:
        return _resolve_sqlite_url(self.env["WAREHOUSE_DB_URL"])

    @property
    def scr_local_parquet(self) -> Path:
        return _resolve(self.env["SCR_LOCAL_PARQUET"])

    def years(self, key: str) -> list[int]:
        return parse_year_range(self.env[key])

    def section(self, name: str) -> dict:
        return self.analytics.get(name, {})


def load_settings(env_file: Path | None = None, toml_file: Path | None = None) -> Settings:
    env_path = env_file if env_file is not None else PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)
    env = {key: os.environ.get(key, default) for key, default in DEFAULTS.items()}

    toml_path = toml_file if toml_file is not None else PROJECT_ROOT / "config" / "pipeline.toml"
    analytics: dict = {}
    if toml_path.exists():
        with toml_path.open("rb") as fh:
            analytics = tomllib.load(fh)
    return Settings(env=env, analytics=analytics)
