from __future__ import annotations

import polars as pl

from credito_pa.common.control import LoadLog
from credito_pa.config import Settings
from credito_pa.quality.contracts import TableContract

SOURCES = [
    {
        "dataset_bronze": "ibge_municipios",
        "nome": "IBGE: API de Localidades (municípios da UF 15)",
        "instituicao": "IBGE",
        "url": "https://servicodados.ibge.gov.br/api/v1/localidades/estados/15/municipios",
        "acesso": "API REST (JSON aninhado)",
        "licenca": "Dado público (LAI 12.527/2011); uso livre com citação da fonte IBGE",
    },
    {
        "dataset_bronze": "ibge_populacao",
        "nome": "IBGE SIDRA: tabelas 6579 (estimativas de população) e 4709 (Censo 2022)",
        "instituicao": "IBGE",
        "url": "https://apisidra.ibge.gov.br/values/t/6579 ; https://apisidra.ibge.gov.br/values/t/4709",
        "acesso": "API REST (JSON), paginada por lotes de anos",
        "licenca": "Dado público (LAI 12.527/2011); uso livre com citação da fonte IBGE",
    },
    {
        "dataset_bronze": "source_db_pib_municipal",
        "nome": "PIB dos Municípios (IBGE SIDRA 5938), servido pelo banco relacional de origem",
        "instituicao": "IBGE (dado), carregado no banco de origem SQLite/PostgreSQL",
        "url": "https://apisidra.ibge.gov.br/values/t/5938 (seed) -> SOURCE_DB_URL",
        "acesso": "Banco relacional (SQLAlchemy), incremental por watermark em updated_at",
        "licenca": "Dado público (LAI 12.527/2011); uso livre com citação da fonte IBGE",
    },
    {
        "dataset_bronze": "estban_municipio",
        "nome": "ESTBAN: Estatística Bancária Mensal por Município (Documento 4500)",
        "instituicao": "Banco Central do Brasil",
        "url": "https://www.bcb.gov.br/content/estatisticas/estatistica_bancaria_estban/municipio/",
        "acesso": "Arquivo CSV (latin-1, ';') em ZIP, incremental mensal com checkpoint",
        "licenca": "Dado aberto do BCB (Plano de Dados Abertos do BCB). O conjunto não está no catálogo CKAN; "
                   "os conjuntos do BCB no catálogo usam ODbL",
    },
    {
        "dataset_bronze": "scr_data",
        "nome": "SCR.data: Painel de Operações de Crédito",
        "instituicao": "Banco Central do Brasil",
        "url": "https://www.bcb.gov.br/pda/desig/planilha_{ano}.zip",
        "acesso": "Arquivo CSV (UTF-8 com BOM, ';') em ZIP anual, lido com polars.scan_csv lazy filtrado para UF=PA",
        "licenca": "Open Data Commons Open Database License (ODbL), conforme dadosabertos.bcb.gov.br (dataset scr_data)",
    },
    {
        "dataset_bronze": "inpe_focos",
        "nome": "INPE Programa Queimadas: focos de calor anuais por estado (satélite de referência)",
        "instituicao": "INPE",
        "url": "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/anual/EstadosBr_sat_ref/PA/",
        "acesso": "Arquivo CSV (UTF-8, ',') em ZIP anual",
        "licenca": "Dado aberto (Plano de Dados Abertos do INPE, Portaria 307/2018; LAI); uso livre com citação da fonte",
    },
]


def collection_dates(settings: Settings) -> dict[str, str]:
    log = LoadLog(settings.layer_dir("_control")).read()
    if log.height == 0:
        return {}
    ok = log.filter(pl.col("status") == "success")
    agg = ok.group_by("dataset").agg(pl.col("finished_at").min().alias("primeira"), pl.col("finished_at").max().alias("ultima"))
    return {r["dataset"]: f"{r['primeira'][:10]} (1ª coleta); {r['ultima'][:10]} (última)" for r in agg.iter_rows(named=True)}


def render_sources(settings: Settings) -> str:
    dates = collection_dates(settings)
    lines = ["| Fonte | Instituição | URL | Forma de acesso | Licença | Data de coleta |", "|---|---|---|---|---|---|"]
    for s in SOURCES:
        lines.append(f"| {s['nome']} | {s['instituicao']} | {s['url']} | {s['acesso']} | {s['licenca']} | "
                     f"{dates.get(s['dataset_bronze'], 'ver data/_control/load_log.parquet')} |")
    return "\n".join(lines)


def _dtype(dt: pl.DataType) -> str:
    return str(dt).replace("Datetime(time_unit='us', time_zone=None)", "Datetime")


def render_table(contract: TableContract) -> str:
    lines = [
        f"### `{contract.layer}.{contract.name}`",
        "",
        contract.description,
        "",
        f"- **Granularidade:** {contract.granularity}",
        f"- **Chave primária:** ({', '.join(contract.primary_key)}), com unicidade verificada por código "
        f"(`quality.checks.assert_primary_key`)",
    ]
    for note in contract.notes:
        lines.append(f"- **Nota:** {note}")
    lines += ["", "| Coluna | Tipo | Nulo? | Domínio válido | Origem | Significado de negócio |", "|---|---|:---:|---|---|---|"]
    for c in contract.columns:
        lines.append(f"| `{c.name}` | {_dtype(c.dtype)} | {'sim' if c.nullable else 'não'} | {c.domain or '-'} | "
                     f"{c.origem} | {c.description} |")
    return "\n".join(lines) + "\n"


def render_dictionary(settings: Settings, layers: dict[str, list[TableContract]]) -> str:
    parts = [
        "# Dicionário de Dados",
        "",
        "> Gerado por `python scripts/gerar_docs.py` a partir dos contratos em `src/credito_pa/silver/contracts.py` "
        "e `src/credito_pa/gold/contracts.py`. Os mesmos contratos validam as tabelas em execução, então este "
        "documento não diverge do código.",
        "",
        "Todas as bases são **agregadas** (por UF, município ou instituição financeira). Nenhuma coluna identifica "
        "pessoa física.",
        "",
        "## Fontes",
        "",
        render_sources(settings),
        "",
    ]
    for layer, contracts in layers.items():
        parts += [f"## Camada {layer}", ""]
        parts += [render_table(c) for c in contracts]
    return "\n".join(parts)
