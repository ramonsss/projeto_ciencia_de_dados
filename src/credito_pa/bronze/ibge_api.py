from __future__ import annotations

import json
from dataclasses import dataclass

import polars as pl

from credito_pa.bronze.contract import BronzeWriter
from credito_pa.common.control import utc_now
from credito_pa.common.http import HttpClient, HttpRequestError
from credito_pa.common.io import write_json_atomic
from credito_pa.common.logging import get_logger
from credito_pa.config import Settings

log = get_logger("bronze.ibge")

SOURCE_SYSTEM = "API_IBGE"
SIDRA_FIELDS = ["NC", "NN", "MC", "MN", "V", "D1C", "D1N", "D2C", "D2N", "D3C", "D3N"]
PERIODOS_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/{tabela}/periodos"


@dataclass(frozen=True)
class SidraQuery:
    tabela: str
    variavel: str
    descricao: str


POPULACAO_QUERIES = [
    SidraQuery("6579", "9324", "estimativa_populacao"),
    SidraQuery("4709", "93", "censo_2022_populacao"),
]


def chunk(items: list, size: int) -> list[list]:
    size = max(1, int(size))
    return [items[i:i + size] for i in range(0, len(items), size)]


def sidra_url(base_url: str, query: SidraQuery, anos: list[int], uf_codigo: int) -> str:
    periodos = ",".join(str(a) for a in anos)
    return f"{base_url.rstrip('/')}/t/{query.tabela}/n6/in%20n3%20{uf_codigo}/v/{query.variavel}/p/{periodos}"


def available_years(client: HttpClient, tabela: str, requested: list[int]) -> list[int]:
    try:
        periodos = client.get_json(PERIODOS_URL.format(tabela=tabela))
        publicados = {int(p["id"]) for p in periodos if str(p.get("id", "")).isdigit()}
        anos = [a for a in requested if a in publicados]
        log.info("SIDRA t%s: anos pedidos=%s publicados no intervalo=%s", tabela, requested, anos)
        return anos
    except (HttpRequestError, KeyError, TypeError) as exc:
        log.warning("Não foi possível ler os períodos da tabela %s (%s). Usando os anos pedidos.", tabela, exc)
        return requested


def parse_sidra_rows(data) -> tuple[list[dict], list[dict]]:
    if not isinstance(data, list):
        return [], [{"raw_record": data, "quarantine_reason": "unexpected_json_structure"}]
    body = data[1:] if data and isinstance(data[0], dict) and data[0].get("V") == "Valor" else data
    validas, invalidas = [], []
    for row in body:
        if isinstance(row, dict) and all(k in row for k in SIDRA_FIELDS):
            validas.append({k: row[k] for k in SIDRA_FIELDS})
        else:
            invalidas.append({"raw_record": row, "quarantine_reason": "schema_mismatch"})
    return validas, invalidas


def fetch_sidra_pages(settings: Settings, client: HttpClient, query: SidraQuery, anos: list[int]):
    base = settings.get("IBGE_SIDRA_URL")
    for page in chunk(anos, int(settings.get("IBGE_PAGE_SIZE_ANOS"))):
        url = sidra_url(base, query, page, settings.uf_codigo)
        source_object = f"sidra/t{query.tabela}/v{query.variavel}/p{page[0]}-{page[-1]}"
        try:
            yield source_object, url, client.get_json(url), None
        except HttpRequestError as exc:
            yield source_object, url, None, exc


def ingest_sidra(settings: Settings, dataset: str, queries: list[SidraQuery], anos: list[int],
                 client: HttpClient | None = None, writer: BronzeWriter | None = None) -> BronzeWriter:
    client = client or HttpClient.from_settings(settings)
    writer = writer or BronzeWriter(settings, dataset, SOURCE_SYSTEM)
    landing = settings.layer_dir("landing") / "ibge" / dataset / writer.ingestion_date
    for query in queries:
        anos_q = available_years(client, query.tabela, anos)
        for source_object, url, data, err in fetch_sidra_pages(settings, client, query, anos_q):
            started = utc_now()
            if err is not None:
                writer.quarantine_records(
                    [{"raw_record": {"url": url, "status": err.status, "detail": err.detail}, "quarantine_reason": err.reason}],
                    source_object,
                )
                writer.log_object(source_object, status="failed", rows_read=0, rows_written=0, rows_quarantined=1,
                                  rows_skipped=0, started_at=started, message=str(err)[:500])
                continue
            write_json_atomic({"url": url, "fetched_at": started.isoformat(), "data": data},
                              landing / (source_object.replace("/", "_") + f"__{writer.load_id}.json"))
            validas, invalidas = parse_sidra_rows(data)
            payload = pl.DataFrame(validas, schema={k: pl.Utf8 for k in SIDRA_FIELDS}) if validas \
                else pl.DataFrame(schema={k: pl.Utf8 for k in SIDRA_FIELDS})
            writer.write_batch(payload, source_object, quarantined=invalidas, started_at=started,
                               message=f"url={url}; tentativas={client.last_attempts}")
    return writer


def ingest_populacao(settings: Settings, client: HttpClient | None = None) -> BronzeWriter:
    return ingest_sidra(settings, "ibge_populacao", POPULACAO_QUERIES, settings.years("IBGE_POP_ANOS"), client)


LOCALIDADES_FIELDS = [
    "id", "nome", "microrregiao_id", "microrregiao_nome", "mesorregiao_id", "mesorregiao_nome",
    "regiao_imediata_id", "regiao_imediata_nome", "regiao_intermediaria_id", "regiao_intermediaria_nome", "uf_sigla",
]


def flatten_municipio(m: dict) -> dict:
    micro = m["microrregiao"]
    meso = micro["mesorregiao"]
    imediata = m["regiao-imediata"]
    intermediaria = imediata["regiao-intermediaria"]
    return {
        "id": m["id"], "nome": m["nome"],
        "microrregiao_id": micro["id"], "microrregiao_nome": micro["nome"],
        "mesorregiao_id": meso["id"], "mesorregiao_nome": meso["nome"],
        "regiao_imediata_id": imediata["id"], "regiao_imediata_nome": imediata["nome"],
        "regiao_intermediaria_id": intermediaria["id"], "regiao_intermediaria_nome": intermediaria["nome"],
        "uf_sigla": meso["UF"]["sigla"],
    }


def ingest_localidades(settings: Settings, client: HttpClient | None = None) -> BronzeWriter:
    client = client or HttpClient.from_settings(settings)
    writer = BronzeWriter(settings, "ibge_municipios", SOURCE_SYSTEM)
    url = settings.get("IBGE_LOCALIDADES_URL").format(uf_codigo=settings.uf_codigo)
    source_object = f"localidades/estados/{settings.uf_codigo}/municipios"
    started = utc_now()
    try:
        data = client.get_json(url)
    except HttpRequestError as exc:
        writer.quarantine_records([{"raw_record": {"url": url, "detail": exc.detail}, "quarantine_reason": exc.reason}], source_object)
        writer.log_object(source_object, status="failed", rows_read=0, rows_written=0, rows_quarantined=1,
                          rows_skipped=0, started_at=started, message=str(exc)[:500])
        return writer
    landing = settings.layer_dir("landing") / "ibge" / "ibge_municipios" / writer.ingestion_date
    write_json_atomic({"url": url, "fetched_at": started.isoformat(), "data": data},
                      landing / f"municipios__{writer.load_id}.json")
    validas, invalidas = [], []
    for m in data if isinstance(data, list) else []:
        try:
            validas.append({k: str(v) for k, v in flatten_municipio(m).items()})
        except (KeyError, TypeError) as exc:
            invalidas.append({"raw_record": json.dumps(m, ensure_ascii=False), "quarantine_reason": f"schema_mismatch:{exc}"})
    payload = pl.DataFrame(validas, schema={k: pl.Utf8 for k in LOCALIDADES_FIELDS})
    writer.write_batch(payload, source_object, quarantined=invalidas, started_at=started, message=f"url={url}")
    return writer
