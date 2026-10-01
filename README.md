# Projeto Integrador: Da Ingestão à Decisão
## Risco de crédito municipal no Pará × riqueza (PIB per capita)

Pipeline de dados completo, de bases públicas reais à recomendação de decisão: **Bronze → Silver → Gold → base
ML-Ready → modelo → ranking de municípios**. O enunciado está em `lauda/Projeto_Da_Ingestao_a_Decisao.pdf`.

**Pergunta central:** como a riqueza de um município do Pará (PIB per capita) se relaciona com o risco de crédito local,
e onde cooperativas e bancos regionais devem (1) fazer campanhas de renegociação e (2) restringir crédito sem garantia?

**Resposta curta** (execução de 2026-10-01; detalhes em [`docs/decisao.md`](docs/decisao.md)):

- **42 de 127 municípios elegíveis** estão em risco alto (provisão ≥ 4,07% da carteira) e concentram **72,1% das perdas
  provisionadas** do estado. Para eles, a recomendação é uma **campanha de renegociação** (regra transparente, com
  precisão de 93,8% e recall de 91,0% no teste).
- **A riqueza não explica o nível de risco** (Spearman de −0,04). Os municípios mais pobres deterioram com mais frequência,
  mas isso se explica pelo **porte menor da carteira**, não pelo PIB per capita em si.
- O **modelo de alerta de deterioração** (AP de 0,20, contra 0,08 do acaso e 0,15 da melhor regra simples) ordena bem os
  municípios, mas em dez/2024 nenhum atingiu o limiar de custo para restringir crédito. A recomendação é **vigilância
  mensal** de 5 municípios.

---

## 1. Arquitetura

```
 FONTES (3 instituições, 4 formas de ingestão)                 CAMADAS                               ENTREGA
 +-----------------------------------------+
 | IBGE Localidades + SIDRA (API REST JSON) |--+
 | PIB municipal (banco relacional, SQLAlch.|--+   +-----------+   +------------+   +-------------+
 |   incremental por watermark)             |  +-->|  BRONZE   |-->|   SILVER   |-->|    GOLD     |--> warehouse (SQL)
 | BCB ESTBAN (CSV latin-1, incremental     |  |   | dado bruto|   | tipado,    |   | fato mensal |
 |   mensal com checkpoint)                 |--+   | +metadados|   | contratos, |   | contexto SCR|
 | BCB SCR.data (CSV, polars.scan_csv lazy, |--+   | +hash     |   | dedup,     |   | base ML     |--> modelo (sklearn)
 |   filtro UF=PA no lazy frame)            |  |   | quarentena|   | integridade|   | ranking     |--> reports/decisao.md
 | INPE Queimadas (CSV anual)               |--+   +-----------+   +------------+   +-------------+
 +-----------------------------------------+         |                  |                 |
                                                 data/quarantine/   reports/silver_     reports/gold_relatorio.md
                                                 data/_control/     relatorio.md        reports/ml_resultados.md
                                                 (load_log, state)
```

| Camada | O que garante | Onde |
|---|---|---|
| **Bronze** | Dado como veio (texto), imutável, particionado por `ingestion_date`, com `ingestion_timestamp`, `source_system`, `source_object`, `load_id` e `record_hash`. Idempotente (anti-join por hash), com quarentena de registros e arquivos e log de cargas. | `src/credito_pa/bronze/` |
| **Silver** | Contrato por tabela (tipos, domínios, PK, granularidade), conversão BR → numérico, datas no fim do mês, deduplicação semântica, quarentena com motivo e relatório de integridade referencial. | `src/credito_pa/silver/` |
| **Gold** | Tabelas orientadas à decisão, com granularidade declarada e PK verificada. Nenhuma limpeza: violação de contrato **falha** a execução. | `src/credito_pa/gold/` |
| **ML** | Label, coorte, t0 e janelas formais, split temporal + grupo, pré-processamento dentro do `Pipeline` e testes anti-vazamento. | `src/credito_pa/ml/`, [`docs/ml_ready.md`](docs/ml_ready.md) |

## 2. Fontes de dados

| Fonte | Instituição | Acesso | Papel |
|---|---|---|---|
| ESTBAN (estatística bancária por município) | BCB | arquivo CSV/ZIP, incremental mensal | **alvo**: provisão/carteira por município |
| SCR.data (painel de crédito) | BCB | arquivo CSV/ZIP (≈ 1,8 GB para 2020–2024), `scan_csv` lazy | contexto regional (só UF) |
| PIB dos Municípios (SIDRA 5938) | IBGE | **banco relacional** (seed a partir da API) | **riqueza** |
| População (SIDRA 6579 e 4709) e Localidades | IBGE | **API REST** | per capita e cadastro |
| Focos de calor | INPE | arquivo CSV/ZIP anual | enriquecimento ambiental |

URLs, licenças e datas de coleta estão em [`docs/dicionario_dados.md`](docs/dicionario_dados.md#fontes). A avaliação
crítica da base e os resultados dos joins (casados e órfãos) estão em
[`docs/relatorio_cruzamento.md`](docs/relatorio_cruzamento.md).

## 3. Como rodar do zero

**Pré-requisitos:** Python **3.12**, acesso à internet e cerca de 3 GB livres em disco (o cache do SCR ocupa 1,8 GB).

```bash
# 1. Ambiente (Windows: troque "source .venv/bin/activate" por ".venv\Scripts\activate")
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pip install -e .

# 2. Configuração (opcional: sem .env valem os defaults, com SQLite local)
cp .env.example .env            # Windows: copy .env.example .env

# 3. Pipeline de ponta a ponta (seed -> bronze -> silver -> gold -> ml -> report)
#    1ª vez: cerca de 20 min (o download e a leitura do SCR levam cerca de 14 min); nas seguintes, cerca de 1 min (cache + idempotência)
python scripts/run_pipeline.py --stage all
```

Ou etapa por etapa:

```bash
python scripts/run_pipeline.py --stage seed      # cria/popula o banco relacional de ORIGEM (PIB) - idempotente
python scripts/run_pipeline.py --stage bronze    # todas as fontes (ou --source ibge --source estban ...)
python scripts/run_pipeline.py --stage silver    # + reports/silver_relatorio.md (integridade referencial)
python scripts/run_pipeline.py --stage gold      # + base ML-Ready + export para o warehouse + reports/gold_relatorio.md
python scripts/run_pipeline.py --stage ml        # validação temporal, modelos, baselines -> reports/ml_resultados.md
python scripts/run_pipeline.py --stage report    # ranking + reports/decisao.md (frase-resposta)
python scripts/gerar_docs.py                     # regenera docs/dicionario_dados.md a partir dos contratos
```

**Demonstração de idempotência (para a defesa):**

```bash
python scripts/demo_idempotencia.py              # roda a Bronze 2x e mostra contagens iguais e "gravadas na 2ª = 0"
python scripts/demo_idempotencia.py --force-scr  # idem, relendo os 60 CSVs do SCR (prova a dedup por hash)
```

**Testes (rodam offline, com fixtures locais):**

```bash
python -m pytest
```

**Dicas:**

- Sem internet para o SCR? Use `SCR_SOURCE_MODE=parquet` e `SCR_LOCAL_PARQUET=<caminho>` no `.env`. No modo `auto`
  (default), o fallback é automático.
- **Trocar para PostgreSQL:** instale `psycopg2-binary==2.9.12` e mude só o `.env`:
  `SOURCE_DB_URL=postgresql+psycopg2://usuario:senha@host:5432/origem` e
  `WAREHOUSE_DB_URL=postgresql+psycopg2://usuario:senha@host:5432/dw`.
- Parâmetros analíticos (janelas, limiares, custos) ficam em `config/pipeline.toml`. Seed global: `SEED=42`.

## 4. Estrutura do repositório

```
config/pipeline.toml        parâmetros analíticos versionados (janelas, limiares, custos)
src/credito_pa/
  config.py                 .env + pipeline.toml -> Settings
  pipeline.py               orquestração por etapas
  common/                   http (retry/backoff), download, hashing, io atômico, controle (load_log, state), db
  bronze/                   contract.py (contrato comum) + uma ingestão por fonte
  silver/                   contracts.py + um módulo por fonte + integrity.py + run.py
  gold/                     contracts.py, fato_mensal.py, ranking.py, export_warehouse.py, run.py
  ml/                       dataset.py (label/coorte/t0/split), train.py, evaluate.py, decision.py, run.py
  quality/                  contratos, checagens (schema/domínio/PK) e gerador do dicionário
scripts/                    run_pipeline.py, seed_source_db.py, demo_idempotencia.py, gerar_docs.py
tests/                      testes offline (idempotência, quarentena, retry, PK, anti-vazamento, modelo, docs)
docs/                       dicionário de dados, relatório de cruzamento, ML-Ready, decisão
notebooks/                  EDA opcional (só lê a Gold)
openspec/                   especificação OpenSpec (proposal, design, specs, tasks)
lauda/                      enunciado do projeto
data/  reports/  .venv/     NÃO versionados (.gitignore)
```

## 5. Documentação

| Documento | Conteúdo |
|---|---|
| [`docs/dicionario_dados.md`](docs/dicionario_dados.md) | Para cada tabela Silver/Gold: coluna, tipo, domínio, origem e significado. Fontes com URL, licença e data de coleta. **Gerado dos contratos.** |
| [`docs/relatorio_cruzamento.md`](docs/relatorio_cruzamento.md) | Avaliação crítica da base, enriquecimento, chave de cruzamento, casados e órfãos e quebras de série |
| [`docs/ml_ready.md`](docs/ml_ready.md) | Label, regra, coorte, t0, janelas, split, baselines, métrica e **checklist anti-vazamento com prova** |
| [`docs/decisao.md`](docs/decisao.md) | Frase-resposta explicada linha a linha, decisor, custos FP/FN, limiar e limitações |
| `reports/*.md` (gerados) | Relatórios vivos de Silver, Gold, modelo e decisão, e a verificação integrada |

## 6. Uso de IA generativa (declaração obrigatória)

O desenvolvimento usou um **assistente de programação baseado em IA generativa (LLM)**, governado pelo framework
**OpenSpec** (`@fission-ai/openspec`). O ciclo foi `/opsx:explore` → `/opsx:propose` → `/opsx:apply` → verificação → arquivo.

| Parte | Uso da IA | Decisão humana |
|---|---|---|
| Exploração das fontes | Testou APIs e arquivos reais; descobriu que o SCR não tem município, a quebra contábil de 2025 no ESTBAN e o atraso de publicação do PIB | O grupo escolheu o ESTBAN como alvo municipal e o SCR como contexto |
| Especificação | Redigiu proposal, design, specs e tasks (`openspec/`) | O grupo revisou e aprovou |
| Código e testes | Implementou o pacote `src/credito_pa`, os scripts e os testes `pytest` | O grupo revisa cada commit |
| Diagnóstico do modelo | Detectou que a label de nível era trivial (persistência com AP 0,974) e propôs alternativas | O grupo escolheu os **dois trilhos** e as **premissas de custo** |
| Documentação | Redigiu README, dicionário, relatórios e textos de decisão | O grupo revisou |

**Regras seguidas:** a IA **não** executou `git commit`/`push`. O versionamento foi feito manualmente pela equipe, a partir
de mensagens Conventional Commits sugeridas a cada tarefa. **Todo integrante precisa saber explicar qualquer trecho:**
recomendamos ler `docs/ml_ready.md` e `docs/decisao.md` e rodar `pytest` e a demo de idempotência antes da defesa.

## 7. Dados, privacidade e licenças

- Só **dados abertos e agregados** (por UF, município ou instituição). Nenhuma coluna identifica pessoa física.
- Licenças: SCR.data sob **ODbL** (catálogo do BCB). ESTBAN como dado aberto do BCB. IBGE e INPE como dados públicos (LAI)
  com citação da fonte. Detalhes em [`docs/dicionario_dados.md`](docs/dicionario_dados.md#fontes).
- Os dados **não** vão para o Git: `data/` e `reports/` estão no `.gitignore`. O repositório guarda os scripts que baixam os dados.
