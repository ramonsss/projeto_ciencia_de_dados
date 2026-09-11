# Projeto Integrador — Da Ingestão à Decisão: Risco de Crédito no Pará

O estado do Pará possui realidades econômicas muito distintas entre seus municípios. Este projeto conecta três fontes de dados para analisar a relação entre o PIB per capita municipal e a taxa de inadimplência de crédito especificamente na região.

## Fontes
- **IBGE (API REST)**: População estimada por município
- **IBGE (PostgreSQL)**: PIB Municipal
- **Banco Central - SCR (CSV)**: Sistema de Informações de Crédito (inadimplência e carteira ativa - dados filtrados para o Estado do PA)

## A Pergunta de Negócio
*"Como a riqueza de um município do Pará (PIB per capita) impacta o risco de crédito da região?"*

**Objetivo:** Cruzando as bases de PIB, População e SCR, o pipeline identifica quais perfis de municípios paraenses apresentam as maiores taxas de inadimplência (ativos problemáticos em relação à carteira ativa). O projeto ajuda a mapear áreas de risco e oportunidades financeiras no estado.

## Execução
```bash
# Exemplo de execução da ingestão e limpeza (Silver)
python scripts/ingest_csv_scr.py
python scripts/silver_scr.py
```

## Arquitetura Medalhão
- **Bronze**: Dados brutos ingeridos por fonte com metadados técnicos. O CSV gigante de 13GB do SCR é filtrado durante a ingestão (via Polars Lazy) para salvar apenas os dados do Pará, otimizando o armazenamento.
- **Silver**: Dados limpos, tipados, com deduplicação e envio de registros problemáticos para quarentena.
- **Gold**: (A ser construída) Agregação do PIB per capita e cálculo da % de inadimplência por município/mês, gerando a base ML-Ready.

## Decisão e Recomendação (A ser finalizada)
**Exemplo de Ação:** Ao final, o pipeline recomendará que cooperativas de crédito e bancos regionais do Norte priorizem campanhas de renegociação de dívidas ou limitem a concessão de novas linhas de crédito sem garantia nos municípios "X" e "Y" do Pará, que possuem PIB estagnado e inadimplência crescente. O custo do erro é alto: liberar crédito que não será pago (falso positivo) ou deixar de financiar produtores promissores (falso negativo).