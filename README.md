# Projeto de Ciência de Dados - SIM / IBGE / BCB

Este projeto conecta três fontes de dados para identificar estados com maior mortalidade proporcional à população e verificar se esses períodos coincidem com aumento da inadimplência de crédito.

## Fontes
- OpenDataSUS / SIM: mortalidade por município e data de óbito
- IBGE: população estimada por UF
- Banco Central: série nacional de inadimplência de crédito

## Objetivo
Cruzando as bases, o pipeline identifica quais estados apresentam maior taxa de mortalidade por 100 mil habitantes e compara esse cenário com a evolução da inadimplência, apoiando priorização de ações preventivas em saúde.

## Execução
```bash
python scripts/run_ingestion.py
```

## Camadas
- Bronze: dados brutos ingeridos por fonte
- Silver: mortalidade por UF e taxa por 100 mil habitantes
- Gold: ranking dos estados e resumo executivo de recomendação

## Recomendação gerada
Os estados com maior risco de mortalidade proporcional à população devem receber prioridade em ações preventivas nos próximos 12 meses. O custo de erro é alocar recursos em regiões de menor prioridade e deixar áreas críticas sem atendimento adequado.