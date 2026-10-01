# Notebooks

Espaço para análise exploratória (EDA). Regra do projeto: notebooks **só leem a camada Gold**
(`data/gold/*.parquet` ou o warehouse em `WAREHOUSE_DB_URL`) e não fazem limpeza. Se precisar limpar,
a correção vai para a Silver.

```python
import polars as pl
fato = pl.read_parquet("data/gold/fato_credito_municipio_mes.parquet")
ranking = pl.read_parquet("data/gold/ranking_priorizacao_municipios.parquet")
```
