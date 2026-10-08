# Dicionário de Dados

> Gerado por `python scripts/gerar_docs.py` a partir dos contratos em `src/credito_pa/silver/contracts.py` e `src/credito_pa/gold/contracts.py`. Os mesmos contratos validam as tabelas em execução, então este documento não diverge do código.

Todas as bases são **agregadas** (por UF, município ou instituição financeira). Nenhuma coluna identifica pessoa física.

## Fontes

| Fonte | Instituição | URL | Forma de acesso | Licença | Data de coleta |
|---|---|---|---|---|---|
| IBGE: API de Localidades (municípios da UF 15) | IBGE | https://servicodados.ibge.gov.br/api/v1/localidades/estados/15/municipios | API REST (JSON aninhado) | Dado público (LAI 12.527/2011); uso livre com citação da fonte IBGE | 2026-10-01 (1ª coleta); 2026-10-01 (última) |
| IBGE SIDRA: tabelas 6579 (estimativas de população) e 4709 (Censo 2022) | IBGE | https://apisidra.ibge.gov.br/values/t/6579 ; https://apisidra.ibge.gov.br/values/t/4709 | API REST (JSON), paginada por lotes de anos | Dado público (LAI 12.527/2011); uso livre com citação da fonte IBGE | 2026-10-01 (1ª coleta); 2026-10-01 (última) |
| PIB dos Municípios (IBGE SIDRA 5938), servido pelo banco relacional de origem | IBGE (dado), carregado no banco de origem SQLite/PostgreSQL | https://apisidra.ibge.gov.br/values/t/5938 (seed) -> SOURCE_DB_URL | Banco relacional (SQLAlchemy), incremental por watermark em updated_at | Dado público (LAI 12.527/2011); uso livre com citação da fonte IBGE | 2026-10-01 (1ª coleta); 2026-10-01 (última) |
| ESTBAN: Estatística Bancária Mensal por Município (Documento 4500) | Banco Central do Brasil | https://www.bcb.gov.br/content/estatisticas/estatistica_bancaria_estban/municipio/ | Arquivo CSV (latin-1, ';') em ZIP, incremental mensal com checkpoint | Dado aberto do BCB (Plano de Dados Abertos do BCB). O conjunto não está no catálogo CKAN; os conjuntos do BCB no catálogo usam ODbL | 2026-10-01 (1ª coleta); 2026-10-01 (última) |
| SCR.data: Painel de Operações de Crédito | Banco Central do Brasil | https://www.bcb.gov.br/pda/desig/planilha_{ano}.zip | Arquivo CSV (UTF-8 com BOM, ';') em ZIP anual, lido com polars.scan_csv lazy filtrado para UF=PA | Open Data Commons Open Database License (ODbL), conforme dadosabertos.bcb.gov.br (dataset scr_data) | 2026-10-01 (1ª coleta); 2026-10-01 (última) |
| INPE Programa Queimadas: focos de calor anuais por estado (satélite de referência) | INPE | https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/anual/EstadosBr_sat_ref/PA/ | Arquivo CSV (UTF-8, ',') em ZIP anual | Dado aberto (Plano de Dados Abertos do INPE, Portaria 307/2018; LAI); uso livre com citação da fonte | 2026-10-01 (1ª coleta); 2026-10-01 (última) |

## Camada Silver

### `silver.dim_municipio`

Cadastro de municípios do PA com hierarquia regional (IBGE Localidades).

- **Granularidade:** Uma linha por município do Pará.
- **Chave primária:** (cod_ibge_municipio), com unicidade verificada por código (`quality.checks.assert_primary_key`)

| Coluna | Tipo | Nulo? | Domínio válido | Origem | Significado de negócio |
|---|---|:---:|---|---|---|
| `cod_ibge_municipio` | String | não | 7 dígitos, iniciando por 15 (UF Pará) | IBGE Localidades: id | Código IBGE do município (chave de cruzamento entre todas as bases) |
| `nome_municipio` | String | não | texto não vazio | IBGE Localidades: nome | Nome oficial do município |
| `nome_normalizado` | String | não | A-Z, 0-9 e espaço | derivado de nome_municipio | Nome sem acento, em caixa alta e sem pontuação (chave de casamento por nome) |
| `microrregiao` | String | não | - | IBGE Localidades: microrregiao.nome | Microrregião geográfica |
| `mesorregiao` | String | não | - | IBGE Localidades: microrregiao.mesorregiao.nome | Mesorregião geográfica |
| `regiao_imediata` | String | não | - | IBGE Localidades: regiao-imediata.nome | Região geográfica imediata |
| `regiao_intermediaria` | String | não | - | IBGE Localidades: regiao-imediata.regiao-intermediaria.nome | Região geográfica intermediária |
| `uf_sigla` | String | não | PA | IBGE Localidades | Sigla da UF |

### `silver.ibge_populacao_ano`

População residente: estimativas anuais (SIDRA 6579), Censo 2022 (SIDRA 4709) e 2023 interpolado.

- **Granularidade:** Uma linha por município do Pará por ano.
- **Chave primária:** (cod_ibge_municipio, ano), com unicidade verificada por código (`quality.checks.assert_primary_key`)
- **Nota:** 2023 não tem estimativa nem censo no SIDRA: valor = média de 2022 (Censo) e 2024 (estimativa), arredondada, marcado como 'interpolada'.

| Coluna | Tipo | Nulo? | Domínio válido | Origem | Significado de negócio |
|---|---|:---:|---|---|---|
| `cod_ibge_municipio` | String | não | 7 dígitos, iniciando por 15 (UF Pará) | SIDRA: D1C | Código IBGE do município (chave de cruzamento entre todas as bases) |
| `ano` | Int32 | não | 2018 a 2025 | SIDRA: D3C | Ano de referência da população |
| `populacao` | Int64 | não | > 0 | SIDRA: V (6579/9324 ou 4709/93); 2023 interpolado | Habitantes |
| `fonte_populacao` | String | não | estimativa | censo | interpolada | regra Silver | Origem do valor de população |

### `silver.ibge_pib_ano`

PIB municipal e VAB por setor (mil R$ correntes), vindos do banco relacional de origem (SIDRA 5938), com PIB per capita.

- **Granularidade:** Uma linha por município do Pará por ano de referência do PIB.
- **Chave primária:** (cod_ibge_municipio, ano), com unicidade verificada por código (`quality.checks.assert_primary_key`)

| Coluna | Tipo | Nulo? | Domínio válido | Origem | Significado de negócio |
|---|---|:---:|---|---|---|
| `cod_ibge_municipio` | String | não | 7 dígitos, iniciando por 15 (UF Pará) | banco de origem pib_municipal.cod_municipio | Código IBGE do município (chave de cruzamento entre todas as bases) |
| `ano` | Int32 | não | 2018 a 2023 | pib_municipal.ano | Ano de referência do PIB (não é o ano de publicação) |
| `pib_mil_reais` | Float64 | não | > 0 | variável 37 | PIB a preços correntes (mil R$) |
| `vab_total_mil_reais` | Float64 | sim | >= 0; nulo em 2022–2023 (VAB setorial municipal não divulgado no SIDRA 5938 para esses anos) | variável 498 | VAB total (mil R$) |
| `vab_agropecuaria_mil_reais` | Float64 | sim | >= 0; nulo em 2022–2023 (VAB setorial municipal não divulgado no SIDRA 5938 para esses anos) | variável 513 | VAB da agropecuária (mil R$) |
| `vab_industria_mil_reais` | Float64 | sim | >= 0; nulo em 2022–2023 (VAB setorial municipal não divulgado no SIDRA 5938 para esses anos) | variável 517 | VAB da indústria (mil R$) |
| `vab_servicos_mil_reais` | Float64 | sim | >= 0; nulo em 2022–2023 (VAB setorial municipal não divulgado no SIDRA 5938 para esses anos) | variável 6575 | VAB de serviços, exceto administração pública (mil R$) |
| `vab_adm_publica_mil_reais` | Float64 | sim | >= 0; nulo em 2022–2023 (VAB setorial municipal não divulgado no SIDRA 5938 para esses anos) | variável 525 | VAB da administração pública (mil R$) |
| `populacao` | Int64 | sim | > 0 | silver.ibge_populacao_ano | População do mesmo ano (para o per capita) |
| `pib_per_capita_reais` | Float64 | sim | > 0; nulo se não houver população do ano | derivado | PIB per capita em R$ correntes = pib_mil_reais*1000/populacao |
| `updated_at_origem` | Datetime | não | - | pib_municipal.updated_at (máximo entre variáveis) | Última atualização da linha no sistema de origem |

### `silver.estban_municipio_instituicao_mes`

Balancete das agências bancárias agregadas por município e instituição (BCB ESTBAN, Documento 4500).

- **Granularidade:** Uma linha por município do Pará por instituição financeira (CNPJ raiz) por mês de competência.
- **Chave primária:** (cod_ibge_municipio, cnpj_raiz, data_competencia), com unicidade verificada por código (`quality.checks.assert_primary_key`)
- **Nota:** Colunas selecionadas pelo CÓDIGO do verbete (regex ^VERBETE_(\d{3})), pois o nome descritivo muda entre anos.
- **Nota:** Crédito rural: até 2022-06 o BCB separava 163 a 166; 164 a 166 somam zero no PA em todo o período, então verbete_163 é contínuo.
- **Nota:** Linhas de agência esperada e não processada pelo BCB (AGEN_PROCESSADAS=0, valores vazios) vão para a quarentena com o motivo 'agencia_nao_processada'.
- **Nota:** Valores em R$ (inteiros no arquivo). Competências de 2025 em diante usam a Res. CMN 4.966 e não são comparáveis às anteriores; essa restrição é aplicada na Gold.

| Coluna | Tipo | Nulo? | Domínio válido | Origem | Significado de negócio |
|---|---|:---:|---|---|---|
| `cod_ibge_municipio` | String | não | 7 dígitos, iniciando por 15 (UF Pará) | ESTBAN: CODMUN_IBGE | Código IBGE do município (chave de cruzamento entre todas as bases) |
| `cnpj_raiz` | String | não | 8 dígitos | ESTBAN: CNPJ | CNPJ raiz (8 dígitos) da instituição |
| `data_competencia` | Date | não | último dia do mês, 2019-01-31 em diante | ESTBAN: #DATA_BASE (AAAAMM) | Mês de referência, padronizado para o último dia do mês |
| `nome_instituicao` | String | não | - | ESTBAN: NOME_INSTITUICAO | Nome da instituição |
| `codmun_bcb` | String | não | - | ESTBAN: CODMUN | Código de município interno do BCB |
| `agencias_processadas` | Int32 | não | >= 0 | ESTBAN: AGEN_PROCESSADAS | Agências da instituição no município com balancete processado |
| `verbete_160` | Float64 | não | valor monetário em R$ | ESTBAN: VERBETE_160 | Operações de crédito (saldo bruto da carteira, R$) |
| `verbete_161` | Float64 | sim | valor monetário em R$ | ESTBAN: VERBETE_161 | Empréstimos e títulos descontados (R$) |
| `verbete_162` | Float64 | sim | valor monetário em R$ | ESTBAN: VERBETE_162 | Financiamentos (R$) |
| `verbete_163` | Float64 | sim | valor monetário em R$ | ESTBAN: VERBETE_163 | Financiamentos rurais e agrícolas: custeio e investimento (R$) |
| `verbete_167` | Float64 | sim | valor monetário em R$ | ESTBAN: VERBETE_167 | Financiamentos agroindustriais (R$) |
| `verbete_169` | Float64 | sim | valor monetário em R$ | ESTBAN: VERBETE_169 | Financiamentos imobiliários (R$) |
| `verbete_171` | Float64 | sim | valor monetário em R$ | ESTBAN: VERBETE_171 | Outras operações de crédito (R$) |
| `verbete_172` | Float64 | sim | valor monetário em R$ | ESTBAN: VERBETE_172 | Outros créditos (R$) |
| `verbete_174` | Float64 | não | <= 0 | ESTBAN: VERBETE_174 | Provisão para operações de crédito (conta redutora, valor <= 0, R$) |
| `verbete_399` | Float64 | sim | valor monetário em R$ | ESTBAN: VERBETE_399 | Total do ativo (R$) |
| `verbete_420` | Float64 | sim | valor monetário em R$ | ESTBAN: VERBETE_420 | Depósitos de poupança (R$) |
| `verbete_432` | Float64 | sim | valor monetário em R$ | ESTBAN: VERBETE_432 | Depósitos a prazo (R$) |
| `verbete_401_419` | Float64 | sim | valor monetário em R$ | ESTBAN: VERBETE_401..419 | Depósitos à vista e demais depósitos, contas 401 a 419 somadas pelo BCB (R$) |

### `silver.scr_pa_mes`

Carteira de crédito do SCR (BCB), agregada pelo próprio BCB por UF e dimensões, filtrada para o PA.

- **Granularidade:** Uma linha por mês de competência por combinação de dimensões do SCR.data para o PA.
- **Chave primária:** (data_competencia, tcb, sr, cliente, ocupacao, cnae_secao, cnae_subclasse, porte, modalidade, origem, indexador), com unicidade verificada por código (`quality.checks.assert_primary_key`)

| Coluna | Tipo | Nulo? | Domínio válido | Origem | Significado de negócio |
|---|---|:---:|---|---|---|
| `data_competencia` | Date | não | último dia do mês, 2019-01-31 em diante | SCR.data: data_base | Mês de referência, padronizado para o último dia do mês |
| `tcb` | String | não | texto não vazio (valores do BCB) | SCR.data: tcb | Tipo de instituição (Bancário, Não bancário, Cooperativas) |
| `sr` | String | não | texto não vazio (valores do BCB) | SCR.data: sr | Segmento prudencial da instituição (S1 a S5; NAO_INFORMADO quando vazio na fonte) |
| `cliente` | String | não | texto não vazio (valores do BCB) | SCR.data: cliente | Tipo de cliente (PF ou PJ) |
| `ocupacao` | String | não | texto não vazio (valores do BCB) | SCR.data: ocupacao | Ocupação do tomador PF ('-' para PJ) |
| `cnae_secao` | String | não | texto não vazio (valores do BCB) | SCR.data: cnae_secao | Seção CNAE do tomador PJ ('-' para PF) |
| `cnae_subclasse` | String | não | texto não vazio (valores do BCB) | SCR.data: cnae_subclasse | Subclasse CNAE do tomador PJ ('-' para PF) |
| `porte` | String | não | texto não vazio (valores do BCB) | SCR.data: porte | Faixa de renda (PF) ou porte (PJ) |
| `modalidade` | String | não | texto não vazio (valores do BCB) | SCR.data: modalidade | Modalidade de crédito |
| `origem` | String | não | texto não vazio (valores do BCB) | SCR.data: origem | Origem dos recursos (com ou sem destinação específica) |
| `indexador` | String | não | texto não vazio (valores do BCB) | SCR.data: indexador | Indexador da operação |
| `numero_de_operacoes` | Int64 | sim | > 0 ou nulo | SCR.data: numero_de_operacoes | Quantidade de operações (nulo quando a fonte informa '<= 15') |
| `operacoes_ate_15` | Boolean | não | true | false | SCR.data: numero_de_operacoes | True quando a fonte omite a quantidade exata ('<= 15', sigilo) |
| `a_vencer_ate_90_dias` | Float64 | não | >= 0 | SCR.data: a_vencer_ate_90_dias | Saldo (R$): a vencer ate 90 dias |
| `a_vencer_de_91_ate_360_dias` | Float64 | não | >= 0 | SCR.data: a_vencer_de_91_ate_360_dias | Saldo (R$): a vencer de 91 ate 360 dias |
| `a_vencer_de_361_ate_1080_dias` | Float64 | não | >= 0 | SCR.data: a_vencer_de_361_ate_1080_dias | Saldo (R$): a vencer de 361 ate 1080 dias |
| `a_vencer_de_1081_ate_1800_dias` | Float64 | não | >= 0 | SCR.data: a_vencer_de_1081_ate_1800_dias | Saldo (R$): a vencer de 1081 ate 1800 dias |
| `a_vencer_de_1801_ate_5400_dias` | Float64 | não | >= 0 | SCR.data: a_vencer_de_1801_ate_5400_dias | Saldo (R$): a vencer de 1801 ate 5400 dias |
| `a_vencer_acima_de_5400_dias` | Float64 | não | >= 0 | SCR.data: a_vencer_acima_de_5400_dias | Saldo (R$): a vencer acima de 5400 dias |
| `vencido_acima_de_15_dias` | Float64 | não | >= 0 | SCR.data: vencido_acima_de_15_dias | Saldo (R$): vencido acima de 15 dias |
| `carteira_ativa` | Float64 | não | >= 0 | SCR.data: carteira_ativa | Saldo (R$): carteira ativa |
| `carteira_inadimplida_arrastada` | Float64 | não | >= 0 | SCR.data: carteira_inadimplida_arrastada | Saldo (R$): carteira inadimplida arrastada |
| `ativo_problematico` | Float64 | não | >= 0 | SCR.data: ativo_problematico | Saldo (R$): ativo problematico |
| `layout_origem` | String | não | csv_oficial | parquet_local | regra Silver | Layout do arquivo de origem |

### `silver.inpe_focos_pa`

Focos de calor (INPE Programa Queimadas), associados ao município IBGE pelo nome normalizado.

- **Granularidade:** Uma linha por foco de calor detectado pelo satélite de referência no Pará.
- **Chave primária:** (foco_id), com unicidade verificada por código (`quality.checks.assert_primary_key`)

| Coluna | Tipo | Nulo? | Domínio válido | Origem | Significado de negócio |
|---|---|:---:|---|---|---|
| `foco_id` | String | não | texto não vazio | INPE: foco_id | Identificador do foco no INPE |
| `data_hora_utc` | Datetime | não | 2019-01-01 em diante | INPE: data_pas | Data e hora da passagem do satélite (UTC) |
| `data_competencia` | Date | não | último dia do mês, 2019-01-31 em diante | derivado de data_hora_utc | Mês de referência, padronizado para o último dia do mês |
| `latitude` | Float64 | não | [-10, 3] (caixa do PA) | INPE: lat | Latitude |
| `longitude` | Float64 | não | [-59, -46] (caixa do PA) | INPE: lon | Longitude |
| `cod_ibge_municipio` | String | não | 7 dígitos, iniciando por 15 (UF Pará) | dim_municipio via nome normalizado de INPE: municipio | Código IBGE do município (chave de cruzamento entre todas as bases) |
| `municipio_inpe` | String | não | - | INPE: municipio | Nome do município como veio do INPE |
| `bioma` | String | não | Amazônia | Cerrado | INPE: bioma | Bioma |

## Camada Gold

### `gold.fato_credito_municipio_mes`

Painel mensal de crédito bancário, risco (provisão/carteira), riqueza conhecida na data, pressão ambiental e contexto regional do SCR. Base dos indicadores e da base ML-Ready.

- **Granularidade:** Uma linha por município do Pará por mês de competência (2019-01 a 2024-12).
- **Chave primária:** (cod_ibge_municipio, data_competencia), com unicidade verificada por código (`quality.checks.assert_primary_key`)
- **Nota:** Grade completa (144 municípios x 72 meses). Município-mês sem agência fica com possui_dado_estban = false e medidas de crédito nulas; ausência de agência não é carteira zero.
- **Nota:** Riqueza (PIB/população) é a CONHECIDA na data: o PIB do ano Y só é publicado em dez/Y+2. Isso evita usar informação futura.

| Coluna | Tipo | Nulo? | Domínio válido | Origem | Significado de negócio |
|---|---|:---:|---|---|---|
| `cod_ibge_municipio` | String | não | 7 dígitos, iniciando por 15 (UF Pará) | silver.dim_municipio | Código IBGE do município |
| `data_competencia` | Date | não | 2019-01-31 a 2024-12-31 | silver.estban (grade completa) | Mês de competência (último dia do mês) |
| `possui_dado_estban` | Boolean | não | true | false | silver.estban_municipio_instituicao_mes | True se há balancete ESTBAN do município no mês (há agência) |
| `n_instituicoes` | Int32 | sim | >= 1; nulo sem agência | ESTBAN: CNPJ distintos | Instituições financeiras com agência no município |
| `agencias` | Int32 | sim | >= 0; nulo sem agência | ESTBAN: AGEN_PROCESSADAS (soma) | Agências com balancete processado |
| `carteira_credito_reais` | Float64 | sim | >= 0 (R$); nulo se o município não tem agência no mês | ESTBAN verbete 160 (soma das instituições) | Saldo da carteira de crédito (R$) |
| `provisao_reais` | Float64 | sim | >= 0 (R$); nulo se o município não tem agência no mês | -(ESTBAN verbete 174) | Provisão para perdas com crédito (R$, positivo) |
| `razao_provisao` | Float64 | sim | [0, 1] | provisao_reais / carteira_credito_reais (nulo se carteira = 0 ou sem agência) | INDICADOR DE RISCO: provisão / carteira de crédito |
| `credito_rural_reais` | Float64 | sim | >= 0 (R$); nulo se o município não tem agência no mês | ESTBAN verbetes 163 + 167 | Crédito rural e agroindustrial (R$) |
| `credito_imobiliario_reais` | Float64 | sim | >= 0 (R$); nulo se o município não tem agência no mês | ESTBAN verbete 169 | Financiamentos imobiliários (R$) |
| `emprestimos_reais` | Float64 | sim | >= 0 (R$); nulo se o município não tem agência no mês | ESTBAN verbete 161 | Empréstimos e títulos descontados (R$) |
| `depositos_reais` | Float64 | sim | >= 0 (R$); nulo se o município não tem agência no mês | ESTBAN verbetes 401..419 + 420 + 432 | Depósitos à vista, de poupança e a prazo (R$) |
| `ano_ibge_referencia` | Int32 | não | <= ano(data_competencia) - 2 | regra: ano-2 em dezembro, ano-3 nos demais meses | Ano do PIB e da população conhecidos na data (defasagem de publicação) |
| `populacao_referencia` | Int64 | sim | > 0 | silver.ibge_populacao_ano | População do ano de referência |
| `pib_per_capita_reais` | Float64 | sim | > 0 | silver.ibge_pib_ano | PIB per capita do ano de referência (R$ correntes) = RIQUEZA |
| `ano_vab_referencia` | Int32 | sim | <= ano_ibge_referencia | silver.ibge_pib_ano | Ano mais recente, até o de referência, com VAB setorial publicado |
| `part_vab_agropecuaria` | Float64 | sim | [0, 1] | silver.ibge_pib_ano | Participação da agropecuária no VAB |
| `part_vab_industria` | Float64 | sim | [0, 1] | silver.ibge_pib_ano | Participação da indústria no VAB |
| `part_vab_adm_publica` | Float64 | sim | [0, 1] | silver.ibge_pib_ano | Participação da administração pública no VAB (dependência de renda pública) |
| `focos_calor_mes` | Int64 | não | >= 0 | silver.inpe_focos_pa (contagem) | Focos de calor detectados no mês |
| `scr_inadimplencia_pa` | Float64 | sim | [0, 1] | gold.contexto_scr_pa_mes (nulo antes de 2020-01) | Contexto: carteira inadimplida / carteira ativa no PA (SCR) |
| `scr_ativo_problematico_pa` | Float64 | sim | [0, 1] | gold.contexto_scr_pa_mes (nulo antes de 2020-01) | Contexto: ativo problemático / carteira ativa no PA (SCR) |

### `gold.contexto_scr_pa_mes`

Indicadores regionais de crédito do SCR.data (BCB) para o Pará; contexto macro-regional comum a todos os municípios no mês.

- **Granularidade:** Uma linha por mês de competência com indicadores agregados do SCR para o PA.
- **Chave primária:** (data_competencia), com unicidade verificada por código (`quality.checks.assert_primary_key`)

| Coluna | Tipo | Nulo? | Domínio válido | Origem | Significado de negócio |
|---|---|:---:|---|---|---|
| `data_competencia` | Date | não | 2020-01-31 a 2024-12-31 | silver.scr_pa_mes | Mês de competência |
| `carteira_ativa_pa` | Float64 | não | > 0 | Σ carteira_ativa | Carteira ativa total no PA (R$) |
| `inadimplencia_pa` | Float64 | não | [0, 1] | Σ carteira_inadimplida_arrastada / Σ carteira_ativa | Carteira inadimplida (atraso > 90 dias, arrastada) / carteira ativa |
| `ativo_problematico_pa` | Float64 | não | [0, 1] | Σ ativo_problematico / Σ carteira_ativa | Ativo problemático / carteira ativa |
| `inadimplencia_pf_pa` | Float64 | não | [0, 1] | recorte cliente = PF | Inadimplência de pessoas físicas |
| `inadimplencia_pj_pa` | Float64 | não | [0, 1] | recorte cliente = PJ | Inadimplência de pessoas jurídicas |
| `inadimplencia_rural_pa` | Float64 | não | [0, 1] | recorte modalidade contém 'Rural' | Inadimplência do crédito rural e agroindustrial (PF + PJ) |

### `gold.ml_base_risco_credito`

Base ML-Ready: features só da janela de observação [t0-11, t0] e label só da janela de predição (t0, t0+6]. Ver docs/ml_ready.md.

- **Granularidade:** Uma linha por município elegível por ponto de corte trimestral t0.
- **Chave primária:** (cod_ibge_municipio, t0), com unicidade verificada por código (`quality.checks.assert_primary_key`)
- **Nota:** Coorte: carteira > 0 nos 12 meses da janela, carteira em t0 >= R$ 5 mi e 6 meses de label completos.
- **Nota:** Os t0 entre o fim do treino e o início do teste ficam no split 'folga' e não entram em treino nem em teste.
- **Nota:** Ajuste aprovado no apply: a label de nível (>= P75 no futuro) era trivial (persistência AP 0,974). A label do modelo passou a ser a deterioração material; o nível atual alimenta o trilho de renegociação por regra.

| Coluna | Tipo | Nulo? | Domínio válido | Origem | Significado de negócio |
|---|---|:---:|---|---|---|
| `cod_ibge_municipio` | String | não | 7 dígitos, iniciando por 15 (UF Pará) | silver.dim_municipio | Código IBGE do município |
| `t0` | Date | não | fim de trimestre | fim de trimestre 2020-12..2024-06, mais o t0 de decisão 2024-12 | Ponto de corte: instante em que o conhecimento do modelo é congelado |
| `split` | String | não | treino | folga | teste | decisao | regra ML | Conjunto da observação no split temporal |
| `grupo_holdout` | Boolean | não | true | false | regra ML | Município sorteado (seed) para o cenário temporal + grupo (só no teste) |
| `data_min_feature` | Date | não | = t0 - 11 meses | auditoria | Competência mais antiga usada nas features |
| `data_max_feature` | Date | não | <= t0 | auditoria | Competência mais recente usada nas features (ANTI-VAZAMENTO: <= t0) |
| `carteira_t0` | Float64 | não | > 0 | fato em t0 | Carteira de crédito em t0 (R$); não é feature, serve para monetizar a decisão |
| `pib_per_capita_t0` | Float64 | sim | > 0 | fato em t0 | PIB per capita conhecido em t0 (R$); para análise por faixas |
| `razao_provisao_t0` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato em t0 | razão provisão/carteira em t0 |
| `razao_provisao_media_12m` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato [t0-11, t0] | média da razão nos 12 meses da janela |
| `razao_provisao_desvio_12m` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato [t0-11, t0] | desvio-padrão da razão na janela |
| `razao_provisao_max_12m` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato [t0-11, t0] | máximo da razão na janela |
| `razao_provisao_tendencia_6m` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato em t0 e t0-6 | razão em t0 menos razão em t0-6 |
| `log_carteira_t0` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato em t0 | log(1 + carteira de crédito em t0) |
| `crescimento_carteira_janela` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato em t0 e t0-11 | carteira em t0 / carteira em t0-11, menos 1 |
| `part_credito_rural_t0` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato em t0 | crédito rural / carteira em t0 |
| `part_credito_imobiliario_t0` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato em t0 | crédito imobiliário / carteira em t0 |
| `part_emprestimos_t0` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato em t0 | empréstimos / carteira em t0 |
| `razao_credito_depositos_t0` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato em t0 | carteira / depósitos em t0 (alavancagem local) |
| `n_instituicoes_t0` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato em t0 | nº de instituições com agência em t0 (concorrência bancária) |
| `log_pib_per_capita` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato em t0 (ano de referência defasado) | log do PIB per capita conhecido em t0 (RIQUEZA) |
| `part_vab_agropecuaria` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato em t0 | participação da agropecuária no VAB conhecido em t0 |
| `part_vab_industria` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato em t0 | participação da indústria no VAB conhecido em t0 |
| `part_vab_adm_publica` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato em t0 | participação da adm. pública no VAB conhecido em t0 |
| `log_populacao` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato em t0 | log da população do ano de referência |
| `focos_12m_por_10mil_hab` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato [t0-11, t0] | focos de calor na janela por 10 mil habitantes |
| `part_focos_ultimos_6m` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | fato [t0-11, t0] | fração dos focos da janela ocorrida nos últimos 6 meses |
| `scr_inadimplencia_t0` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | contexto SCR em t0 | inadimplência SCR-PA em t0 (contexto regional) |
| `scr_ativo_problematico_t0` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | contexto SCR em t0 | ativo problemático SCR-PA em t0 |
| `scr_inadimplencia_tendencia_6m` | Float64 | sim | real; nulo permitido (imputado só com estatísticas do treino) | contexto SCR em t0 e t0-6 | variação da inadimplência SCR-PA em 6 meses |
| `label_inicio` | Date | sim | > t0; nulo no split decisao | auditoria | 1º mês da janela de predição (ANTI-VAZAMENTO: > t0) |
| `label_fim` | Date | sim | nulo no split decisao | auditoria | Último mês da janela de predição (= t0 + 6 meses) |
| `razao_provisao_futura_media` | Float64 | sim | [0, 1]; nulo no split decisao | fato (t0, t0+6] | Média da razão provisão/carteira em (t0, t0+6]: base do EVENTO (NÃO é feature) |
| `deterioracao` | Int8 | sim | 0 | 1; nulo no split decisao | regra de rotulagem (config [ml]) | LABEL (trilho concessão): 1 se razao_provisao_futura_media >= 1,25 x razao_provisao_t0 E >= razao_provisao_t0 + 0,5 p.p. |
| `alto_risco_atual` | Int8 | não | 0 | 1 | regra de negócio | Trilho renegociação: 1 se razao_provisao_t0 >= limiar_alto_risco (conhecido em t0) |
| `alto_risco_futuro` | Int8 | sim | 0 | 1; nulo no split decisao | fato (t0, t0+6] | 1 se razao_provisao_futura_media >= limiar_alto_risco; só para AVALIAR a regra de renegociação (NÃO é feature) |
| `limiar_alto_risco` | Float64 | não | [0, 1] | data/gold/ml_artifacts/limiar_alto_risco.json | P75 da razão provisão/carteira município-mês até 2023-06 (congelado) |

### `gold.ranking_priorizacao_municipios`

Entrega de decisão em dois trilhos para os próximos 6 meses (jan-jun/2025): renegociação (regra de risco atual) e restrição de crédito sem garantia (alerta de deterioração do modelo).

- **Granularidade:** Uma linha por município do Pará elegível no t0 de decisão (2024-12).
- **Chave primária:** (cod_ibge_municipio), com unicidade verificada por código (`quality.checks.assert_primary_key`)

| Coluna | Tipo | Nulo? | Domínio válido | Origem | Significado de negócio |
|---|---|:---:|---|---|---|
| `cod_ibge_municipio` | String | não | 7 dígitos, iniciando por 15 (UF Pará) | silver.dim_municipio | Código IBGE do município |
| `nome_municipio` | String | não | - | silver.dim_municipio | Nome do município |
| `mesorregiao` | String | não | - | silver.dim_municipio | Mesorregião |
| `t0` | Date | não | 2024-12-31 | config [ml] t0_decisao | Data de decisão (conhecimento congelado) |
| `ordem_prioridade` | Int32 | não | >= 1 | regra de priorização | Posição na fila de ação (1 = mais urgente) |
| `acao_recomendada` | String | não | renegociar_e_restringir | renegociar_priorizar | restringir_credito_sem_garantia | manter | regra de decisão | Ação para a diretoria de crédito |
| `carteira_credito_reais` | Float64 | não | > 0 | ESTBAN | Carteira de crédito em t0 (R$) |
| `exposicao_sem_garantia_reais` | Float64 | não | >= 0 | carteira x fracao_carteira_exposta | Exposição estimada em linhas sem garantia (R$) |
| `pib_per_capita_reais` | Float64 | sim | > 0 | IBGE | PIB per capita conhecido em t0 (R$) |
| `razao_provisao_t0` | Float64 | não | [0, 1] | ESTBAN | Risco atual: provisão / carteira em t0 |
| `razao_provisao_tendencia_6m` | Float64 | sim | real | ESTBAN | Variação do risco nos últimos 6 meses |
| `alto_risco_atual` | Int8 | não | 0 | 1 | regra | Trilho renegociação: risco atual >= limiar P75 |
| `prob_deterioracao` | Float64 | não | [0, 1] | data/gold/ml_artifacts/modelo_final.joblib | Probabilidade de deterioração material em 6 meses (modelo) |
| `alerta_deterioracao` | Int8 | não | 0 | 1 | regra | Trilho concessão: prob_deterioracao >= limiar de custo |
| `ganho_esperado_restricao_reais` | Float64 | não | real | custos [custos] | Valor esperado de restringir as linhas sem garantia: p x C_FN x exposição - (1-p) x C_FP x exposição (R$) |
