# Relatório de Cruzamento das Bases

> Os números abaixo vêm da execução de 2026-10-01. A versão viva, regenerada a cada execução, fica em
> `reports/silver_relatorio.md` (gerado por `python scripts/run_pipeline.py --stage silver`).

## 1. Pergunta e por que estas bases

**Pergunta central:** como a riqueza de um município do Pará (PIB per capita) se relaciona com o risco de
crédito local, e quais municípios priorizar em renegociação e na concessão de crédito sem garantia?

| Base | Instituição | Papel no projeto | Granularidade de origem |
|---|---|---|---|
| ESTBAN | BCB | **Alvo**: risco de crédito municipal = provisão / carteira | município × instituição × mês |
| PIB dos Municípios (via banco relacional) | IBGE | **Riqueza**: PIB, PIB per capita e composição setorial | município × ano |
| População (SIDRA) | IBGE | denominador do per capita e porte do município | município × ano |
| Localidades | IBGE | cadastro e hierarquia regional (dimensão) | município |
| SCR.data | BCB | **contexto regional**: inadimplência e ativo problemático do PA | UF × mês × dimensões |
| Queimadas | INPE | **enriquecimento**: pressão ambiental/agro (focos de calor) | foco (lat/lon, data) |

Três instituições (IBGE, BCB e INPE) e quatro formas de acesso: API REST, arquivo CSV/ZIP, banco relacional e
carga incremental com checkpoint.

## 2. Avaliação crítica da riqueza da base

A combinação pedida originalmente (PIB + População + SCR) **não sustenta a pergunta no nível municipal**:

1. **O SCR.data só chega até UF.** O arquivo do PA tem cerca de 20 a 34 mil linhas por mês, mas todas são da
   mesma UF. Para o modelo isso dá **uma** série temporal de 60 pontos, sem variabilidade entre municípios.
   A versão "SCR por sub-região" (API Olinda) divide o PA em só 3 prefixos de CEP (66, 67 e 68).
2. **O PIB municipal é anual e sai com cerca de 2 anos de defasagem.** Em jun/2024 o PIB mais recente
   disponível era o de 2021. O VAB setorial de 2022 e 2023 não foi divulgado no SIDRA.

**Enriquecimento adotado:**

- **ESTBAN (BCB):** traz crédito e **provisão para perdas** por município, mês a mês, para os 144 municípios
  (114 com série completa em 2019–2024). Em dez/2024 a razão provisão/carteira variou de 0,03% a 84% entre
  municípios (mediana de 3,2%). É ela que dá variabilidade à análise.
- **Focos de calor (INPE):** 230.850 focos em 2019–2024, todos associados a um município. Servem de proxy de
  atividade agropecuária e de pressão ambiental, relevantes para o crédito rural, que cresceu de R$ 2,4 bi para
  R$ 16,2 bi no período.

**Fontes avaliadas e não incorporadas:** Portal da Transparência (exige chave de API; extensão documentada) e
Novo CAGED (microdados em 7z via FTP, de alto volume; fica como próxima iteração).

## 3. Chave de cruzamento

- **Chave principal:** `cod_ibge_municipio` (7 dígitos, começando por 15), presente na origem do IBGE
  (Localidades `id`, SIDRA `D1C`), do ESTBAN (`CODMUN_IBGE`) e do banco de origem (`cod_municipio`).
- **INPE:** não tem código. A associação é feita pelo **nome normalizado** (sem acento, caixa alta, sem
  pontuação), contra `dim_municipio.nome_normalizado`.
- **Tempo:** `data_competencia` (último dia do mês) para ESTBAN, SCR e INPE; `ano` para PIB e população.
- **SCR:** junta-se à Gold **só pelo mês** (contexto regional, igual para todos os municípios do mês).

## 4. Resultado dos joins (casados, órfãos e tratamento)

| Join | Lado fonte | Casados | Órfãos fonte | Órfãos dimensão | Tratamento |
|---|---:|---:|---:|---:|---|
| ESTBAN × dim_municipio | 26.416 | 26.416 | 0 | 0 municípios (910 município-mês ausentes em 30 municípios) | órfão da fonte vai para a quarentena; município-mês ausente **não** vira zero e sai da coorte do t0 afetado |
| População × dim_municipio | 1.152 | 1.152 | 0 | 0 | 2023 interpolado (marcado) |
| PIB × dim_municipio | 864 | 864 | 0 | 0 | órfão vai para a quarentena |
| PIB × População (município, ano) | 864 | 864 | 0 | — | sem população, o per capita fica nulo |
| INPE (nome) × dim_municipio | 230.850 | 230.850 | 0 | 0 | sem nome casado, o foco vai para a quarentena (`municipio_nao_encontrado`) |
| SCR × dim_municipio | 1.650.167 | n/a | n/a | n/a | SCR é por UF; junta-se só por mês |

**Casos dignos de nota:**

- **Agência não processada (ESTBAN):** 1 linha (Banpará, São Domingos do Capim, jun/2022) veio sem valores e
  sem código IBGE porque o BCB não processou o balancete (`AGEN_PROCESSADAS = 0`). Ela foi para a quarentena
  com o motivo `agencia_nao_processada`. Não recuperamos o código: não haveria valor a associar.
- **Municípios sem agência bancária em parte do período:** Aveiro (68 de 72 meses sem dado), Chaves (64),
  Placas (58), São João da Ponta (51) e Peixe-Boi (45), entre outros. Não ter agência é diferente de ter
  crédito zero, por isso esses município-mês ficam ausentes.

## 5. Quebras de série conhecidas (documentadas, não corrigidas em silêncio)

| Quebra | Efeito | Tratamento |
|---|---|---|
| ESTBAN jan/2025 (Res. CMN 4.966/IFRS 9) | a razão provisão/carteira do PA cai de 4,28% para 0,02% | a Gold usa só até 2024-12 |
| ESTBAN: nomes de verbete mudam entre anos | ex.: `VERBETE_110_ENCAIXE` passa a `VERBETE_110_DISPONIBILIDADES` | harmonização pelo código numérico |
| ESTBAN: 2023-01 publicado como CSV sem ZIP | nome e formato do arquivo mudam | formato identificado pelos bytes |
| ESTBAN: verbetes 164 a 166 deixam de existir em 2022 | somavam zero no PA | `verbete_163` é contínuo |
| População: Censo 2022 vs. estimativas | Belém: 1,51 mi (estimativa 2021) e 1,30 mi (Censo 2022) | marcado em `fonte_populacao`; o modelo usa log-população |
| SCR CSV oficial vs. Parquet local | layouts de colunas diferentes | a Silver harmoniza (`layout_origem`) |
