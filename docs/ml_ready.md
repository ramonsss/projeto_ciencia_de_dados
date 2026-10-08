# Base ML-Ready e Anti-Vazamento

> As definições abaixo foram fixadas **antes** do treino final. Os números da coorte vêm de `reports/ml_ready_resumo.json`
> (execução de 2026-10-01). Os parâmetros estão em `config/pipeline.toml` (seção `[ml]`) e o código, em
> `src/credito_pa/ml/dataset.py`.
> Tabela: `gold.ml_base_risco_credito`, com granularidade "uma linha por município elegível por ponto de corte trimestral t0".

## 0. Por que a label é "deterioração", e não "nível de risco"

A primeira versão usava a label **"razão provisão/carteira média nos próximos 6 meses ≥ P75"**. Na avaliação, o
baseline de **persistência** (a regra "o risco de hoje é o de amanhã") atingiu **AP = 0,974** no teste, e os modelos
empataram (0,972). A correlação entre a razão em t0 e a futura é de **0,89**. A métrica alta disparou o alerta de
investigação. A causa **não era vazamento** (os testes da seção 3 continuam passando): era um problema trivial, em que o
ML não agrega nada.

Decisão do grupo (registrada nos artefatos OpenSpec), em **dois trilhos**:

1. **Renegociação → regra, sem ML:** `alto_risco_atual = razão em t0 ≥ P75`. A persistência já resolve o problema
   (precisão de 93,8% e recall de 91,0% no teste para antecipar o alto risco futuro).
2. **Concessão de crédito sem garantia → modelo de alerta de DETERIORAÇÃO MATERIAL** (definição abaixo).

**Declaração de transparência:** durante esse diagnóstico, algumas variantes de label de deterioração foram inspecionadas
no conjunto de teste. Para não escolher a label pelo teste, a definição foi fixada por **critério de negócio** ("piora
de pelo menos 25% **e** de pelo menos meio ponto percentual"), e não pela variante de maior AP. Mesmo assim, as métricas
de teste devem ser lidas como **ligeiramente otimistas**.

## 1. Definições

| Elemento | Definição adotada |
|---|---|
| **Entidade** | Município do Pará (`cod_ibge_municipio`). |
| **Label (positivo)** | `deterioracao = 1`: o risco de crédito do município **piora materialmente nos próximos 6 meses**. O evento é conhecido no fim da janela de predição (t0 + 6 meses), quando os 6 balancetes ESTBAN estão publicados. |
| **Regra de rotulagem** | `deterioracao = 1` ⇔ `F ≥ 1,25 × R0` **e** `F ≥ R0 + 0,005`, em que `R0 = razao_provisao(t0)` e `F = média de razao_provisao[m]` para m em (t0, t0+6]. A razão é provisão (−verbete 174) / carteira (verbete 160). Código: `deterioracao_expr`. Exemplos testados: 0,2% → 0,4% **não** é deterioração (só +0,2 p.p.); 3,0% → 4,0% é. |
| **Limiar de alto risco (trilho renegociação)** | **P75 = 4,073%** da razão município-mês (carteira ≥ R$ 5 mi), calculado **só com competências até 2023-06** (fim da última janela de label do treino; 5.924 município-mês). Fica congelado em `data/gold/ml_artifacts/limiar_alto_risco.json`. |
| **Coorte (elegível em t0)** | (i) carteira > 0 em **todos** os 12 meses da janela de observação; (ii) carteira em t0 ≥ **R$ 5 mi**; (iii) para observações rotuladas, os 6 meses da janela de predição com dado. |
| **Casos excluídos** | `carteira_incompleta_na_janela`: município sem agência em algum mês. Ausência de agência não é risco zero. Entre 3 e 25 municípios por t0. `carteira_abaixo_minimo`: em carteiras pequenas, um único contrato provisionado faz a razão saltar. Entre 10 e 15 por t0. `janela_predicao_incompleta`: 1 caso em 2020-12 e 1 em 2021-03. A contagem por t0 está em `reports/ml_ready_resumo.json`. |
| **Ponto de corte (t0)** | Fim de cada trimestre, de 2020-12 a 2024-06 (15 cortes), mais o **t0 de decisão 2024-12** (sem label, usado no ranking). |
| **Janela de observação** | [t0 − 11 meses, t0]: 12 competências, todas **≤ t0**. |
| **Janela de predição** | (t0, t0 + 6 meses]: 6 competências, todas **> t0**. |
| **Split principal (temporal)** | **Treino:** t0 de 2020-12 a 2022-12 (9 cortes; 1.002 obs.; 119 municípios; prevalência de **7,4%**). **Folga:** t0 2023-03 (120 obs.; fora do treino e do teste). **Teste:** t0 de 2023-06 a 2024-06 (5 cortes; 629 obs.; 127 municípios; prevalência de **8,1%**). A última label do treino termina em **2023-06**, que é ≤ o 1º t0 de teste. |
| **Split complementar (temporal + grupo)** | Mesmos cortes. **25 municípios (20%)**, sorteados com `SEED=42`, ficam **fora do treino** e só aparecem no teste. Treino: 815 obs. de 96 municípios. Teste: 123 obs. de 25 municípios. Interseção vazia. |
| **Por que temporal + grupo** | A decisão é sempre sobre o **futuro** (temporal), e o mesmo município aparece em vários t0. O cenário por grupo mede a generalização para municípios nunca vistos. |
| **Validação (escolha de modelo e de limiar)** | Previsões fora da amostra nos **4 últimos t0 do treino**: cada um é previsto por um modelo ajustado só com observações cuja label terminou até ele (`label_fim ≤ t0_val`, folga de 6 meses). O **modelo é escolhido pela AP de validação** e o **limiar, pelo custo de validação**. O teste é usado uma única vez, para reportar. |
| **Baselines** | (1) **Prevalência**: score constante = taxa de positivos do treino. (2) **Nível atual**: score = `razao_provisao_t0`. (3) **Tendência recente**: score = `razao_provisao_tendencia_6m`. |
| **Métrica** | **Average Precision (AP, área sob a curva precisão-recall)** como principal. Com prevalência de cerca de 8%, a ROC-AUC fica otimista, porque a classe negativa domina. A AP mede a qualidade da fila de prioridade, e a AP do baseline de prevalência (≈ 0,08) é a referência mínima. Também são reportados ROC-AUC, Brier (calibração), intervalo de confiança de 95% da AP por bootstrap (seed fixa) e precisão e recall no limiar de custo. |

## 2. Features (todas calculadas na janela [t0−11, t0])

| Grupo | Features | Origem |
|---|---|---|
| Risco observado | `razao_provisao_t0`, média, desvio e máximo em 12 meses, tendência em 6 meses | ESTBAN |
| Carteira | log da carteira, crescimento na janela, participação de rural, imobiliário e empréstimos, crédito/depósitos, nº de instituições | ESTBAN |
| **Riqueza** | `log_pib_per_capita` e composição do VAB (agro, indústria, administração pública) **conhecidos em t0**, `log_populacao` | IBGE (banco de origem e SIDRA) |
| Ambiente | focos de calor em 12 meses por 10 mil habitantes, fração dos focos nos últimos 6 meses | INPE |
| Contexto regional | inadimplência e ativo problemático do SCR-PA em t0, tendência em 6 meses | SCR (BCB) |

**PIB sem informação futura:** o PIB do ano Y é publicado em dez/Y+2. Em t0 = 2023-06, o PIB usado é o de **2020**.
O VAB setorial de 2022–2023 não foi divulgado, então vale o ano mais recente já publicado (`join_asof` para trás).

Colunas **que não são features** e ficam fora do vetor X (`NAO_FEATURES` em `dataset.py`): `deterioracao` (label),
`razao_provisao_futura_media`, `alto_risco_futuro`, `label_inicio`, `label_fim`, `limiar_alto_risco`, `split`,
`grupo_holdout` e `meses_com_label`. `carteira_t0` e `pib_per_capita_t0` servem só para monetizar a decisão e para a
análise por faixas.

## 3. Checklist anti-vazamento (respondido item a item, com prova)

| # | Pergunta | Resposta | Evidência (executável: `pytest`) |
|---|---|---|---|
| 1 | Toda feature existia antes do t0? | **Sim.** As features só leem competências ≤ t0, e a riqueza usa o PIB já publicado em t0. | `tests/test_ml_dataset.py::test_features_nao_usam_dado_posterior_a_t0` (corromper **todo** dado > t0 não muda nenhuma feature); `tests/test_gold.py::test_pib_conhecido_na_data_respeita_defasagem`; o contrato Gold `data_max_feature <= t0` bloqueia a gravação se violado. |
| 2 | As agregações foram calculadas apenas com dados anteriores ao t0 de cada observação? | **Sim.** Média, desvio, máximo, tendência e somas de focos são calculados por t0, filtrando [t0−11, t0]. O limiar de alto risco usa só dados até o fim da janela de label do treino. | `test_features_nao_usam_dado_posterior_a_t0`; `test_limiar_do_label_usa_somente_periodo_de_treino`; `test_label_usa_somente_janela_de_predicao`; contrato Gold `label_inicio > t0`. |
| 3 | O split respeita tempo e grupo? Nenhuma entidade aparece em treino e teste ao mesmo tempo? | **Tempo: sim** (última label do treino ≤ 1º t0 do teste, com 1 trimestre de folga; a validação interna também tem folga). **Grupo:** no cenário temporal + grupo, a interseção de municípios é **vazia**. No cenário só temporal, o mesmo município aparece em períodos distintos, e isso é declarado: a decisão real é sobre os mesmos municípios no futuro. | `tests/test_ml_dataset.py::test_split_temporal_sem_sobreposicao_de_janelas`; `test_split_por_grupo_sem_municipio_em_treino_e_teste`; `test_todas_as_linhas_respeitam_t0`; `tests/test_ml_model.py::test_validacao_oof_respeita_folga_temporal`. |
| 4 | Scalers, encoders e imputadores foram ajustados (fit) somente no treino? | **Sim.** `SimpleImputer` e `StandardScaler` ficam dentro de um `sklearn.Pipeline`, que recebe `fit` só com X de treino e depois `predict` no teste. Não há encoder (todas as features são numéricas). | `tests/test_ml_model.py::test_imputador_e_scaler_ajustados_somente_no_treino` (as estatísticas aprendidas são iguais às do treino e diferentes das de treino + teste, e não mudam ao prever o teste). |
| 5 | (extra) Label ou target entraram como feature? | **Não.** | `tests/test_ml_dataset.py::test_label_e_target_nao_sao_features`. |
| 6 | (extra) Resultado reprodutível? | **Sim.** A seed é fixa e a ordem, determinística. | `test_label_reproduzivel_e_limiar_congelado`; `tests/test_ml_model.py::test_treino_reprodutivel`. |

**Métrica alta demais:** se a AP de teste passar de 0,95 (`alerta_ap_suspeita`), o relatório do modelo emite um alerta
e lista as features mais importantes. Foi esse mecanismo que revelou a trivialidade da label de nível (seção 0). Com a
label de deterioração, a AP de teste do modelo é **0,204** (IC95 de 0,140 a 0,309), contra 0,081 da prevalência, 0,146
do nível atual e 0,134 da tendência. O desempenho é modesto e plausível. Detalhes em `reports/ml_resultados.md` e
`docs/decisao.md`.
