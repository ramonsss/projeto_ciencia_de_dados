# Entrega de Decisão: Risco de Crédito Municipal no Pará

> Números da execução de 2026-10-01 (t0 de decisão = **2024-12-31**; horizonte = **jan a jun/2025**).
> Fonte de cada número: `reports/decisao.json`, `reports/ml_resultados.json` e `gold.ranking_priorizacao_municipios`.
> Para regenerar: `python scripts/run_pipeline.py --stage all`. O teste `tests/test_docs.py` confere estes números contra o JSON.

## 1. Frase-resposta do projeto

> Cruzando as bases **ESTBAN e SCR.data (Banco Central), PIB dos Municípios e População (IBGE) e focos de calor (INPE)**,
> identificamos que **42 dos 127 municípios elegíveis do Pará já estão em risco alto** (provisão ≥ 4,07% da carteira) e
> **concentram 72,1% das perdas já provisionadas do estado**. A riqueza **não explica o nível de risco** (Spearman entre
> PIB per capita e provisão = −0,04), embora o quintil mais pobre tenha a maior taxa de deterioração (12,0%, contra 4,0% a
> 8,3% nos demais), efeito que desaparece ao controlar pelo porte da carteira (odds ratio de 1,01).
> Recomendamos que **a diretoria de crédito das cooperativas e dos bancos regionais com atuação no Pará** faça **uma
> campanha de renegociação de dívidas nos 42 municípios de risco alto** e **mantenha por ora as linhas sem garantia sem
> restrição, com vigilância mensal dos 5 municípios de maior probabilidade de deterioração**, nos próximos **6 meses
> (jan a jun/2025)**, priorizando **Piçarra, Palestina do Pará, Ulianópolis, Água Azul do Norte e Eldorado do Carajás**.
> Se agir, o ganho esperado é **atacar 72,1% das perdas provisionadas atuando em 42 de 127 municípios, com uma regra que
> acerta 93,8% e cobre 91,0% dos municípios que seguirão em risco alto**. Se errarmos, o custo é **6,2% da campanha
> gasta em municípios que melhorariam sozinhos (falso positivo) e 9,0% dos municípios de risco alto fora da campanha
> (falso negativo)**, além de **R$ 3,7 mi de perda esperada residual** no crédito sem garantia não restringido.

A versão gerada automaticamente, com todos os campos, fica em `reports/decisao.md`.

## 2. Explicação linha a linha da frase

| Trecho | Valor | De onde vem (código) | Como interpretar |
|---|---|---|---|
| "42 dos 127 municípios elegíveis" | 42 / 127 | `ranking_priorizacao_municipios`: `alto_risco_atual = 1` / linhas no t0 2024-12 | São elegíveis os municípios com agência nos 12 meses e carteira ≥ R$ 5 mi. Os demais 17 não têm carteira estável para avaliação. |
| "provisão ≥ 4,07% da carteira" | P75 = 4,073% | `ml/dataset.py::label_threshold` (só dados até 2023-06) | "Alto risco" é estar no quarto superior do histórico do estado. |
| "72,1% das perdas já provisionadas" | 72,1% | `gold/ranking.py::build_decisao` (Σ razão × carteira dos 42 / total) | O foco em um terço dos municípios cobre quase três quartos da perda reconhecida. |
| "Spearman = −0,04" | −0,0435 | correlação de postos entre PIB per capita e provisão no t0 | Não há relação monotônica entre riqueza e risco atual. |
| "12,0% no quintil mais pobre, contra 4,0% a 8,3%" | quintis | `ml/decision.py::efeito_riqueza` | Os municípios mais pobres pioram com mais frequência (associação, não causa). |
| "odds ratio de 1,01" | 1,0105 | `ml/run.py::coeficiente_riqueza` (logística padronizada) | Com carteira, risco atual, setor e contexto no modelo, o PIB per capita não acrescenta informação. |
| "campanha de renegociação nos 42" | regra | `alto_risco_atual` (sem ML) | Trilho 1: a persistência do risco é tão forte que a regra simples basta e é auditável. |
| "linhas sem garantia sem restrição por ora" | limiar 44,4% | `ml/decision.py::escolher_limiar` (custo de validação) | Trilho 2: o maior score em dez/2024 (Trairão, 34,5%) não paga o custo de restringir. |
| "vigilância dos 5 de maior probabilidade" | Trairão, Tracuateua, Muaná, Bagre, Faro | `vigilancia_deterioracao` (fora do trilho 1) | São os candidatos a restrição se o score subir na próxima atualização mensal. |
| "priorizando Piçarra, ..." | top 5 | `ordem_prioridade` (risco atual, em ordem decrescente) | Piçarra tem provisão de 84,5% da carteira: caso mais urgente. |
| "acerta 93,8% e cobre 91,0%" | precisão e recall | `ml/run.py::avaliar_trilho_renegociacao` (teste 2023-06 a 2024-06) | De cada 100 municípios na campanha, cerca de 94 seguem em risco alto. De cada 100 que estarão em risco alto, 91 estão na campanha. |
| "6,2% ... 9,0%" | 1 − precisão; 1 − recall | idem | São o falso positivo e o falso negativo do trilho 1. |
| "R$ 3,7 mi de perda esperada residual" | R$ 3.680.113 | Σ p × C_FN × exposição dos não restringidos | Custo de não agir no trilho 2, se as probabilidades se confirmarem. |

## 3. Decisor, ação e prazo

- **Decisor:** diretoria/comitê de crédito de cooperativas de crédito (ex.: sistemas cooperativos com atuação no Norte)
  e bancos regionais (ex.: banco estadual) que operam no Pará.
- **Ação 1 (trilho renegociação):** campanha ativa de renegociação de dívidas, com contato, alongamento e
  repactuação, nos municípios com `acao_recomendada = renegociar_priorizar`, na ordem de `ordem_prioridade`.
- **Ação 2 (trilho concessão):** exigir garantia ou reduzir o limite de **novas** linhas sem garantia nos municípios com
  `alerta_deterioracao = 1`. Em dez/2024, **nenhum**. Vigilância mensal da lista de vigilância.
- **Prazo:** 6 meses (jan a jun/2025), igual à janela de predição. Reavaliação mensal quando sair cada novo ESTBAN.

## 4. Custos dos erros e limiar (trilho concessão)

| Erro | Significado prático | Custo por R$ exposto | Premissa |
|---|---|---:|---|
| **Falso negativo** | O município deteriorou e não foi restringido: "emprestar e tomar calote" | **C_FN = 4,50%** | LGD 45% × PD adicional 10% |
| **Falso positivo** | O município foi restringido e não deteriorou: "deixar de financiar produtor promissor" | **C_FP = 2,45%** | margem líquida 9% a.a. × 25% de bons clientes perdidos + campanha 0,2% |

- Exposição = 20% da carteira do município (linhas sem garantia no horizonte). Os parâmetros ficam em `config/pipeline.toml [custos]`.
- **Limiar teórico** = C_FP / (C_FP + C_FN) = **35,3%**: só vale restringir se a probabilidade de deterioração passar desse valor.
- **Limiar adotado = 44,4%**, escolhido minimizando o custo nas previsões fora da amostra da validação temporal (não no teste).
  Na curva de custo da validação, o mínimo fica em 44,4% (custo de 1,54 unidades, com 10 sinalizados). Em 36,2%, o custo
  é 1,76 (19 sinalizados), e não agir custa 1,58. Ou seja, os scores entre 35% e 44% ainda erraram demais na validação.
- **Ligação métrica → consequência:** com AP de 0,204 (cerca de 2,5× a prevalência), a precisão no topo da fila fica perto de
  33%, abaixo dos 35% necessários para compensar o custo. Por isso o modelo **ordena** bem (serve para vigilância), mas
  **raramente justifica** restringir crédito. No teste, sinalizou 3 municípios (1 acerto) e economizou **R$ 325.181**
  frente a não agir (R$ 18,28 mi contra R$ 18,60 mi).

## 5. Desempenho do modelo (alerta de deterioração)

| Abordagem | AP teste (temporal) | IC95 | AP teste (municípios não vistos) |
|---|---:|---|---:|
| Prevalência | 0,081 | n/a | 0,106 |
| Regra: nível atual | 0,146 | 0,085 a 0,252 | 0,213 |
| Regra: tendência 6 meses | 0,134 | 0,078 a 0,233 | 0,210 |
| Regressão logística | 0,191 | 0,125 a 0,292 | 0,383 |
| **Gradient boosting (escolhido na validação)** | **0,204** | **0,140 a 0,309** | 0,315 |

Variáveis mais importantes (queda de AP ao permutar): log da carteira (0,096), média de provisão em 12 meses (0,028),
participação do crédito imobiliário (0,015), log da população (0,012) e focos de calor por habitante (0,011).
A direção do efeito do porte foi conferida nas observações rotuladas: a taxa de deterioração é de **19,9%** no quartil de
menor carteira (mediana de R$ 10,4 mi) e de **0,5%** no de maior carteira (mediana de R$ 515 mi).

## 6. Resposta à pergunta central: "como a riqueza impacta o risco de crédito?"

1. **No nível de risco, quase nada.** As medianas de provisão por quintil de PIB per capita ficam entre 2,1% e 2,6%, sem
   ordem, e Spearman = −0,04.
2. **Na dinâmica, há associação.** O quintil mais pobre (PIB per capita até R$ 8.560) deteriora com mais frequência
   (12,0%, contra 4,0% a 8,3%).
3. **Mas a riqueza não é o mecanismo direto.** Com o porte e a composição da carteira no modelo, o efeito do PIB per
   capita some (odds ratio de 1,01). Municípios mais pobres têm carteiras menores (Spearman de 0,32 entre PIB per capita e
   carteira), e é o porte pequeno que antecipa a piora (19,9% de deterioração no quartil de menor carteira, contra 0,5% no maior).

**Implicação para o decisor:** PIB per capita **não** deve ser critério de concessão por si só. Isso evitaria, inclusive,
penalizar municípios pobres de baixo risco. O critério deve ser o risco acumulado da carteira local (trilho 1) e o
alerta do modelo (trilho 2).

## 7. Limitações (o que os dados não permitem afirmar)

| Limitação | Efeito | O que seria preciso |
|---|---|---|
| **Proxy contábil:** provisão/carteira (ESTBAN) não é inadimplência observada | Mede a perda esperada reconhecida pelos bancos, que tem defasagem e critérios próprios | Inadimplência por município (SCR com abertura municipal, não pública) |
| **Local de contabilização:** o ESTBAN registra o crédito na agência, não no domicílio do tomador | Municípios-polo podem concentrar operações de vizinhos | Microdados do SCR por domicílio (acesso restrito) |
| **Só bancos com agência:** cooperativas e fintechs sem agência local não aparecem no ESTBAN | A carteira de cooperativas do Norte está subrepresentada | Dados das próprias cooperativas (base interna do decisor) |
| **Quebra de 2025 (Res. CMN 4.966):** a provisão muda de critério | O modelo foi treinado até dez/2024. As probabilidades para 2025 assumem a regra contábil antiga | Recalibrar o limiar com cerca de 12 meses sob a 4.966 |
| **Amostra pequena** (≈ 127 municípios × 15 t0) | ICs largos (AP de 0,14 a 0,31) | Mais anos ou granularidade intra-municipal |
| **Associação, não causalidade** | "PIB per capita não importa" vale **condicionado** às demais variáveis | Desenho causal (ex.: choque exógeno de renda) |
| **Rótulos inspecionados no diagnóstico** | Métricas de teste levemente otimistas (ver `docs/ml_ready.md` §0) | Nova janela de teste (2025-H2) com regra contábil estável |
| **Custos são premissas** | O limiar e a economia em R$ dependem de LGD, PD e margem | Parâmetros reais da cooperativa parceira em `config/pipeline.toml` |
