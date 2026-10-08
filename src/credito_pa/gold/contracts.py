from __future__ import annotations

import polars as pl

from credito_pa.quality.contracts import Col, TableContract
from credito_pa.silver.contracts import COD_IBGE_DOMAIN, COD_IBGE_PA


def _cod() -> Col:
    return Col("cod_ibge_municipio", pl.Utf8, "Código IBGE do município", "silver.dim_municipio", COD_IBGE_DOMAIN,
               check=COD_IBGE_PA)


def _ratio(name: str, desc: str, origem: str, nullable: bool = True) -> Col:
    return Col(name, pl.Float64, desc, origem, "[0, 1]", nullable=nullable, check=pl.col(name).is_between(0, 1))


def _money(name: str, desc: str, origem: str) -> Col:
    return Col(name, pl.Float64, desc, origem, ">= 0 (R$); nulo se o município não tem agência no mês", nullable=True,
               check=pl.col(name) >= 0)


FATO_CREDITO_MUNICIPIO_MES = TableContract(
    name="fato_credito_municipio_mes", layer="gold",
    granularity="Uma linha por município do Pará por mês de competência (2019-01 a 2024-12).",
    primary_key=["cod_ibge_municipio", "data_competencia"],
    description="Painel mensal de crédito bancário, risco (provisão/carteira), riqueza conhecida na data, "
                "pressão ambiental e contexto regional do SCR. Base dos indicadores e da base ML-Ready.",
    columns=[
        _cod(),
        Col("data_competencia", pl.Date, "Mês de competência (último dia do mês)", "silver.estban (grade completa)",
            "2019-01-31 a 2024-12-31"),
        Col("possui_dado_estban", pl.Boolean, "True se há balancete ESTBAN do município no mês (há agência)",
            "silver.estban_municipio_instituicao_mes", "true | false"),
        Col("n_instituicoes", pl.Int32, "Instituições financeiras com agência no município", "ESTBAN: CNPJ distintos",
            ">= 1; nulo sem agência", nullable=True, check=pl.col("n_instituicoes") >= 1),
        Col("agencias", pl.Int32, "Agências com balancete processado", "ESTBAN: AGEN_PROCESSADAS (soma)",
            ">= 0; nulo sem agência", nullable=True, check=pl.col("agencias") >= 0),
        _money("carteira_credito_reais", "Saldo da carteira de crédito (R$)", "ESTBAN verbete 160 (soma das instituições)"),
        _money("provisao_reais", "Provisão para perdas com crédito (R$, positivo)", "-(ESTBAN verbete 174)"),
        _ratio("razao_provisao", "INDICADOR DE RISCO: provisão / carteira de crédito",
               "provisao_reais / carteira_credito_reais (nulo se carteira = 0 ou sem agência)"),
        _money("credito_rural_reais", "Crédito rural e agroindustrial (R$)", "ESTBAN verbetes 163 + 167"),
        _money("credito_imobiliario_reais", "Financiamentos imobiliários (R$)", "ESTBAN verbete 169"),
        _money("emprestimos_reais", "Empréstimos e títulos descontados (R$)", "ESTBAN verbete 161"),
        _money("depositos_reais", "Depósitos à vista, de poupança e a prazo (R$)", "ESTBAN verbetes 401..419 + 420 + 432"),
        Col("ano_ibge_referencia", pl.Int32, "Ano do PIB e da população conhecidos na data (defasagem de publicação)",
            "regra: ano-2 em dezembro, ano-3 nos demais meses", "<= ano(data_competencia) - 2"),
        Col("populacao_referencia", pl.Int64, "População do ano de referência", "silver.ibge_populacao_ano", "> 0",
            nullable=True, check=pl.col("populacao_referencia") > 0),
        Col("pib_per_capita_reais", pl.Float64, "PIB per capita do ano de referência (R$ correntes) = RIQUEZA",
            "silver.ibge_pib_ano", "> 0", nullable=True, check=pl.col("pib_per_capita_reais") > 0),
        Col("ano_vab_referencia", pl.Int32, "Ano mais recente, até o de referência, com VAB setorial publicado",
            "silver.ibge_pib_ano", "<= ano_ibge_referencia", nullable=True),
        _ratio("part_vab_agropecuaria", "Participação da agropecuária no VAB", "silver.ibge_pib_ano"),
        _ratio("part_vab_industria", "Participação da indústria no VAB", "silver.ibge_pib_ano"),
        _ratio("part_vab_adm_publica", "Participação da administração pública no VAB (dependência de renda pública)",
               "silver.ibge_pib_ano"),
        Col("focos_calor_mes", pl.Int64, "Focos de calor detectados no mês", "silver.inpe_focos_pa (contagem)", ">= 0",
            check=pl.col("focos_calor_mes") >= 0),
        _ratio("scr_inadimplencia_pa", "Contexto: carteira inadimplida / carteira ativa no PA (SCR)",
               "gold.contexto_scr_pa_mes (nulo antes de 2020-01)"),
        _ratio("scr_ativo_problematico_pa", "Contexto: ativo problemático / carteira ativa no PA (SCR)",
               "gold.contexto_scr_pa_mes (nulo antes de 2020-01)"),
    ],
    notes=["Grade completa (144 municípios x 72 meses). Município-mês sem agência fica com possui_dado_estban = false "
           "e medidas de crédito nulas; ausência de agência não é carteira zero.",
           "Riqueza (PIB/população) é a CONHECIDA na data: o PIB do ano Y só é publicado em dez/Y+2. Isso evita usar "
           "informação futura."],
)

CONTEXTO_SCR_PA_MES = TableContract(
    name="contexto_scr_pa_mes", layer="gold",
    granularity="Uma linha por mês de competência com indicadores agregados do SCR para o PA.",
    primary_key=["data_competencia"],
    description="Indicadores regionais de crédito do SCR.data (BCB) para o Pará; contexto macro-regional comum a "
                "todos os municípios no mês.",
    columns=[
        Col("data_competencia", pl.Date, "Mês de competência", "silver.scr_pa_mes", "2020-01-31 a 2024-12-31"),
        Col("carteira_ativa_pa", pl.Float64, "Carteira ativa total no PA (R$)", "Σ carteira_ativa", "> 0",
            check=pl.col("carteira_ativa_pa") > 0),
        _ratio("inadimplencia_pa", "Carteira inadimplida (atraso > 90 dias, arrastada) / carteira ativa",
               "Σ carteira_inadimplida_arrastada / Σ carteira_ativa", nullable=False),
        _ratio("ativo_problematico_pa", "Ativo problemático / carteira ativa", "Σ ativo_problematico / Σ carteira_ativa",
               nullable=False),
        _ratio("inadimplencia_pf_pa", "Inadimplência de pessoas físicas", "recorte cliente = PF", nullable=False),
        _ratio("inadimplencia_pj_pa", "Inadimplência de pessoas jurídicas", "recorte cliente = PJ", nullable=False),
        _ratio("inadimplencia_rural_pa", "Inadimplência do crédito rural e agroindustrial (PF + PJ)",
               "recorte modalidade contém 'Rural'", nullable=False),
    ],
)

_FEATURE_DESC = {
    "razao_provisao_t0": ("razão provisão/carteira em t0", "fato em t0"),
    "razao_provisao_media_12m": ("média da razão nos 12 meses da janela", "fato [t0-11, t0]"),
    "razao_provisao_desvio_12m": ("desvio-padrão da razão na janela", "fato [t0-11, t0]"),
    "razao_provisao_max_12m": ("máximo da razão na janela", "fato [t0-11, t0]"),
    "razao_provisao_tendencia_6m": ("razão em t0 menos razão em t0-6", "fato em t0 e t0-6"),
    "log_carteira_t0": ("log(1 + carteira de crédito em t0)", "fato em t0"),
    "crescimento_carteira_janela": ("carteira em t0 / carteira em t0-11, menos 1", "fato em t0 e t0-11"),
    "part_credito_rural_t0": ("crédito rural / carteira em t0", "fato em t0"),
    "part_credito_imobiliario_t0": ("crédito imobiliário / carteira em t0", "fato em t0"),
    "part_emprestimos_t0": ("empréstimos / carteira em t0", "fato em t0"),
    "razao_credito_depositos_t0": ("carteira / depósitos em t0 (alavancagem local)", "fato em t0"),
    "n_instituicoes_t0": ("nº de instituições com agência em t0 (concorrência bancária)", "fato em t0"),
    "log_pib_per_capita": ("log do PIB per capita conhecido em t0 (RIQUEZA)", "fato em t0 (ano de referência defasado)"),
    "part_vab_agropecuaria": ("participação da agropecuária no VAB conhecido em t0", "fato em t0"),
    "part_vab_industria": ("participação da indústria no VAB conhecido em t0", "fato em t0"),
    "part_vab_adm_publica": ("participação da adm. pública no VAB conhecido em t0", "fato em t0"),
    "log_populacao": ("log da população do ano de referência", "fato em t0"),
    "focos_12m_por_10mil_hab": ("focos de calor na janela por 10 mil habitantes", "fato [t0-11, t0]"),
    "part_focos_ultimos_6m": ("fração dos focos da janela ocorrida nos últimos 6 meses", "fato [t0-11, t0]"),
    "scr_inadimplencia_t0": ("inadimplência SCR-PA em t0 (contexto regional)", "contexto SCR em t0"),
    "scr_ativo_problematico_t0": ("ativo problemático SCR-PA em t0", "contexto SCR em t0"),
    "scr_inadimplencia_tendencia_6m": ("variação da inadimplência SCR-PA em 6 meses", "contexto SCR em t0 e t0-6"),
}

ML_BASE_RISCO_CREDITO = TableContract(
    name="ml_base_risco_credito", layer="gold",
    granularity="Uma linha por município elegível por ponto de corte trimestral t0.",
    primary_key=["cod_ibge_municipio", "t0"],
    description="Base ML-Ready: features só da janela de observação [t0-11, t0] e label só da janela de "
                "predição (t0, t0+6]. Ver docs/ml_ready.md.",
    columns=[
        _cod(),
        Col("t0", pl.Date, "Ponto de corte: instante em que o conhecimento do modelo é congelado",
            "fim de trimestre 2020-12..2024-06, mais o t0 de decisão 2024-12", "fim de trimestre"),
        Col("split", pl.Utf8, "Conjunto da observação no split temporal", "regra ML",
            "treino | folga | teste | decisao", check=pl.col("split").is_in(["treino", "folga", "teste", "decisao"])),
        Col("grupo_holdout", pl.Boolean, "Município sorteado (seed) para o cenário temporal + grupo (só no teste)",
            "regra ML", "true | false"),
        Col("data_min_feature", pl.Date, "Competência mais antiga usada nas features", "auditoria", "= t0 - 11 meses"),
        Col("data_max_feature", pl.Date, "Competência mais recente usada nas features (ANTI-VAZAMENTO: <= t0)",
            "auditoria", "<= t0", check=pl.col("data_max_feature") <= pl.col("t0")),
        Col("carteira_t0", pl.Float64, "Carteira de crédito em t0 (R$); não é feature, serve para monetizar a decisão",
            "fato em t0", "> 0", check=pl.col("carteira_t0") > 0),
        Col("pib_per_capita_t0", pl.Float64, "PIB per capita conhecido em t0 (R$); para análise por faixas",
            "fato em t0", "> 0", nullable=True, check=pl.col("pib_per_capita_t0") > 0),
        *[Col(f, pl.Float64, d, o, "real; nulo permitido (imputado só com estatísticas do treino)", nullable=True)
          for f, (d, o) in _FEATURE_DESC.items()],
        Col("label_inicio", pl.Date, "1º mês da janela de predição (ANTI-VAZAMENTO: > t0)", "auditoria",
            "> t0; nulo no split decisao", nullable=True, check=pl.col("label_inicio") > pl.col("t0")),
        Col("label_fim", pl.Date, "Último mês da janela de predição (= t0 + 6 meses)", "auditoria",
            "nulo no split decisao", nullable=True),
        Col("razao_provisao_futura_media", pl.Float64, "Média da razão provisão/carteira em (t0, t0+6]: base do "
            "EVENTO (NÃO é feature)", "fato (t0, t0+6]", "[0, 1]; nulo no split decisao", nullable=True,
            check=pl.col("razao_provisao_futura_media").is_between(0, 1)),
        Col("deterioracao", pl.Int8, "LABEL (trilho concessão): 1 se razao_provisao_futura_media >= 1,25 x "
            "razao_provisao_t0 E >= razao_provisao_t0 + 0,5 p.p.", "regra de rotulagem (config [ml])",
            "0 | 1; nulo no split decisao", nullable=True, check=pl.col("deterioracao").is_in([0, 1])),
        Col("alto_risco_atual", pl.Int8, "Trilho renegociação: 1 se razao_provisao_t0 >= limiar_alto_risco "
            "(conhecido em t0)", "regra de negócio", "0 | 1", check=pl.col("alto_risco_atual").is_in([0, 1])),
        Col("alto_risco_futuro", pl.Int8, "1 se razao_provisao_futura_media >= limiar_alto_risco; só para AVALIAR a "
            "regra de renegociação (NÃO é feature)", "fato (t0, t0+6]", "0 | 1; nulo no split decisao",
            nullable=True, check=pl.col("alto_risco_futuro").is_in([0, 1])),
        Col("limiar_alto_risco", pl.Float64, "P75 da razão provisão/carteira município-mês até 2023-06 (congelado)",
            "data/gold/ml_artifacts/limiar_alto_risco.json", "[0, 1]",
            check=pl.col("limiar_alto_risco").is_between(0, 1)),
    ],
    notes=["Coorte: carteira > 0 nos 12 meses da janela, carteira em t0 >= R$ 5 mi e 6 meses de label completos.",
           "Os t0 entre o fim do treino e o início do teste ficam no split 'folga' e não entram em treino nem em teste.",
           "Ajuste aprovado no apply: a label de nível (>= P75 no futuro) era trivial (persistência AP 0,974). A label "
           "do modelo passou a ser a deterioração material; o nível atual alimenta o trilho de renegociação por regra."],
)

ACOES = ["renegociar_e_restringir", "renegociar_priorizar", "restringir_credito_sem_garantia", "manter"]

RANKING_PRIORIZACAO_MUNICIPIOS = TableContract(
    name="ranking_priorizacao_municipios", layer="gold",
    granularity="Uma linha por município do Pará elegível no t0 de decisão (2024-12).",
    primary_key=["cod_ibge_municipio"],
    description="Entrega de decisão em dois trilhos para os próximos 6 meses (jan-jun/2025): renegociação (regra de "
                "risco atual) e restrição de crédito sem garantia (alerta de deterioração do modelo).",
    columns=[
        _cod(),
        Col("nome_municipio", pl.Utf8, "Nome do município", "silver.dim_municipio"),
        Col("mesorregiao", pl.Utf8, "Mesorregião", "silver.dim_municipio"),
        Col("t0", pl.Date, "Data de decisão (conhecimento congelado)", "config [ml] t0_decisao", "2024-12-31"),
        Col("ordem_prioridade", pl.Int32, "Posição na fila de ação (1 = mais urgente)", "regra de priorização", ">= 1",
            check=pl.col("ordem_prioridade") >= 1),
        Col("acao_recomendada", pl.Utf8, "Ação para a diretoria de crédito", "regra de decisão", " | ".join(ACOES),
            check=pl.col("acao_recomendada").is_in(ACOES)),
        Col("carteira_credito_reais", pl.Float64, "Carteira de crédito em t0 (R$)", "ESTBAN", "> 0",
            check=pl.col("carteira_credito_reais") > 0),
        Col("exposicao_sem_garantia_reais", pl.Float64, "Exposição estimada em linhas sem garantia (R$)",
            "carteira x fracao_carteira_exposta", ">= 0", check=pl.col("exposicao_sem_garantia_reais") >= 0),
        Col("pib_per_capita_reais", pl.Float64, "PIB per capita conhecido em t0 (R$)", "IBGE", "> 0", nullable=True,
            check=pl.col("pib_per_capita_reais") > 0),
        Col("razao_provisao_t0", pl.Float64, "Risco atual: provisão / carteira em t0", "ESTBAN", "[0, 1]",
            check=pl.col("razao_provisao_t0").is_between(0, 1)),
        Col("razao_provisao_tendencia_6m", pl.Float64, "Variação do risco nos últimos 6 meses", "ESTBAN", "real",
            nullable=True),
        Col("alto_risco_atual", pl.Int8, "Trilho renegociação: risco atual >= limiar P75", "regra", "0 | 1",
            check=pl.col("alto_risco_atual").is_in([0, 1])),
        Col("prob_deterioracao", pl.Float64, "Probabilidade de deterioração material em 6 meses (modelo)",
            "data/gold/ml_artifacts/modelo_final.joblib", "[0, 1]", check=pl.col("prob_deterioracao").is_between(0, 1)),
        Col("alerta_deterioracao", pl.Int8, "Trilho concessão: prob_deterioracao >= limiar de custo", "regra", "0 | 1",
            check=pl.col("alerta_deterioracao").is_in([0, 1])),
        Col("ganho_esperado_restricao_reais", pl.Float64, "Valor esperado de restringir as linhas sem garantia: "
            "p x C_FN x exposição - (1-p) x C_FP x exposição (R$)", "custos [custos]", "real"),
    ],
)

GOLD_CONTRACTS = [FATO_CREDITO_MUNICIPIO_MES, CONTEXTO_SCR_PA_MES, ML_BASE_RISCO_CREDITO, RANKING_PRIORIZACAO_MUNICIPIOS]
