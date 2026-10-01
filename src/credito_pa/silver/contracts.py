from __future__ import annotations

import polars as pl

from credito_pa.quality.contracts import Col, TableContract

COD_IBGE_PA = pl.col("cod_ibge_municipio").str.contains(r"^15\d{5}$")
COD_IBGE_DOMAIN = "7 dígitos, iniciando por 15 (UF Pará)"


def _cod_ibge(origem: str) -> Col:
    return Col("cod_ibge_municipio", pl.Utf8, "Código IBGE do município (chave de cruzamento entre todas as bases)",
               origem, COD_IBGE_DOMAIN, check=COD_IBGE_PA)


def _comp(origem: str) -> Col:
    return Col("data_competencia", pl.Date, "Mês de referência, padronizado para o último dia do mês", origem,
               "último dia do mês, 2019-01-31 em diante",
               check=pl.col("data_competencia") == pl.col("data_competencia").dt.month_end())


DIM_MUNICIPIO = TableContract(
    name="dim_municipio", layer="silver",
    granularity="Uma linha por município do Pará.",
    primary_key=["cod_ibge_municipio"],
    description="Cadastro de municípios do PA com hierarquia regional (IBGE Localidades).",
    columns=[
        _cod_ibge("IBGE Localidades: id"),
        Col("nome_municipio", pl.Utf8, "Nome oficial do município", "IBGE Localidades: nome", "texto não vazio",
            check=pl.col("nome_municipio").str.len_chars() > 0),
        Col("nome_normalizado", pl.Utf8, "Nome sem acento, em caixa alta e sem pontuação (chave de casamento por nome)",
            "derivado de nome_municipio", "A-Z, 0-9 e espaço"),
        Col("microrregiao", pl.Utf8, "Microrregião geográfica", "IBGE Localidades: microrregiao.nome"),
        Col("mesorregiao", pl.Utf8, "Mesorregião geográfica", "IBGE Localidades: microrregiao.mesorregiao.nome"),
        Col("regiao_imediata", pl.Utf8, "Região geográfica imediata", "IBGE Localidades: regiao-imediata.nome"),
        Col("regiao_intermediaria", pl.Utf8, "Região geográfica intermediária",
            "IBGE Localidades: regiao-imediata.regiao-intermediaria.nome"),
        Col("uf_sigla", pl.Utf8, "Sigla da UF", "IBGE Localidades", "PA", check=pl.col("uf_sigla") == "PA"),
    ],
)

IBGE_POPULACAO_ANO = TableContract(
    name="ibge_populacao_ano", layer="silver",
    granularity="Uma linha por município do Pará por ano.",
    primary_key=["cod_ibge_municipio", "ano"],
    description="População residente: estimativas anuais (SIDRA 6579), Censo 2022 (SIDRA 4709) e 2023 interpolado.",
    columns=[
        _cod_ibge("SIDRA: D1C"),
        Col("ano", pl.Int32, "Ano de referência da população", "SIDRA: D3C", "2018 a 2025",
            check=pl.col("ano").is_between(2000, 2030)),
        Col("populacao", pl.Int64, "Habitantes", "SIDRA: V (6579/9324 ou 4709/93); 2023 interpolado", "> 0",
            check=pl.col("populacao") > 0),
        Col("fonte_populacao", pl.Utf8, "Origem do valor de população", "regra Silver",
            "estimativa | censo | interpolada", check=pl.col("fonte_populacao").is_in(["estimativa", "censo", "interpolada"])),
    ],
    notes=["2023 não tem estimativa nem censo no SIDRA: valor = média de 2022 (Censo) e 2024 (estimativa), "
           "arredondada, marcado como 'interpolada'."],
)

_VAB_NOTE = "nulo em 2022–2023 (VAB setorial municipal não divulgado no SIDRA 5938 para esses anos)"

IBGE_PIB_ANO = TableContract(
    name="ibge_pib_ano", layer="silver",
    granularity="Uma linha por município do Pará por ano de referência do PIB.",
    primary_key=["cod_ibge_municipio", "ano"],
    description="PIB municipal e VAB por setor (mil R$ correntes), vindos do banco relacional de origem (SIDRA 5938), com PIB per capita.",
    columns=[
        _cod_ibge("banco de origem pib_municipal.cod_municipio"),
        Col("ano", pl.Int32, "Ano de referência do PIB (não é o ano de publicação)", "pib_municipal.ano", "2018 a 2023",
            check=pl.col("ano").is_between(2000, 2030)),
        Col("pib_mil_reais", pl.Float64, "PIB a preços correntes (mil R$)", "variável 37", "> 0",
            check=pl.col("pib_mil_reais") > 0),
        Col("vab_total_mil_reais", pl.Float64, "VAB total (mil R$)", "variável 498", f">= 0; {_VAB_NOTE}",
            nullable=True, check=pl.col("vab_total_mil_reais") >= 0),
        Col("vab_agropecuaria_mil_reais", pl.Float64, "VAB da agropecuária (mil R$)", "variável 513", f">= 0; {_VAB_NOTE}",
            nullable=True, check=pl.col("vab_agropecuaria_mil_reais") >= 0),
        Col("vab_industria_mil_reais", pl.Float64, "VAB da indústria (mil R$)", "variável 517", f">= 0; {_VAB_NOTE}",
            nullable=True, check=pl.col("vab_industria_mil_reais") >= 0),
        Col("vab_servicos_mil_reais", pl.Float64, "VAB de serviços, exceto administração pública (mil R$)", "variável 6575",
            f">= 0; {_VAB_NOTE}", nullable=True, check=pl.col("vab_servicos_mil_reais") >= 0),
        Col("vab_adm_publica_mil_reais", pl.Float64, "VAB da administração pública (mil R$)", "variável 525",
            f">= 0; {_VAB_NOTE}", nullable=True, check=pl.col("vab_adm_publica_mil_reais") >= 0),
        Col("populacao", pl.Int64, "População do mesmo ano (para o per capita)", "silver.ibge_populacao_ano", "> 0",
            nullable=True, check=pl.col("populacao") > 0),
        Col("pib_per_capita_reais", pl.Float64, "PIB per capita em R$ correntes = pib_mil_reais*1000/populacao",
            "derivado", "> 0; nulo se não houver população do ano", nullable=True, check=pl.col("pib_per_capita_reais") > 0),
        Col("updated_at_origem", pl.Datetime("us"), "Última atualização da linha no sistema de origem",
            "pib_municipal.updated_at (máximo entre variáveis)"),
    ],
)

ESTBAN_VERBETES = {
    "verbete_160": "Operações de crédito (saldo bruto da carteira, R$)",
    "verbete_161": "Empréstimos e títulos descontados (R$)",
    "verbete_162": "Financiamentos (R$)",
    "verbete_163": "Financiamentos rurais e agrícolas: custeio e investimento (R$)",
    "verbete_167": "Financiamentos agroindustriais (R$)",
    "verbete_169": "Financiamentos imobiliários (R$)",
    "verbete_171": "Outras operações de crédito (R$)",
    "verbete_172": "Outros créditos (R$)",
    "verbete_174": "Provisão para operações de crédito (conta redutora, valor <= 0, R$)",
    "verbete_399": "Total do ativo (R$)",
    "verbete_420": "Depósitos de poupança (R$)",
    "verbete_432": "Depósitos a prazo (R$)",
    "verbete_401_419": "Depósitos à vista e demais depósitos, contas 401 a 419 somadas pelo BCB (R$)",
}

ESTBAN_MUNICIPIO_INSTITUICAO_MES = TableContract(
    name="estban_municipio_instituicao_mes", layer="silver",
    granularity="Uma linha por município do Pará por instituição financeira (CNPJ raiz) por mês de competência.",
    primary_key=["cod_ibge_municipio", "cnpj_raiz", "data_competencia"],
    description="Balancete das agências bancárias agregadas por município e instituição (BCB ESTBAN, Documento 4500).",
    columns=[
        _cod_ibge("ESTBAN: CODMUN_IBGE"),
        Col("cnpj_raiz", pl.Utf8, "CNPJ raiz (8 dígitos) da instituição", "ESTBAN: CNPJ", "8 dígitos",
            check=pl.col("cnpj_raiz").str.contains(r"^\d{8}$")),
        _comp("ESTBAN: #DATA_BASE (AAAAMM)"),
        Col("nome_instituicao", pl.Utf8, "Nome da instituição", "ESTBAN: NOME_INSTITUICAO"),
        Col("codmun_bcb", pl.Utf8, "Código de município interno do BCB", "ESTBAN: CODMUN"),
        Col("agencias_processadas", pl.Int32, "Agências da instituição no município com balancete processado",
            "ESTBAN: AGEN_PROCESSADAS", ">= 0", check=pl.col("agencias_processadas") >= 0),
        *[
            Col(name, pl.Float64, desc, f"ESTBAN: {name.upper().replace('_401_419', '_401..419')}",
                "<= 0" if name == "verbete_174" else "valor monetário em R$",
                nullable=name not in ("verbete_160", "verbete_174"),
                check=(pl.col(name) <= 0) if name == "verbete_174" else None)
            for name, desc in ESTBAN_VERBETES.items()
        ],
    ],
    notes=["Colunas selecionadas pelo CÓDIGO do verbete (regex ^VERBETE_(\\d{3})), pois o nome descritivo muda entre anos.",
           "Crédito rural: até 2022-06 o BCB separava 163 a 166; 164 a 166 somam zero no PA em todo o período, então "
           "verbete_163 é contínuo.",
           "Linhas de agência esperada e não processada pelo BCB (AGEN_PROCESSADAS=0, valores vazios) vão para a "
           "quarentena com o motivo 'agencia_nao_processada'.",
           "Valores em R$ (inteiros no arquivo). Competências de 2025 em diante usam a Res. CMN 4.966 e não são "
           "comparáveis às anteriores; essa restrição é aplicada na Gold."],
)

SCR_DIMS = ["tcb", "sr", "cliente", "ocupacao", "cnae_secao", "cnae_subclasse", "porte", "modalidade", "origem", "indexador"]
SCR_VALORES = [
    "a_vencer_ate_90_dias", "a_vencer_de_91_ate_360_dias", "a_vencer_de_361_ate_1080_dias",
    "a_vencer_de_1081_ate_1800_dias", "a_vencer_de_1801_ate_5400_dias", "a_vencer_acima_de_5400_dias",
    "vencido_acima_de_15_dias", "carteira_ativa", "carteira_inadimplida_arrastada", "ativo_problematico",
]
_SCR_DIM_DESC = {
    "tcb": "Tipo de instituição (Bancário, Não bancário, Cooperativas)",
    "sr": "Segmento prudencial da instituição (S1 a S5; NAO_INFORMADO quando vazio na fonte)",
    "cliente": "Tipo de cliente (PF ou PJ)",
    "ocupacao": "Ocupação do tomador PF ('-' para PJ)",
    "cnae_secao": "Seção CNAE do tomador PJ ('-' para PF)",
    "cnae_subclasse": "Subclasse CNAE do tomador PJ ('-' para PF)",
    "porte": "Faixa de renda (PF) ou porte (PJ)",
    "modalidade": "Modalidade de crédito",
    "origem": "Origem dos recursos (com ou sem destinação específica)",
    "indexador": "Indexador da operação",
}

SCR_PA_MES = TableContract(
    name="scr_pa_mes", layer="silver",
    granularity="Uma linha por mês de competência por combinação de dimensões do SCR.data para o PA.",
    primary_key=["data_competencia", *SCR_DIMS],
    description="Carteira de crédito do SCR (BCB), agregada pelo próprio BCB por UF e dimensões, filtrada para o PA.",
    columns=[
        _comp("SCR.data: data_base"),
        *[Col(d, pl.Utf8, _SCR_DIM_DESC[d], f"SCR.data: {d}", "texto não vazio (valores do BCB)") for d in SCR_DIMS],
        Col("numero_de_operacoes", pl.Int64, "Quantidade de operações (nulo quando a fonte informa '<= 15')",
            "SCR.data: numero_de_operacoes", "> 0 ou nulo", nullable=True, check=pl.col("numero_de_operacoes") > 0),
        Col("operacoes_ate_15", pl.Boolean, "True quando a fonte omite a quantidade exata ('<= 15', sigilo)",
            "SCR.data: numero_de_operacoes", "true | false"),
        *[Col(v, pl.Float64, f"Saldo (R$): {v.replace('_', ' ')}", f"SCR.data: {v}", ">= 0", check=pl.col(v) >= 0)
          for v in SCR_VALORES],
        Col("layout_origem", pl.Utf8, "Layout do arquivo de origem", "regra Silver", "csv_oficial | parquet_local",
            check=pl.col("layout_origem").is_in(["csv_oficial", "parquet_local"])),
    ],
)

INPE_FOCOS_PA = TableContract(
    name="inpe_focos_pa", layer="silver",
    granularity="Uma linha por foco de calor detectado pelo satélite de referência no Pará.",
    primary_key=["foco_id"],
    description="Focos de calor (INPE Programa Queimadas), associados ao município IBGE pelo nome normalizado.",
    columns=[
        Col("foco_id", pl.Utf8, "Identificador do foco no INPE", "INPE: foco_id", "texto não vazio"),
        Col("data_hora_utc", pl.Datetime("us"), "Data e hora da passagem do satélite (UTC)", "INPE: data_pas",
            "2019-01-01 em diante"),
        _comp("derivado de data_hora_utc"),
        Col("latitude", pl.Float64, "Latitude", "INPE: lat", "[-10, 3] (caixa do PA)", check=pl.col("latitude").is_between(-10, 3)),
        Col("longitude", pl.Float64, "Longitude", "INPE: lon", "[-59, -46] (caixa do PA)",
            check=pl.col("longitude").is_between(-59, -46)),
        _cod_ibge("dim_municipio via nome normalizado de INPE: municipio"),
        Col("municipio_inpe", pl.Utf8, "Nome do município como veio do INPE", "INPE: municipio"),
        Col("bioma", pl.Utf8, "Bioma", "INPE: bioma", "Amazônia | Cerrado"),
    ],
)

SILVER_CONTRACTS = [
    DIM_MUNICIPIO, IBGE_POPULACAO_ANO, IBGE_PIB_ANO, ESTBAN_MUNICIPIO_INSTITUICAO_MES, SCR_PA_MES, INPE_FOCOS_PA,
]
