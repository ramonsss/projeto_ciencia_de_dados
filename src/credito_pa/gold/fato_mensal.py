from __future__ import annotations

import polars as pl

from credito_pa.config import Settings
from credito_pa.gold.base import month_grid, parse_month
from credito_pa.silver.base import read_silver


def ano_ibge_referencia(data_competencia: pl.Expr, defasagem_anos: int, mes_publicacao: int) -> pl.Expr:
    ano = data_competencia.dt.year()
    return (pl.when(data_competencia.dt.month() >= mes_publicacao).then(ano - defasagem_anos)
            .otherwise(ano - defasagem_anos - 1)).cast(pl.Int32)


def build_contexto_scr(settings: Settings) -> pl.DataFrame:
    scr = read_silver(settings, "scr_pa_mes")
    fim = parse_month(settings.section("analise")["competencia_fim_comparavel"])
    scr = scr.filter(pl.col("data_competencia") <= fim)

    def taxa(mask: pl.Expr | None = None) -> pl.Expr:
        num, den = pl.col("carteira_inadimplida_arrastada"), pl.col("carteira_ativa")
        if mask is not None:
            num, den = num.filter(mask), den.filter(mask)
        return num.sum() / den.sum()

    return scr.group_by("data_competencia").agg(
        pl.col("carteira_ativa").sum().alias("carteira_ativa_pa"),
        taxa().alias("inadimplencia_pa"),
        (pl.col("ativo_problematico").sum() / pl.col("carteira_ativa").sum()).alias("ativo_problematico_pa"),
        taxa(pl.col("cliente") == "PF").alias("inadimplencia_pf_pa"),
        taxa(pl.col("cliente") == "PJ").alias("inadimplencia_pj_pa"),
        taxa(pl.col("modalidade").str.contains("Rural")).alias("inadimplencia_rural_pa"),
    ).sort("data_competencia")


def build_fato(settings: Settings, contexto: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    analise, pib_cfg = settings.section("analise"), settings.section("pib")
    inicio, fim = analise["competencia_inicio"], analise["competencia_fim_comparavel"]
    dim = read_silver(settings, "dim_municipio")
    estban = read_silver(settings, "estban_municipio_instituicao_mes")
    excluidas = estban.filter(pl.col("data_competencia") > parse_month(fim))
    estban = estban.filter(pl.col("data_competencia").is_between(parse_month(inicio), parse_month(fim)))

    credito = estban.group_by("cod_ibge_municipio", "data_competencia").agg(
        pl.col("cnpj_raiz").n_unique().cast(pl.Int32).alias("n_instituicoes"),
        pl.col("agencias_processadas").sum().cast(pl.Int32).alias("agencias"),
        pl.col("verbete_160").sum().alias("carteira_credito_reais"),
        (-pl.col("verbete_174").sum()).alias("provisao_reais"),
        (pl.col("verbete_163").fill_null(0) + pl.col("verbete_167").fill_null(0)).sum().alias("credito_rural_reais"),
        pl.col("verbete_169").sum().alias("credito_imobiliario_reais"),
        pl.col("verbete_161").sum().alias("emprestimos_reais"),
        (pl.col("verbete_401_419").fill_null(0) + pl.col("verbete_420").fill_null(0) + pl.col("verbete_432").fill_null(0))
        .sum().alias("depositos_reais"),
    )
    grid = dim.select("cod_ibge_municipio").join(pl.DataFrame({"data_competencia": month_grid(inicio, fim)}), how="cross")
    fato = grid.join(credito, on=["cod_ibge_municipio", "data_competencia"], how="left").with_columns(
        pl.col("n_instituicoes").is_not_null().alias("possui_dado_estban"),
        pl.when(pl.col("carteira_credito_reais") > 0)
        .then(pl.col("provisao_reais") / pl.col("carteira_credito_reais")).otherwise(None).alias("razao_provisao"),
        ano_ibge_referencia(pl.col("data_competencia"), pib_cfg["defasagem_anos"], pib_cfg["mes_publicacao"])
        .alias("ano_ibge_referencia"),
    )

    pib = read_silver(settings, "ibge_pib_ano")
    fato = fato.join(
        pib.select("cod_ibge_municipio", pl.col("ano").alias("ano_ibge_referencia"),
                   pl.col("populacao").alias("populacao_referencia"), "pib_per_capita_reais"),
        on=["cod_ibge_municipio", "ano_ibge_referencia"], how="left",
    )
    vab = (pib.filter(pl.col("vab_total_mil_reais").is_not_null())
           .select("cod_ibge_municipio", pl.col("ano").alias("ano_vab_referencia"),
                   (pl.col("vab_agropecuaria_mil_reais") / pl.col("vab_total_mil_reais")).alias("part_vab_agropecuaria"),
                   (pl.col("vab_industria_mil_reais") / pl.col("vab_total_mil_reais")).alias("part_vab_industria"),
                   (pl.col("vab_adm_publica_mil_reais") / pl.col("vab_total_mil_reais")).alias("part_vab_adm_publica"))
           .sort("ano_vab_referencia"))
    fato = fato.sort("ano_ibge_referencia").join_asof(
        vab, left_on="ano_ibge_referencia", right_on="ano_vab_referencia", by="cod_ibge_municipio", strategy="backward",
        check_sortedness=False,
    )

    focos = read_silver(settings, "inpe_focos_pa").group_by("cod_ibge_municipio", "data_competencia").agg(
        pl.len().cast(pl.Int64).alias("focos_calor_mes"))
    fato = fato.join(focos, on=["cod_ibge_municipio", "data_competencia"], how="left").with_columns(
        pl.col("focos_calor_mes").fill_null(0))
    fato = fato.join(contexto.select("data_competencia", pl.col("inadimplencia_pa").alias("scr_inadimplencia_pa"),
                                     pl.col("ativo_problematico_pa").alias("scr_ativo_problematico_pa")),
                     on="data_competencia", how="left")
    info = {"competencias_excluidas_quebra_contabil": excluidas["data_competencia"].n_unique(),
            "linhas_estban_excluidas_quebra_contabil": excluidas.height}
    return fato, info
