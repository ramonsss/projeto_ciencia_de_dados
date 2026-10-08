from __future__ import annotations

import math
from datetime import date

import numpy as np
import polars as pl

from credito_pa.common.io import write_json_atomic
from credito_pa.config import Settings
from credito_pa.gold.base import parse_month, read_gold

FEATURES = [
    "razao_provisao_t0", "razao_provisao_media_12m", "razao_provisao_desvio_12m", "razao_provisao_max_12m",
    "razao_provisao_tendencia_6m",
    "log_carteira_t0", "crescimento_carteira_janela", "part_credito_rural_t0", "part_credito_imobiliario_t0",
    "part_emprestimos_t0", "razao_credito_depositos_t0", "n_instituicoes_t0",
    "log_pib_per_capita", "part_vab_agropecuaria", "part_vab_industria", "part_vab_adm_publica", "log_populacao",
    "focos_12m_por_10mil_hab", "part_focos_ultimos_6m",
    "scr_inadimplencia_t0", "scr_ativo_problematico_t0", "scr_inadimplencia_tendencia_6m",
]
LABEL = "deterioracao"
TARGET_CONTINUO = "razao_provisao_futura_media"
NAO_FEATURES = {LABEL, TARGET_CONTINUO, "alto_risco_futuro", "label_inicio", "label_fim", "split",
                "grupo_holdout", "limiar_alto_risco", "meses_com_label"}


def add_months(d: date, n: int) -> date:
    y, m = divmod(d.year * 12 + d.month - 1 + n, 12)
    return pl.select(pl.date(y, m + 1, 1).dt.month_end()).item()


def quarter_ends(inicio: str, fim: str) -> list[date]:
    t, end, out = parse_month(inicio), parse_month(fim), []
    while t <= end:
        out.append(t)
        t = add_months(t, 3)
    return out


def build_features(fato: pl.DataFrame, t0: date, w: int) -> pl.DataFrame:
    inicio = add_months(t0, -(w - 1))
    janela = fato.filter(pl.col("data_competencia").is_between(inicio, t0))
    t6 = add_months(t0, -6)
    em = lambda d: pl.col("data_competencia") == d
    ult6 = pl.col("data_competencia") > add_months(t0, -6)
    agg = janela.sort("data_competencia").group_by("cod_ibge_municipio").agg(
        pl.col("data_competencia").max().alias("data_max_feature"),
        pl.col("data_competencia").min().alias("data_min_feature"),
        ((pl.col("carteira_credito_reais") > 0).fill_null(False)).sum().alias("meses_com_carteira"),
        pl.col("carteira_credito_reais").filter(em(t0)).first().alias("carteira_t0"),
        pl.col("carteira_credito_reais").filter(em(inicio)).first().alias("carteira_inicio_janela"),
        pl.col("razao_provisao").filter(em(t0)).first().alias("razao_provisao_t0"),
        pl.col("razao_provisao").mean().alias("razao_provisao_media_12m"),
        pl.col("razao_provisao").std().alias("razao_provisao_desvio_12m"),
        pl.col("razao_provisao").max().alias("razao_provisao_max_12m"),
        pl.col("razao_provisao").filter(em(t6)).first().alias("razao_provisao_t0_menos_6"),
        (pl.col("credito_rural_reais") / pl.col("carteira_credito_reais")).filter(em(t0)).first().alias("part_credito_rural_t0"),
        (pl.col("credito_imobiliario_reais") / pl.col("carteira_credito_reais")).filter(em(t0)).first().alias("part_credito_imobiliario_t0"),
        (pl.col("emprestimos_reais") / pl.col("carteira_credito_reais")).filter(em(t0)).first().alias("part_emprestimos_t0"),
        (pl.col("carteira_credito_reais") / pl.col("depositos_reais")).filter(em(t0)).first().alias("razao_credito_depositos_t0"),
        pl.col("n_instituicoes").filter(em(t0)).first().cast(pl.Float64).alias("n_instituicoes_t0"),
        pl.col("pib_per_capita_reais").filter(em(t0)).first().alias("pib_per_capita_t0"),
        pl.col("populacao_referencia").filter(em(t0)).first().alias("populacao_t0"),
        pl.col("part_vab_agropecuaria").filter(em(t0)).first(),
        pl.col("part_vab_industria").filter(em(t0)).first(),
        pl.col("part_vab_adm_publica").filter(em(t0)).first(),
        pl.col("focos_calor_mes").sum().alias("focos_12m"),
        pl.col("focos_calor_mes").filter(ult6).sum().alias("focos_ult6m"),
        pl.col("scr_inadimplencia_pa").filter(em(t0)).first().alias("scr_inadimplencia_t0"),
        pl.col("scr_ativo_problematico_pa").filter(em(t0)).first().alias("scr_ativo_problematico_t0"),
        pl.col("scr_inadimplencia_pa").filter(em(t6)).first().alias("scr_inad_t0_menos_6"),
    )
    return agg.with_columns(
        pl.lit(t0).alias("t0"),
        (pl.col("razao_provisao_t0") - pl.col("razao_provisao_t0_menos_6")).alias("razao_provisao_tendencia_6m"),
        pl.col("carteira_t0").log1p().alias("log_carteira_t0"),
        (pl.col("carteira_t0") / pl.col("carteira_inicio_janela") - 1).alias("crescimento_carteira_janela"),
        pl.col("pib_per_capita_t0").log().alias("log_pib_per_capita"),
        pl.col("populacao_t0").cast(pl.Float64).log().alias("log_populacao"),
        (pl.col("focos_12m") / pl.col("populacao_t0") * 10_000).alias("focos_12m_por_10mil_hab"),
        pl.when(pl.col("focos_12m") > 0).then(pl.col("focos_ult6m") / pl.col("focos_12m")).otherwise(None)
        .alias("part_focos_ultimos_6m"),
        (pl.col("scr_inadimplencia_t0") - pl.col("scr_inad_t0_menos_6")).alias("scr_inadimplencia_tendencia_6m"),
    )


def build_label(fato: pl.DataFrame, t0: date, h: int) -> pl.DataFrame:
    fim = add_months(t0, h)
    futuro = fato.filter((pl.col("data_competencia") > t0) & (pl.col("data_competencia") <= fim))
    return futuro.group_by("cod_ibge_municipio").agg(
        pl.col("razao_provisao").mean().alias(TARGET_CONTINUO),
        pl.col("razao_provisao").is_not_null().sum().alias("meses_com_label"),
        pl.col("data_competencia").min().alias("label_inicio"),
        pl.col("data_competencia").max().alias("label_fim"),
    )


def deterioracao_expr(futura: pl.Expr, atual: pl.Expr, fator: float, minimo: float) -> pl.Expr:
    cond = (futura >= fator * atual) & (futura >= atual + minimo)
    return pl.when(futura.is_null() | atual.is_null()).then(None).otherwise(cond.cast(pl.Int8))


def label_threshold(fato: pl.DataFrame, ate: date, percentil: float, carteira_minima: float) -> dict:
    base = fato.filter((pl.col("data_competencia") <= ate) & (pl.col("carteira_credito_reais") >= carteira_minima)
                       & pl.col("razao_provisao").is_not_null())
    valor = base.select(pl.col("razao_provisao").quantile(percentil / 100, interpolation="linear")).item()
    return {"percentil": percentil, "valor": float(valor), "competencias_ate": ate.isoformat(),
            "n_municipio_mes": base.height, "carteira_minima_reais": carteira_minima}


def sample_holdout_municipios(municipios: list[str], fracao: float, seed: int) -> set[str]:
    rng = np.random.default_rng(seed)
    ordenados = sorted(municipios)
    n = max(1, math.floor(len(ordenados) * fracao))
    return set(rng.choice(ordenados, size=n, replace=False).tolist())


def build_ml_base(settings: Settings, fato: pl.DataFrame | None = None) -> tuple[pl.DataFrame, dict]:
    cfg = settings.section("ml")
    w, h = cfg["janela_observacao_meses"], cfg["janela_predicao_meses"]
    carteira_min = float(cfg["carteira_minima_reais"])
    treino_fim, teste_ini = parse_month(cfg["treino_t0_fim"]), parse_month(cfg["teste_t0_inicio"])
    t0_decisao = parse_month(cfg["t0_decisao"])
    fato = fato if fato is not None else read_gold(settings, "fato_credito_municipio_mes")

    fim_label_treino = add_months(treino_fim, h)
    if fim_label_treino > teste_ini:
        raise ValueError(f"Split inválido: labels do treino vão até {fim_label_treino} > 1º t0 de teste {teste_ini}")
    limiar = label_threshold(fato, fim_label_treino, cfg["percentil_alto_risco"], carteira_min)
    write_json_atomic(limiar, settings.layer_dir("gold") / "ml_artifacts" / "limiar_alto_risco.json")
    fator, minimo = cfg["deterioracao_fator_relativo"], cfg["deterioracao_minimo_absoluto"]

    t0s = quarter_ends(cfg["t0_inicio"], cfg["t0_fim"]) + [t0_decisao]
    n_municipios = fato["cod_ibge_municipio"].n_unique()
    partes, exclusoes = [], []
    for t0 in t0s:
        feats = build_features(fato, t0, w)
        decisao = t0 == t0_decisao
        if not decisao:
            feats = feats.join(build_label(fato, t0, h), on="cod_ibge_municipio", how="left")
        else:
            feats = feats.with_columns(pl.lit(None, dtype=pl.Float64).alias(TARGET_CONTINUO),
                                       pl.lit(None, dtype=pl.UInt32).alias("meses_com_label"),
                                       pl.lit(None, dtype=pl.Date).alias("label_inicio"),
                                       pl.lit(None, dtype=pl.Date).alias("label_fim"))
        motivo = (pl.when(pl.col("meses_com_carteira") < w).then(pl.lit("carteira_incompleta_na_janela"))
                  .when(pl.col("carteira_t0").fill_null(0) < carteira_min).then(pl.lit("carteira_abaixo_minimo")))
        if not decisao:
            motivo = motivo.when(pl.col("meses_com_label").fill_null(0) < h).then(pl.lit("janela_predicao_incompleta"))
        feats = feats.with_columns(motivo.otherwise(None).alias("motivo_exclusao"))
        fora = feats.filter(pl.col("motivo_exclusao").is_not_null())
        ausentes = n_municipios - feats.height
        exclusoes.append({"t0": t0.isoformat(), "elegiveis": feats.height - fora.height,
                          **{m: n for m, n in fora["motivo_exclusao"].value_counts().iter_rows()},
                          **({"sem_dado_na_janela": ausentes} if ausentes else {})})
        partes.append(feats.filter(pl.col("motivo_exclusao").is_null()))

    base = pl.concat(partes, how="vertical_relaxed")
    municipios = base.filter(pl.col("t0") <= treino_fim)["cod_ibge_municipio"].unique().to_list() + \
        base.filter(pl.col("t0") >= teste_ini)["cod_ibge_municipio"].unique().to_list()
    holdout = sample_holdout_municipios(list(set(municipios)), cfg["fracao_municipios_teste_grupo"], settings.seed)
    base = base.with_columns(
        pl.when(pl.col("t0") == t0_decisao).then(pl.lit("decisao"))
        .when(pl.col("t0") <= treino_fim).then(pl.lit("treino"))
        .when(pl.col("t0") >= teste_ini).then(pl.lit("teste"))
        .otherwise(pl.lit("folga")).alias("split"),
        pl.col("cod_ibge_municipio").is_in(list(holdout)).alias("grupo_holdout"),
        deterioracao_expr(pl.col(TARGET_CONTINUO), pl.col("razao_provisao_t0"), fator, minimo).alias(LABEL),
        (pl.col("razao_provisao_t0") >= limiar["valor"]).cast(pl.Int8).alias("alto_risco_atual"),
        pl.when(pl.col(TARGET_CONTINUO).is_null()).then(None)
        .otherwise((pl.col(TARGET_CONTINUO) >= limiar["valor"]).cast(pl.Int8)).alias("alto_risco_futuro"),
        pl.lit(limiar["valor"]).alias("limiar_alto_risco"),
    ).sort("t0", "cod_ibge_municipio")
    resumo = {"limiar_alto_risco": limiar, "label": {"nome": LABEL, "fator_relativo": fator, "minimo_absoluto": minimo},
              "t0s": [t.isoformat() for t in t0s], "exclusoes_por_t0": exclusoes,
              "municipios_holdout_grupo": sorted(holdout),
              "linhas_por_split": dict(base.group_by("split").len().iter_rows()),
              "prevalencia_por_split": {s: round(v, 4) for s, v in base.filter(pl.col(LABEL).is_not_null())
                                        .group_by("split").agg(pl.col(LABEL).mean()).iter_rows()}}
    return base, resumo


def split_frames(base: pl.DataFrame, cenario: str = "temporal") -> tuple[pl.DataFrame, pl.DataFrame]:
    treino = base.filter(pl.col("split") == "treino")
    teste = base.filter(pl.col("split") == "teste")
    if cenario == "temporal_grupo":
        treino = treino.filter(~pl.col("grupo_holdout"))
        teste = teste.filter(pl.col("grupo_holdout"))
    elif cenario != "temporal":
        raise ValueError(cenario)
    return treino, teste
