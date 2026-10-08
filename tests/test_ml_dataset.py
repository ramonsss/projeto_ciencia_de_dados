from datetime import date

import numpy as np
import polars as pl
import pytest

from credito_pa.gold.base import month_grid
from credito_pa.ml.dataset import (
    FEATURES,
    LABEL,
    NAO_FEATURES,
    TARGET_CONTINUO,
    add_months,
    build_features,
    build_label,
    build_ml_base,
    deterioracao_expr,
    label_threshold,
    quarter_ends,
    sample_holdout_municipios,
    split_frames,
)

T0 = date(2022, 6, 30)


def synthetic_fato(n_mun=12, seed=0) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    meses = month_grid("2019-01", "2024-12")
    rows = []
    for i in range(n_mun):
        base_risk = rng.uniform(0.01, 0.08)
        for j, d in enumerate(meses):
            cart = 1e7 * (1 + i) * (1 + 0.01 * j)
            rows.append({
                "cod_ibge_municipio": f"15{i:05d}", "data_competencia": d,
                "carteira_credito_reais": cart,
                "razao_provisao": float(np.clip(base_risk + rng.normal(0, 0.005), 0, 1)),
                "credito_rural_reais": cart * 0.2, "credito_imobiliario_reais": cart * 0.1,
                "emprestimos_reais": cart * 0.5, "depositos_reais": cart * 0.8, "n_instituicoes": 3,
                "pib_per_capita_reais": 15000.0 + 1000 * i, "populacao_referencia": 20000 + 1000 * i,
                "part_vab_agropecuaria": 0.3, "part_vab_industria": 0.1, "part_vab_adm_publica": 0.4,
                "focos_calor_mes": int(rng.integers(0, 50)),
                "scr_inadimplencia_pa": 0.03 + 0.0001 * j, "scr_ativo_problematico_pa": 0.06,
            })
    return pl.DataFrame(rows)


def corromper(df: pl.DataFrame, mask: pl.Expr) -> pl.DataFrame:
    num = [c for c, t in df.schema.items() if t.is_numeric()]
    return df.with_columns([pl.when(mask).then(pl.lit(9.99e9)).otherwise(pl.col(c)).cast(df.schema[c]).alias(c) for c in num])


def test_features_nao_usam_dado_posterior_a_t0():
    fato = synthetic_fato()
    a = build_features(fato, T0, 12).sort("cod_ibge_municipio")
    b = build_features(corromper(fato, pl.col("data_competencia") > T0), T0, 12).sort("cod_ibge_municipio")
    assert a.select(FEATURES).equals(b.select(FEATURES))
    assert (a["data_max_feature"] <= T0).all()
    assert (a["data_min_feature"] == add_months(T0, -11)).all()


def test_label_usa_somente_janela_de_predicao():
    fato = synthetic_fato()
    a = build_label(fato, T0, 6).sort("cod_ibge_municipio")
    corr_passado = corromper(fato, pl.col("data_competencia") <= T0)
    corr_depois = corromper(fato, pl.col("data_competencia") > add_months(T0, 6))
    assert a.equals(build_label(corr_passado, T0, 6).sort("cod_ibge_municipio"))
    assert a.equals(build_label(corr_depois, T0, 6).sort("cod_ibge_municipio"))
    assert (a["label_inicio"] > T0).all() and (a["label_fim"] == add_months(T0, 6)).all()


def test_limiar_do_label_usa_somente_periodo_de_treino():
    fato = synthetic_fato()
    ate = date(2023, 6, 30)
    a = label_threshold(fato, ate, 75, 5e6)
    b = label_threshold(corromper(fato, pl.col("data_competencia") > ate), ate, 75, 5e6)
    assert a == b


def test_t0_trimestrais():
    assert quarter_ends("2020-12", "2021-06") == [date(2020, 12, 31), date(2021, 3, 31), date(2021, 6, 30)]


@pytest.fixture
def base(settings):
    df, resumo = build_ml_base(settings, synthetic_fato())
    return df, resumo


def test_split_temporal_sem_sobreposicao_de_janelas(base):
    df, _ = base
    treino, teste = split_frames(df, "temporal")
    assert treino["label_fim"].max() <= teste["t0"].min()
    assert treino["t0"].max() < teste["t0"].min()
    assert set(df["split"].unique()) <= {"treino", "folga", "teste", "decisao"}
    assert df.filter(pl.col("split") == "decisao")[LABEL].null_count() == df.filter(pl.col("split") == "decisao").height


def test_split_por_grupo_sem_municipio_em_treino_e_teste(base):
    df, _ = base
    treino, teste = split_frames(df, "temporal_grupo")
    inter = set(treino["cod_ibge_municipio"]) & set(teste["cod_ibge_municipio"])
    assert inter == set() and teste.height > 0


def test_todas_as_linhas_respeitam_t0(base):
    df, _ = base
    assert df.filter(pl.col("data_max_feature") > pl.col("t0")).height == 0
    assert df.filter(pl.col("label_inicio") <= pl.col("t0")).height == 0


def test_label_reproduzivel_e_limiar_congelado(settings, base):
    df1, r1 = base
    df2, r2 = build_ml_base(settings, synthetic_fato())
    assert df1.equals(df2) and r1["limiar_alto_risco"] == r2["limiar_alto_risco"]
    assert (settings.layer_dir("gold") / "ml_artifacts" / "limiar_alto_risco.json").exists()
    rot = df1.filter(pl.col(LABEL).is_not_null())
    fut, atual = rot[TARGET_CONTINUO], rot["razao_provisao_t0"]
    esperado = ((fut >= 1.25 * atual) & (fut >= atual + 0.005)).cast(pl.Int8)
    assert rot[LABEL].equals(esperado.alias(LABEL))
    lim = r1["limiar_alto_risco"]["valor"]
    assert df1["alto_risco_atual"].equals((df1["razao_provisao_t0"] >= lim).cast(pl.Int8).alias("alto_risco_atual"))


@pytest.mark.parametrize("atual,futura,esperado", [
    (0.002, 0.004, 0),
    (0.030, 0.036, 0),
    (0.030, 0.040, 1),
    (0.000, 0.006, 1),
    (0.050, 0.040, 0),
])
def test_regra_de_deterioracao_material(atual, futura, esperado):
    df = pl.DataFrame({"f": [futura], "a": [atual]})
    assert df.select(deterioracao_expr(pl.col("f"), pl.col("a"), 1.25, 0.005)).item() == esperado


def test_label_e_target_nao_sao_features():
    assert NAO_FEATURES.isdisjoint(FEATURES)
    assert {"deterioracao", "alto_risco_futuro", "razao_provisao_futura_media"} <= NAO_FEATURES


def test_sorteio_de_grupo_reproduzivel_com_seed():
    muns = [f"15{i:05d}" for i in range(50)]
    assert sample_holdout_municipios(muns, 0.2, 42) == sample_holdout_municipios(list(reversed(muns)), 0.2, 42)
    assert len(sample_holdout_municipios(muns, 0.2, 42)) == 10
