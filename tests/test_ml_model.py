import numpy as np
import polars as pl
import pytest

from credito_pa.ml import decision
from credito_pa.ml.dataset import FEATURES, LABEL, build_ml_base, split_frames
from credito_pa.ml.decision import custos_unitarios, escolher_limiar, oof_predictions
from credito_pa.ml.evaluate import alerta_metrica_suspeita, metrics
from credito_pa.ml.train import fit_model, make_models, predict_proba, xy
from test_ml_dataset import synthetic_fato


@pytest.fixture
def splits(settings):
    base, _ = build_ml_base(settings, synthetic_fato(n_mun=30, seed=1))
    base = base.with_columns(pl.when(pl.int_range(pl.len()) % 7 == 0).then(None)
                             .otherwise(pl.col("part_focos_ultimos_6m")).alias("part_focos_ultimos_6m"))
    treino, teste = split_frames(base, "temporal")
    treino = treino.with_columns((pl.col("razao_provisao_t0") > pl.col("razao_provisao_t0").median()).cast(pl.Int8).alias(LABEL))
    teste = teste.with_columns((pl.col("razao_provisao_t0") > pl.col("razao_provisao_t0").median()).cast(pl.Int8).alias(LABEL))
    return treino, teste


def test_imputador_e_scaler_ajustados_somente_no_treino(splits, settings):
    treino, teste = splits
    model = fit_model(make_models(settings.seed)["regressao_logistica"], treino)
    X_tr, _ = xy(treino)
    X_all = np.vstack([X_tr, xy(teste)[0]])
    imputer, scaler = model.named_steps["imputer"], model.named_steps["scaler"]
    np.testing.assert_allclose(imputer.statistics_, np.nanmedian(X_tr, axis=0))
    X_tr_imp = np.where(np.isnan(X_tr), np.nanmedian(X_tr, axis=0), X_tr)
    np.testing.assert_allclose(scaler.mean_, X_tr_imp.mean(axis=0))
    assert not np.allclose(scaler.mean_, np.where(np.isnan(X_all), np.nanmedian(X_all, axis=0), X_all).mean(axis=0))
    antes = imputer.statistics_.copy()
    predict_proba(model, teste)
    np.testing.assert_array_equal(antes, imputer.statistics_)


def test_treino_reprodutivel(splits, settings):
    treino, teste = splits
    for name in make_models(settings.seed):
        p1 = predict_proba(fit_model(make_models(settings.seed)[name], treino), teste)
        p2 = predict_proba(fit_model(make_models(settings.seed)[name], treino), teste)
        np.testing.assert_array_equal(p1, p2)


def test_validacao_oof_respeita_folga_temporal(splits, settings, monkeypatch):
    treino, _ = splits
    vistos = []
    original = decision.fit_model

    def espia(model, fit_df):
        vistos.append(fit_df["label_fim"].max())
        return original(model, fit_df)

    monkeypatch.setattr(decision, "fit_model", espia)
    val_df, _ = oof_predictions(make_models(settings.seed), treino, 2)
    t0s_val = sorted(val_df["t0"].unique().to_list())
    assert all(v <= t for v, t in zip(vistos[::2], t0s_val)) and len(vistos) == 4


def test_custos_e_limiar_teorico(settings):
    cu = custos_unitarios(settings)
    assert cu["c_fn"] == pytest.approx(0.045) and cu["c_fp"] == pytest.approx(0.0245)
    assert cu["limiar_teorico"] == pytest.approx(0.0245 / 0.0695, abs=1e-4)


def test_limiar_minimiza_custo():
    y = np.array([0, 0, 0, 1, 1])
    score = np.array([0.1, 0.2, 0.3, 0.8, 0.9])
    r = escolher_limiar(y, score, c_fn=0.045, c_fp=0.0245)
    assert r["custo_validacao"] == 0 and 0.3 < r["limiar"] <= 0.8


def test_alerta_de_metrica_suspeita():
    imp = [{"feature": "x", "queda_ap_media": 0.5}]
    assert alerta_metrica_suspeita(0.97, 0.95, imp).startswith("ALERTA")
    assert alerta_metrica_suspeita(0.30, 0.95, imp) is None


def test_metricas_basicas():
    y = np.array([0, 1, 0, 1])
    m = metrics(y, np.array([0.1, 0.9, 0.2, 0.8]), np.array([0, 1, 0, 1]))
    assert m["average_precision"] == 1.0 and m["precisao"] == 1.0 and m["recall"] == 1.0
    assert "brier" in m and "brier" not in metrics(y, np.array([1, 2, 3, 4]), probabilistic=False)


def test_features_sao_todas_numericas(splits):
    treino, _ = splits
    X, _ = xy(treino)
    assert X.shape[1] == len(FEATURES) and X.dtype == float
