from __future__ import annotations

import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from credito_pa.ml.dataset import FEATURES, LABEL


def xy(df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    X = df.select(FEATURES).to_numpy().astype(float)
    y = df[LABEL].to_numpy().astype(int) if LABEL in df.columns and df[LABEL].null_count() == 0 else None
    return X, y


def make_models(seed: int) -> dict[str, Pipeline]:
    return {
        "regressao_logistica": Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(C=0.5, max_iter=5000, random_state=seed)),
        ]),
        "gradient_boosting": Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("clf", HistGradientBoostingClassifier(max_depth=3, learning_rate=0.05, max_iter=200,
                                                   min_samples_leaf=30, l2_regularization=1.0,
                                                   early_stopping=False, random_state=seed)),
        ]),
    }


def fit_model(model: Pipeline, treino: pl.DataFrame) -> Pipeline:
    X, y = xy(treino)
    return model.fit(X, y)


def predict_proba(model: Pipeline, df: pl.DataFrame) -> np.ndarray:
    X, _ = xy(df)
    return model.predict_proba(X)[:, 1]


def baseline_prevalencia(treino: pl.DataFrame, df: pl.DataFrame) -> np.ndarray:
    return np.full(df.height, float(treino[LABEL].mean()))


def baseline_nivel_atual(df: pl.DataFrame) -> np.ndarray:
    return df["razao_provisao_t0"].fill_null(0).to_numpy().astype(float)


def baseline_tendencia(df: pl.DataFrame) -> np.ndarray:
    return df["razao_provisao_tendencia_6m"].fill_null(0).to_numpy().astype(float)


BASELINES_REGRA = {"baseline_nivel_atual": baseline_nivel_atual, "baseline_tendencia_6m": baseline_tendencia}
