from __future__ import annotations

import numpy as np
import polars as pl
from sklearn.inspection import permutation_importance
from sklearn.metrics import average_precision_score, brier_score_loss, precision_score, recall_score, roc_auc_score

from credito_pa.ml.dataset import FEATURES
from credito_pa.ml.train import xy


def metrics(y: np.ndarray, score: np.ndarray, pred: np.ndarray | None = None, probabilistic: bool = True) -> dict:
    out = {
        "n": int(len(y)),
        "prevalencia": round(float(y.mean()), 4),
        "average_precision": round(float(average_precision_score(y, score)), 4),
        "roc_auc": round(float(roc_auc_score(y, score)), 4) if len(set(score)) > 1 else 0.5,
    }
    if probabilistic:
        out["brier"] = round(float(brier_score_loss(y, score)), 4)
    if pred is not None:
        out.update({
            "precisao": round(float(precision_score(y, pred, zero_division=0)), 4),
            "recall": round(float(recall_score(y, pred, zero_division=0)), 4),
            "sinalizados": int(pred.sum()),
            "vp": int(((pred == 1) & (y == 1)).sum()), "fp": int(((pred == 1) & (y == 0)).sum()),
            "fn": int(((pred == 0) & (y == 1)).sum()), "vn": int(((pred == 0) & (y == 0)).sum()),
        })
    return out


def bootstrap_ap(y: np.ndarray, score: np.ndarray, n: int, seed: int) -> list[float]:
    rng = np.random.default_rng(seed)
    vals = []
    for _ in range(n):
        idx = rng.integers(0, len(y), len(y))
        if y[idx].min() == y[idx].max():
            continue
        vals.append(average_precision_score(y[idx], score[idx]))
    return [round(float(np.percentile(vals, 2.5)), 4), round(float(np.percentile(vals, 97.5)), 4)]


def permutation_importances(model, teste: pl.DataFrame, seed: int, n_repeats: int = 20) -> list[dict]:
    X, y = xy(teste)
    r = permutation_importance(model, X, y, scoring="average_precision", n_repeats=n_repeats, random_state=seed)
    order = np.argsort(-r.importances_mean)
    return [{"feature": FEATURES[i], "queda_ap_media": round(float(r.importances_mean[i]), 4),
             "desvio": round(float(r.importances_std[i]), 4)} for i in order]


def alerta_metrica_suspeita(ap: float, limite: float, importancias: list[dict]) -> str | None:
    if ap < limite:
        return None
    top = ", ".join(f"{d['feature']} ({d['queda_ap_media']})" for d in importancias[:5])
    return (f"ALERTA: AP de teste = {ap:.3f} >= {limite}. Investigar possível vazamento. "
            f"Features de maior importância: {top}.")
