from __future__ import annotations

import numpy as np
import polars as pl
from sklearn.base import clone
from sklearn.metrics import average_precision_score

from credito_pa.config import Settings
from credito_pa.ml.dataset import LABEL
from credito_pa.ml.train import BASELINES_REGRA, fit_model, predict_proba


def custos_unitarios(settings: Settings) -> dict:
    c = settings.section("custos")
    c_fn = c["lgd"] * c["pd_adicional"]
    c_fp = c["margem_liquida_anual"] * c["fracao_bons_clientes_perdidos"] + c["custo_campanha_relativo"]
    return {"c_fn": round(c_fn, 6), "c_fp": round(c_fp, 6), "razao_fn_fp": round(c_fn / c_fp, 3),
            "limiar_teorico": round(c_fp / (c_fp + c_fn), 4), "fracao_carteira_exposta": c["fracao_carteira_exposta"]}


def oof_predictions(models: dict, treino: pl.DataFrame, n_val: int) -> tuple[pl.DataFrame, dict[str, np.ndarray]]:
    t0s = sorted(treino["t0"].unique().to_list())
    val_t0s = t0s[-n_val:]
    linhas, preds = [], {name: [] for name in [*models, *BASELINES_REGRA]}
    for t_val in val_t0s:
        fit_df = treino.filter(pl.col("label_fim") <= t_val)
        val_df = treino.filter(pl.col("t0") == t_val)
        linhas.append(val_df)
        for name, model in models.items():
            preds[name].append(predict_proba(fit_model(clone(model), fit_df), val_df))
        for name, fn in BASELINES_REGRA.items():
            preds[name].append(fn(val_df))
    return pl.concat(linhas), {k: np.concatenate(v) for k, v in preds.items()}


def custo(y: np.ndarray, pred: np.ndarray, c_fn: float, c_fp: float, exposicao: np.ndarray | None = None) -> float:
    w = np.ones_like(y, dtype=float) if exposicao is None else exposicao
    fn = (pred == 0) & (y == 1)
    fp = (pred == 1) & (y == 0)
    return float((fn * c_fn * w).sum() + (fp * c_fp * w).sum())


def curva_custo(y: np.ndarray, score: np.ndarray, c_fn: float, c_fp: float) -> list[dict]:
    candidatos = np.unique(np.quantile(score, np.linspace(0, 1, 101)))
    candidatos = np.append(candidatos, score.max() + 1e-9)
    return [{"limiar": float(t), "custo": custo(y, (score >= t).astype(int), c_fn, c_fp),
             "sinalizados": int((score >= t).sum())} for t in candidatos]


def escolher_limiar(y: np.ndarray, score: np.ndarray, c_fn: float, c_fp: float) -> dict:
    curva = curva_custo(y, score, c_fn, c_fp)
    melhor = min(curva, key=lambda d: (d["custo"], -d["limiar"]))
    return {"limiar": melhor["limiar"], "custo_validacao": round(melhor["custo"], 4),
            "sinalizados_validacao": melhor["sinalizados"], "curva": curva}


def ap_oof(y: np.ndarray, preds: dict[str, np.ndarray]) -> dict[str, float]:
    return {k: round(float(average_precision_score(y, v)), 4) for k, v in preds.items()}


def exposicao(df: pl.DataFrame, fracao: float) -> np.ndarray:
    return df["carteira_t0"].to_numpy() * fracao


def resumo_custo_teste(y: np.ndarray, pred: np.ndarray, exp: np.ndarray, cu: dict) -> dict:
    fn = (pred == 0) & (y == 1)
    fp = (pred == 1) & (y == 0)
    vp = (pred == 1) & (y == 1)
    return {
        "sinalizados": int(pred.sum()), "vp": int(vp.sum()), "fp": int(fp.sum()), "fn": int(fn.sum()),
        "custo_fn_reais": round(float((fn * cu["c_fn"] * exp).sum()), 2),
        "custo_fp_reais": round(float((fp * cu["c_fp"] * exp).sum()), 2),
        "custo_total_reais": round(float((fn * cu["c_fn"] * exp).sum() + (fp * cu["c_fp"] * exp).sum()), 2),
        "custo_unidades": round(custo(y, pred, cu["c_fn"], cu["c_fp"]), 4),
    }


def efeito_riqueza(base: pl.DataFrame) -> list[dict]:
    rot = base.filter(pl.col(LABEL).is_not_null() & pl.col("pib_per_capita_t0").is_not_null())
    rot = rot.with_columns(((pl.col("pib_per_capita_t0").rank("ordinal") * 5 / (pl.len() + 1)).floor().cast(pl.Int32) + 1)
                           .alias("quintil"))
    return (rot.group_by("quintil").agg(
        pl.col("pib_per_capita_t0").min().round(0).alias("pib_pc_min"),
        pl.col("pib_per_capita_t0").max().round(0).alias("pib_pc_max"),
        pl.len().alias("observacoes"),
        pl.col("razao_provisao_t0").mean().round(4).alias("razao_provisao_media"),
        pl.col("razao_provisao_t0").median().round(4).alias("razao_provisao_mediana"),
        pl.col("alto_risco_atual").mean().round(4).alias("taxa_alto_risco"),
        pl.col(LABEL).mean().round(4).alias("taxa_deterioracao"),
    ).sort("quintil").to_dicts())
