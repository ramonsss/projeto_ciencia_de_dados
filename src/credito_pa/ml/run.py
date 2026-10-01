from __future__ import annotations

import joblib
import numpy as np
import polars as pl

from credito_pa.common.io import write_json_atomic, write_text_atomic
from credito_pa.config import Settings
from credito_pa.gold.base import read_gold
from credito_pa.ml.dataset import FEATURES, LABEL, split_frames
from credito_pa.ml.decision import (
    ap_oof,
    custos_unitarios,
    escolher_limiar,
    exposicao,
    oof_predictions,
    resumo_custo_teste,
)
from credito_pa.ml.evaluate import alerta_metrica_suspeita, bootstrap_ap, metrics, permutation_importances
from credito_pa.ml.train import BASELINES_REGRA, baseline_prevalencia, fit_model, make_models, predict_proba


def _artifacts(settings: Settings):
    path = settings.layer_dir("gold") / "ml_artifacts"
    path.mkdir(parents=True, exist_ok=True)
    return path


def avaliar_cenario(settings: Settings, base: pl.DataFrame, cenario: str, limiares: dict | None = None) -> dict:
    cfg, seed = settings.section("ml"), settings.seed
    cu = custos_unitarios(settings)
    treino, teste = split_frames(base, cenario)
    y_te = teste[LABEL].to_numpy().astype(int)
    exp_te = exposicao(teste, cu["fracao_carteira_exposta"])
    models = make_models(seed)

    oof_df, oof = oof_predictions(models, treino, cfg["n_splits_validacao"])
    y_oof = oof_df[LABEL].to_numpy().astype(int)
    ap_val = ap_oof(y_oof, oof)
    if limiares is None:
        limiares = {k: escolher_limiar(y_oof, v, cu["c_fn"], cu["c_fp"]) for k, v in oof.items()}

    resultados, scores = {}, {}
    prev = baseline_prevalencia(treino, teste)
    ap_prev = round(float(y_te.mean()), 4)
    resultados["baseline_prevalencia"] = {**metrics(y_te, prev), "ap_validacao": round(float(treino[LABEL].mean()), 4),
                                          "ic95_ap": [ap_prev, ap_prev]}
    fitted = {}
    for name, model in models.items():
        fitted[name] = fit_model(model, treino)
        scores[name] = predict_proba(fitted[name], teste)
    for name, fn in BASELINES_REGRA.items():
        scores[name] = fn(teste)
    for name, s in scores.items():
        lim = limiares[name]["limiar"]
        pred = (s >= lim).astype(int)
        resultados[name] = {
            **metrics(y_te, s, pred, probabilistic=name in models),
            "ap_validacao": ap_val[name], "limiar": round(lim, 6),
            "ic95_ap": bootstrap_ap(y_te, s, cfg["bootstrap_n"], seed),
            "custo_teste": resumo_custo_teste(y_te, pred, exp_te, cu),
        }
    for nome, pred in {"nao_agir": np.zeros_like(y_te), "agir_em_todos": np.ones_like(y_te)}.items():
        resultados[nome] = {"custo_teste": resumo_custo_teste(y_te, pred, exp_te, cu)}

    escolhido = max(models, key=lambda n: ap_val[n])
    imp = permutation_importances(fitted[escolhido], teste, seed)
    alerta = alerta_metrica_suspeita(resultados[escolhido]["average_precision"], cfg["alerta_ap_suspeita"], imp)
    return {
        "cenario": cenario, "n_treino": treino.height, "n_teste": teste.height,
        "municipios_treino": treino["cod_ibge_municipio"].n_unique(), "municipios_teste": teste["cod_ibge_municipio"].n_unique(),
        "intersecao_municipios": len(set(treino["cod_ibge_municipio"]) & set(teste["cod_ibge_municipio"])),
        "n_validacao_oof": len(y_oof), "prevalencia_validacao": round(float(y_oof.mean()), 4),
        "modelo_escolhido": escolhido, "resultados": resultados, "importancia_permutacao": imp, "alerta": alerta,
        "_limiares": limiares, "_fitted": fitted,
    }


def avaliar_trilho_renegociacao(base: pl.DataFrame) -> dict:
    _, teste = split_frames(base, "temporal")
    y = teste["alto_risco_futuro"].to_numpy().astype(int)
    m = metrics(y, teste["razao_provisao_t0"].to_numpy(), teste["alto_risco_atual"].to_numpy().astype(int),
                probabilistic=False)
    return {**m, "observacao": "AP alta vem da autocorrelação do risco (persistência), não de vazamento. Por isso "
                               "este trilho usa regra transparente e o ML foi direcionado ao alerta de deterioração."}


def coeficiente_riqueza(settings: Settings, base: pl.DataFrame) -> dict:
    treino, _ = split_frames(base, "temporal")
    model = fit_model(make_models(settings.seed)["regressao_logistica"], treino)
    coef = float(model.named_steps["clf"].coef_[0][FEATURES.index("log_pib_per_capita")])
    return {"feature": "log_pib_per_capita", "coef_padronizado": round(coef, 4),
            "odds_ratio_por_1_desvio": round(float(np.exp(coef)), 4)}


def run_ml(settings: Settings) -> dict:
    base = read_gold(settings, "ml_base_risco_credito")
    temporal = avaliar_cenario(settings, base, "temporal")
    grupo = avaliar_cenario(settings, base, "temporal_grupo", limiares=temporal["_limiares"])
    escolhido = temporal["modelo_escolhido"]

    rotuladas = base.filter(pl.col(LABEL).is_not_null())
    final = fit_model(make_models(settings.seed)[escolhido], rotuladas)
    art = _artifacts(settings)
    joblib.dump({"modelo": final, "features": FEATURES, "limiar": temporal["_limiares"][escolhido]["limiar"],
                 "nome": escolhido}, art / "modelo_final.joblib")

    cu = custos_unitarios(settings)
    resultado = {
        "label": LABEL, "custos": cu, "modelo_escolhido": escolhido,
        "criterio_escolha": "maior average precision nas previsões fora da amostra do treino (validação temporal com folga)",
        "limiar_escolhido": round(temporal["_limiares"][escolhido]["limiar"], 6),
        "curva_custo_validacao": temporal["_limiares"][escolhido]["curva"],
        "cenarios": {c["cenario"]: {k: v for k, v in c.items() if not k.startswith("_")} for c in (temporal, grupo)},
        "trilho_renegociacao": avaliar_trilho_renegociacao(base),
        "riqueza_no_modelo": coeficiente_riqueza(settings, base),
        "observacoes_treino_final": rotuladas.height,
    }
    write_json_atomic(resultado, settings.reports_dir / "ml_resultados.json")
    write_text_atomic(render_ml_markdown(resultado), settings.reports_dir / "ml_resultados.md")
    return resultado


def render_ml_markdown(r: dict) -> str:
    cu = r["custos"]
    lines = ["# Resultados do modelo (alerta de deterioração)", "",
             f"- **Label:** `{r['label']}`. **Modelo escolhido:** `{r['modelo_escolhido']}` ({r['criterio_escolha']}).",
             f"- **Custos:** C_FN = {cu['c_fn']:.4f} e C_FP = {cu['c_fp']:.4f} por R$ exposto (FN/FP = {cu['razao_fn_fp']}); "
             f"limiar teórico = {cu['limiar_teorico']}; **limiar escolhido na validação = {r['limiar_escolhido']:.4f}**.", ""]
    for nome, c in r["cenarios"].items():
        lines += [f"## Cenário `{nome}`", "",
                  f"Treino: {c['n_treino']} obs. ({c['municipios_treino']} municípios); teste: {c['n_teste']} obs. "
                  f"({c['municipios_teste']} municípios); municípios em comum: {c['intersecao_municipios']}.", "",
                  "| Abordagem | AP validação | AP teste | IC95 AP | ROC-AUC | Brier | Precisão | Recall | Sinalizados | Custo teste (R$) |",
                  "|---|---:|---:|---|---:|---:|---:|---:|---:|---:|"]
        for nome_m, m in c["resultados"].items():
            ct = m.get("custo_teste", {})
            lines.append(f"| {nome_m} | {m.get('ap_validacao', '')} | {m.get('average_precision', '')} | {m.get('ic95_ap', '')} | "
                         f"{m.get('roc_auc', '')} | {m.get('brier', '') or ''} | {m.get('precisao', '')} | {m.get('recall', '')} | "
                         f"{ct.get('sinalizados', '')} | {ct.get('custo_total_reais', 0):,.0f} |")
        lines += ["", "Importância por permutação (queda de AP no teste), top 8:", ""]
        lines += [f"- `{d['feature']}`: {d['queda_ap_media']} (±{d['desvio']})" for d in c["importancia_permutacao"][:8]]
        if c["alerta"]:
            lines += ["", f"> **{c['alerta']}**"]
        lines.append("")
    t = r["trilho_renegociacao"]
    lines += ["## Trilho renegociação (regra de nível atual, sem ML)", "",
              f"- Prever `alto_risco_futuro` pela regra `alto_risco_atual`: AP = {t['average_precision']}, precisão = {t['precisao']}, "
              f"recall = {t['recall']} (n = {t['n']}).", f"- {t['observacao']}", "",
              "## Riqueza no modelo", "",
              f"- Coeficiente padronizado de `log_pib_per_capita` (logística): {r['riqueza_no_modelo']['coef_padronizado']} "
              f"(odds ratio por 1 desvio = {r['riqueza_no_modelo']['odds_ratio_por_1_desvio']}).", ""]
    return "\n".join(lines)


def run_report(settings: Settings) -> dict:
    from credito_pa.gold.ranking import run_ranking_e_decisao

    return run_ranking_e_decisao(settings)
