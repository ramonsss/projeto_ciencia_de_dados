from __future__ import annotations

import json

import joblib
import polars as pl

from credito_pa.common.io import write_json_atomic, write_text_atomic
from credito_pa.config import Settings
from credito_pa.gold.base import finalize_gold, read_gold
from credito_pa.gold.contracts import RANKING_PRIORIZACAO_MUNICIPIOS
from credito_pa.gold.export_warehouse import export_tables
from credito_pa.ml.decision import custos_unitarios, efeito_riqueza
from credito_pa.ml.train import xy
from credito_pa.silver.base import read_silver

ORDEM_ACAO = {"renegociar_e_restringir": 1, "renegociar_priorizar": 2, "restringir_credito_sem_garantia": 3, "manter": 4}


def build_ranking(settings: Settings, base: pl.DataFrame, artefato: dict, cu: dict) -> pl.DataFrame:
    dec = base.filter(pl.col("split") == "decisao")
    X, _ = xy(dec)
    prob = artefato["modelo"].predict_proba(X)[:, 1]
    dim = read_silver(settings, "dim_municipio").select("cod_ibge_municipio", "nome_municipio", "mesorregiao")
    df = dec.with_columns(pl.Series("prob_deterioracao", prob)).join(dim, on="cod_ibge_municipio", how="left")
    df = df.with_columns(
        (pl.col("prob_deterioracao") >= artefato["limiar"]).cast(pl.Int8).alias("alerta_deterioracao"),
        (pl.col("carteira_t0") * cu["fracao_carteira_exposta"]).alias("exposicao_sem_garantia_reais"),
        pl.col("carteira_t0").alias("carteira_credito_reais"),
        pl.col("pib_per_capita_t0").alias("pib_per_capita_reais"),
    ).with_columns(
        pl.when((pl.col("alto_risco_atual") == 1) & (pl.col("alerta_deterioracao") == 1)).then(pl.lit("renegociar_e_restringir"))
        .when(pl.col("alto_risco_atual") == 1).then(pl.lit("renegociar_priorizar"))
        .when(pl.col("alerta_deterioracao") == 1).then(pl.lit("restringir_credito_sem_garantia"))
        .otherwise(pl.lit("manter")).alias("acao_recomendada"),
        (pl.col("prob_deterioracao") * cu["c_fn"] * pl.col("exposicao_sem_garantia_reais")
         - (1 - pl.col("prob_deterioracao")) * cu["c_fp"] * pl.col("exposicao_sem_garantia_reais"))
        .alias("ganho_esperado_restricao_reais"),
    )
    df = df.with_columns(
        pl.col("acao_recomendada").replace_strict(ORDEM_ACAO).alias("_ordem_acao"),
        pl.when(pl.col("alto_risco_atual") == 1).then(pl.col("razao_provisao_t0")).otherwise(pl.col("prob_deterioracao"))
        .alias("_chave"),
    ).sort(["_ordem_acao", "_chave"], descending=[False, True])
    return df.with_columns(pl.int_range(1, pl.len() + 1, dtype=pl.Int32).alias("ordem_prioridade"))


def _fmt_reais(v: float) -> str:
    if abs(v) >= 1e9:
        return f"R$ {v / 1e9:,.2f} bi".replace(",", "X").replace(".", ",").replace("X", ".")
    if abs(v) >= 1e6:
        return f"R$ {v / 1e6:,.1f} mi".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {v:,.0f}".replace(",", ".")


def _pct(v: float, casas: int = 1) -> str:
    return f"{v * 100:.{casas}f}%".replace(".", ",")


def _num(v: float, casas: int = 2) -> str:
    return f"{v:.{casas}f}".replace(".", ",")


def build_decisao(settings: Settings, ranking: pl.DataFrame, base: pl.DataFrame, ml: dict, cu: dict) -> dict:
    ranking = ranking.sort("ordem_prioridade")
    lim_alto = float(base["limiar_alto_risco"][0])
    quintis = efeito_riqueza(base)
    q1, q5 = quintis[0], quintis[-1]
    spearman = ranking.select(pl.corr("pib_per_capita_reais", "razao_provisao_t0", method="spearman")).item()
    ranking = ranking.with_columns((pl.col("razao_provisao_t0") * pl.col("carteira_credito_reais")).alias("_provisao"))
    reneg = ranking.filter(pl.col("alto_risco_atual") == 1)
    restr = ranking.filter(pl.col("alerta_deterioracao") == 1)
    nao_sinal = ranking.filter(pl.col("alerta_deterioracao") == 0)
    vigilancia = nao_sinal.filter(pl.col("alto_risco_atual") == 0).sort("prob_deterioracao", descending=True).head(5)
    top = ranking.head(5)["nome_municipio"].to_list()
    share_provisao = float(reneg["_provisao"].sum() / ranking["_provisao"].sum())
    cen = ml["cenarios"]["temporal"]
    esc = ml["modelo_escolhido"]
    r_mod, r_nao = cen["resultados"][esc], cen["resultados"]["nao_agir"]
    t1 = ml["trilho_renegociacao"]
    economia_teste = r_nao["custo_teste"]["custo_total_reais"] - r_mod["custo_teste"]["custo_total_reais"]
    ganho_esperado = float(restr["ganho_esperado_restricao_reais"].sum()) if restr.height else 0.0
    custo_fp_esperado = float(((1 - restr["prob_deterioracao"]) * cu["c_fp"] * restr["exposicao_sem_garantia_reais"]).sum())
    custo_fn_residual = float((nao_sinal["prob_deterioracao"] * cu["c_fn"] * nao_sinal["exposicao_sem_garantia_reais"]).sum())
    lim_det = ml["limiar_escolhido"]
    vig_txt = ", ".join(f"{r['nome_municipio']} ({_pct(r['prob_deterioracao'])})" for r in vigilancia.iter_rows(named=True))
    if restr.height:
        acao_restr = (f"a restrição (exigência de garantia ou redução de limite) de novas linhas sem garantia em "
                      f"{restr.height} município(s) com alerta de deterioração")
        ganho_restr = f"{_fmt_reais(ganho_esperado)} de perda líquida esperada evitada nas linhas sem garantia restringidas"
    else:
        acao_restr = (f"mantenha, por ora, as linhas sem garantia sem restrição (nenhum município atingiu a probabilidade "
                      f"de deterioração que compensa o custo de restringir, {_pct(lim_det)}), com vigilância mensal "
                      f"dos 5 de maior probabilidade")
        ganho_restr = (f"no trilho de concessão, o modelo reduziu o custo em {_fmt_reais(economia_teste)} frente a não agir "
                       f"no teste histórico (2023-06 a 2024-06)")

    campos = {
        "bases": "ESTBAN e SCR.data (Banco Central), PIB dos Municípios e População (IBGE) e focos de calor (INPE)",
        "identificamos": (
            f"{reneg.height} dos {ranking.height} municípios elegíveis do Pará já estão em risco alto (provisão ≥ "
            f"{_pct(lim_alto, 2)} da carteira) e concentram {_pct(share_provisao)} das perdas já provisionadas do estado. "
            f"A riqueza NÃO explica o NÍVEL de risco: o quintil mais pobre de PIB per capita (até "
            f"{_fmt_reais(q1['pib_pc_max'])}) tem provisão mediana de {_pct(q1['razao_provisao_mediana'], 2)}, contra "
            f"{_pct(q5['razao_provisao_mediana'], 2)} no mais rico (Spearman = {_num(spearman)} em dez/2024). Já a taxa de "
            f"DETERIORAÇÃO é de {_pct(q1['taxa_deterioracao'])} no quintil mais pobre, contra "
            f"{_pct(min(q['taxa_deterioracao'] for q in quintis[1:]))} a {_pct(max(q['taxa_deterioracao'] for q in quintis[1:]))} "
            f"nos demais. Controlando pelas demais variáveis, porém, o PIB per capita não altera a chance de deterioração "
            f"(odds ratio de {_num(ml['riqueza_no_modelo']['odds_ratio_por_1_desvio'])} por desvio): o que antecipa a piora "
            f"é o porte e a composição da carteira local, menores nos municípios mais pobres"
        ),
        "decisor": "a diretoria de crédito das cooperativas de crédito e dos bancos regionais com atuação no Pará",
        "acao": f"uma campanha de renegociação de dívidas nos {reneg.height} municípios de risco alto e {acao_restr}",
        "prazo": "6 meses (jan a jun/2025, janela de predição a partir do t0 de dez/2024)",
        "priorizando": ", ".join(top),
        "ganho": (f"atacar {_pct(share_provisao)} das perdas provisionadas do estado atuando em {reneg.height} de "
                  f"{ranking.height} municípios. A regra acerta {_pct(t1['precisao'])} dos municípios que seguirão em "
                  f"risco alto (precisão no teste) e cobre {_pct(t1['recall'])} deles (recall). Além disso, {ganho_restr}"),
        "custo_erro": (f"{_pct(1 - t1['precisao'])} da campanha direcionada a municípios que sairiam do risco alto "
                       f"sozinhos (falso positivo) e {_pct(1 - t1['recall'])} dos municípios que entrarão em risco alto "
                       f"fora da campanha (falso negativo). No crédito sem garantia, a perda esperada residual nos municípios "
                       f"não restringidos é de {_fmt_reais(custo_fn_residual)}"
                       + (f", e a margem em risco nos restringidos é de {_fmt_reais(custo_fp_esperado)}" if restr.height else "")),
    }
    campos["vigilancia"] = vig_txt
    frase = (f"Cruzando as bases {campos['bases']}, identificamos que {campos['identificamos']}. "
             f"Recomendamos que {campos['decisor']} faça {campos['acao']} nos próximos {campos['prazo']}, priorizando "
             f"{campos['priorizando']}. Se agir, o ganho esperado é {campos['ganho']}; se errarmos, o custo é "
             f"{campos['custo_erro']}.")
    if not restr.height:
        frase += f" Vigilância de deterioração (sem ação imediata): {vig_txt}."
    return {
        "frase_resposta": frase, "campos_frase": campos,
        "t0_decisao": str(ranking["t0"][0]), "municipios_elegiveis": ranking.height,
        "contagem_acoes": dict(ranking.group_by("acao_recomendada").len().iter_rows()),
        "limiar_alto_risco": lim_alto, "limiar_deterioracao": ml["limiar_escolhido"], "custos": cu,
        "modelo": {"nome": esc, "ap_teste": r_mod["average_precision"], "ic95_ap": r_mod["ic95_ap"],
                   "ap_prevalencia": cen["resultados"]["baseline_prevalencia"]["average_precision"],
                   "ap_nivel_atual": cen["resultados"]["baseline_nivel_atual"]["average_precision"],
                   "ap_tendencia": cen["resultados"]["baseline_tendencia_6m"]["average_precision"],
                   "ap_grupo": ml["cenarios"]["temporal_grupo"]["resultados"][esc]["average_precision"],
                   "custo_teste": r_mod["custo_teste"], "custo_nao_agir_teste": r_nao["custo_teste"]},
        "trilho_renegociacao": ml["trilho_renegociacao"],
        "riqueza": {"quintis": quintis, "spearman_pib_pc_vs_razao_dez2024": round(spearman, 4),
                    "modelo": ml["riqueza_no_modelo"]},
        "valores": {"ganho_esperado_restricao_reais": round(ganho_esperado, 2),
                    "custo_fp_esperado_reais": round(custo_fp_esperado, 2),
                    "custo_fn_residual_reais": round(custo_fn_residual, 2), "economia_teste_reais": round(economia_teste, 2),
                    "carteira_risco_alto_reais": round(float(reneg["carteira_credito_reais"].sum()), 2),
                    "participacao_provisao_risco_alto": round(share_provisao, 4)},
        "vigilancia_deterioracao": vigilancia.select("nome_municipio", "prob_deterioracao", "razao_provisao_t0",
                                                     "carteira_credito_reais").to_dicts(),
        "top10": ranking.head(10).select("ordem_prioridade", "nome_municipio", "acao_recomendada", "razao_provisao_t0",
                                         "prob_deterioracao", "carteira_credito_reais").to_dicts(),
    }


def render_decisao_md(d: dict) -> str:
    lines = ["# Relatório de Decisão (gerado)", "", "Gerado por `python scripts/run_pipeline.py --stage report`.", "",
             "## Frase-resposta", "", f"> {d['frase_resposta']}", "",
             f"## Ações recomendadas (t0 = {d['t0_decisao']})", ""]
    lines += [f"- `{k}`: {v} município(s)" for k, v in sorted(d["contagem_acoes"].items())]
    lines += ["", "## Top 10 da fila de prioridade", "",
              "| # | Município | Ação | Risco atual | Prob. deterioração | Carteira |", "|---:|---|---|---:|---:|---:|"]
    for r in d["top10"]:
        lines.append(f"| {r['ordem_prioridade']} | {r['nome_municipio']} | {r['acao_recomendada']} | "
                     f"{_pct(r['razao_provisao_t0'], 2)} | {_pct(r['prob_deterioracao'])} | {_fmt_reais(r['carteira_credito_reais'])} |")
    lines += ["", "## Vigilância de deterioração (informativo; abaixo do limiar de ação)", ""]
    lines += [f"- {v['nome_municipio']}: probabilidade {_pct(v['prob_deterioracao'])}, risco atual "
              f"{_pct(v['razao_provisao_t0'], 2)}" for v in d["vigilancia_deterioracao"]]
    lines += ["", "## Riqueza × risco (quintis de PIB per capita, observações rotuladas)", "",
              "| Quintil | PIB pc (R$) | Obs. | Provisão média | Provisão mediana | % alto risco | % deterioração |",
              "|---:|---|---:|---:|---:|---:|---:|"]
    for q in d["riqueza"]["quintis"]:
        lines.append(f"| {q['quintil']} | {q['pib_pc_min']:,.0f} a {q['pib_pc_max']:,.0f} | {q['observacoes']} | "
                     f"{_pct(q['razao_provisao_media'], 2)} | {_pct(q['razao_provisao_mediana'], 2)} | "
                     f"{_pct(q['taxa_alto_risco'])} | {_pct(q['taxa_deterioracao'])} |")
    m = d["modelo"]
    lines += ["", "## Modelo", "",
              f"- `{m['nome']}`: AP de teste = {m['ap_teste']} (IC95 {m['ic95_ap']}); prevalência = {m['ap_prevalencia']}; "
              f"regra de nível atual = {m['ap_nivel_atual']}; regra de tendência = {m['ap_tendencia']}; "
              f"municípios não vistos (grupo) = {m['ap_grupo']}.", ""]
    return "\n".join(lines)


def run_ranking_e_decisao(settings: Settings) -> dict:
    base = read_gold(settings, "ml_base_risco_credito")
    artefato = joblib.load(settings.layer_dir("gold") / "ml_artifacts" / "modelo_final.joblib")
    ml = json.loads((settings.reports_dir / "ml_resultados.json").read_text(encoding="utf-8"))
    cu = custos_unitarios(settings)
    ranking = build_ranking(settings, base, artefato, cu)
    info = finalize_gold(settings, ranking, RANKING_PRIORIZACAO_MUNICIPIOS)
    export_tables(settings, [RANKING_PRIORIZACAO_MUNICIPIOS.name])
    ranking = read_gold(settings, RANKING_PRIORIZACAO_MUNICIPIOS.name)
    decisao = build_decisao(settings, ranking, base, ml, cu)
    decisao["ranking_pk"] = info
    write_json_atomic(decisao, settings.reports_dir / "decisao.json")
    write_text_atomic(render_decisao_md(decisao), settings.reports_dir / "decisao.md")
    return decisao
