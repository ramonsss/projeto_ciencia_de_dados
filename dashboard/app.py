from __future__ import annotations

import json

import altair as alt
import pandas as pd
import polars as pl
import streamlit as st

from credito_pa.config import load_settings

AZUL, LARANJA, VERDE_AGUA = "#2a78d6", "#eb6834", "#1baf7a"
TINTA, TINTA_2, MUDO, GRADE, EIXO = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"

ACOES = {
    "renegociar_priorizar": "Renegociar (prioritário)",
    "restringir_sem_garantia": "Restringir sem garantia",
    "manter": "Manter",
}
MODELOS = {
    "gradient_boosting": "Gradient boosting",
    "regressao_logistica": "Regressão logística",
    "baseline_prevalencia": "Baseline: prevalência (acaso)",
    "baseline_nivel_atual": "Baseline: nível atual",
    "baseline_tendencia_6m": "Baseline: tendência 6m",
}
CORES_ACAO = {ACOES["renegociar_priorizar"]: LARANJA, ACOES["restringir_sem_garantia"]: VERDE_AGUA, ACOES["manter"]: AZUL}
LOCALE_BR = {"decimal": ",", "thousands": ".", "grouping": [3], "currency": ["R$ ", ""]}
MESES = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro",
         "novembro", "dezembro"]
LOCALE_TEMPO_BR = {
    "dateTime": "%A, %e de %B de %Y. %X", "date": "%d/%m/%Y", "time": "%H:%M:%S", "periods": ["AM", "PM"],
    "days": ["domingo", "segunda", "terça", "quarta", "quinta", "sexta", "sábado"],
    "shortDays": ["dom", "seg", "ter", "qua", "qui", "sex", "sáb"],
    "months": MESES, "shortMonths": [m[:3] for m in MESES],
}

st.set_page_config(page_title="Risco de crédito municipal no Pará", layout="wide")


def pct(valor: float, casas: int = 1) -> str:
    return f"{valor * 100:.{casas}f}%".replace(".", ",")


def num(valor: float, casas: int = 2) -> str:
    return f"{valor:.{casas}f}".replace(".", ",")


def reais(valor: float) -> str:
    if abs(valor) >= 1e9:
        return f"R\\$ {valor / 1e9:.1f} bi".replace(".", ",")
    if abs(valor) >= 1e6:
        return f"R\\$ {valor / 1e6:.1f} mi".replace(".", ",")
    return f"R\\$ {valor:,.0f}".replace(",", ".")


def cor_acao(presentes) -> alt.Color:
    dominio = [a for a in CORES_ACAO if a in set(presentes)]
    return alt.Color("acao:N", scale=alt.Scale(domain=dominio, range=[CORES_ACAO[a] for a in dominio]))


def estilo(chart: alt.Chart | alt.LayerChart, altura: int = 320):
    return (
        chart.properties(height=altura)
        .configure(locale={"number": LOCALE_BR, "time": LOCALE_TEMPO_BR}, font="system-ui, -apple-system, 'Segoe UI', sans-serif")
        .configure_view(stroke=None)
        .configure_axis(gridColor=GRADE, domainColor=EIXO, tickColor=EIXO, labelColor=MUDO, titleColor=TINTA_2,
                        labelFontSize=12, titleFontSize=12, titleFontWeight="normal")
        .configure_legend(labelColor=TINTA_2, titleColor=TINTA_2, labelFontSize=12, orient="top", title=None)
    )


@st.cache_data
def carregar() -> dict:
    settings = load_settings()
    gold, reports = settings.data_dir / "gold", settings.reports_dir
    arquivos = {
        "ranking": gold / "ranking_priorizacao_municipios.parquet",
        "fato": gold / "fato_credito_municipio_mes.parquet",
        "scr": gold / "contexto_scr_pa_mes.parquet",
        "decisao": reports / "decisao.json",
        "ml": reports / "ml_resultados.json",
    }
    faltando = [str(p) for p in arquivos.values() if not p.exists()]
    if faltando:
        return {"faltando": faltando}
    dados = {k: pl.read_parquet(p).to_pandas() for k, p in arquivos.items() if p.suffix == ".parquet"}
    dados.update({k: json.loads(p.read_text(encoding="utf-8")) for k, p in arquivos.items() if p.suffix == ".json"})
    return dados


dados = carregar()
if "faltando" in dados:
    st.error("Faltam saídas do pipeline. Rode `python scripts/run_pipeline.py --stage all` e recarregue.")
    st.code("\n".join(dados["faltando"]))
    st.stop()

ranking, fato, scr, decisao, ml = (dados[k] for k in ("ranking", "fato", "scr", "decisao", "ml"))
limiar = decisao["limiar_alto_risco"]
modelo, trilho, valores = decisao["modelo"], decisao["trilho_renegociacao"], decisao["valores"]
n_alto = decisao["contagem_acoes"].get("renegociar_priorizar", 0)
ranking = ranking.assign(acao=ranking["acao_recomendada"].map(ACOES).fillna(ranking["acao_recomendada"]))

st.title("Risco de crédito municipal no Pará × riqueza")
st.caption(
    f"Painel de decisão para a diretoria de crédito de cooperativas e bancos regionais. Lê só a camada Gold e os "
    f"relatórios do pipeline. Data de corte (t0): {decisao['t0_decisao']}."
)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Municípios em risco alto", f"{n_alto} de {decisao['municipios_elegiveis']}",
          help=f"Provisão ≥ {pct(limiar, 2)} da carteira de crédito no t0.")
k2.metric("Perdas provisionadas concentradas neles", pct(valores["participacao_provisao_risco_alto"]))
k3.metric("Precisão / recall da regra de renegociação", f"{pct(trilho['precisao'])} / {pct(trilho['recall'])}",
          help="Medidos no conjunto de teste (t0 de 2023-06 a 2024-06).")
k4.metric("AP do alerta de deterioração", num(modelo['ap_teste'], 3),
          help=f"Average precision no teste. Acaso (prevalência): {num(modelo['ap_prevalencia'], 3)}; "
               f"melhor regra simples: {num(max(modelo['ap_nivel_atual'], modelo['ap_tendencia']), 3)}.")

aba_decisao, aba_fila, aba_riqueza, aba_evolucao, aba_modelo = st.tabs(
    ["Decisão", "Fila de prioridade", "Riqueza × risco", "Evolução", "Modelo"]
)

with aba_decisao:
    campos = decisao["campos_frase"]
    st.subheader("Frase-resposta")
    for rotulo, chave in [
        ("Bases cruzadas", "bases"), ("Identificamos que", "identificamos"), ("Quem decide", "decisor"),
        ("Ação recomendada", "acao"), ("Prazo", "prazo"), ("Priorizando", "priorizando"),
        ("Se agir, o ganho esperado", "ganho"), ("Se errarmos, o custo", "custo_erro"),
    ]:
        st.markdown(f"**{rotulo}:** {campos[chave]}".replace("$", "\\$"))

    st.subheader("Vigilância de deterioração")
    st.caption(
        f"Municípios de maior probabilidade de piora nos próximos 6 meses. Nenhum atingiu o limiar de ação de "
        f"{pct(decisao['limiar_deterioracao'])}, então a recomendação é acompanhar mensalmente, sem restringir."
    )
    vigilancia = pd.DataFrame(decisao["vigilancia_deterioracao"])
    st.dataframe(
        vigilancia.assign(
            prob_deterioracao=vigilancia["prob_deterioracao"] * 100,
            razao_provisao_t0=vigilancia["razao_provisao_t0"] * 100,
            carteira_credito_reais=vigilancia["carteira_credito_reais"] / 1e6,
        ),
        hide_index=True,
        column_config={
            "nome_municipio": "Município",
            "prob_deterioracao": st.column_config.NumberColumn("Prob. de deterioração", format="%.1f%%"),
            "razao_provisao_t0": st.column_config.NumberColumn("Risco atual (provisão/carteira)", format="%.2f%%"),
            "carteira_credito_reais": st.column_config.NumberColumn("Carteira (R$ mi)", format="%.1f"),
        },
    )

with aba_fila:
    f1, f2, f3 = st.columns([2, 2, 1])
    mesos = f1.multiselect("Mesorregião", sorted(ranking["mesorregiao"].dropna().unique()))
    acoes = f2.multiselect("Ação recomendada", sorted(ranking["acao"].unique()))
    top_n = f3.slider("Municípios no gráfico", 5, 40, 20)

    fila = ranking.sort_values("ordem_prioridade")
    if mesos:
        fila = fila[fila["mesorregiao"].isin(mesos)]
    if acoes:
        fila = fila[fila["acao"].isin(acoes)]

    if fila.empty:
        st.info("Nenhum município para os filtros escolhidos.")
    else:
        topo = fila.head(top_n)
        st.subheader(f"Risco atual dos {len(topo)} primeiros da fila")
        st.caption(f"Provisão sobre a carteira no t0, em escala logarítmica. A linha marca o limiar de risco alto ({pct(limiar, 2)}).")
        base = alt.Chart(topo).encode(
            y=alt.Y("nome_municipio:N", sort=alt.SortField("ordem_prioridade"), title=None),
            x=alt.X("razao_provisao_t0:Q", scale=alt.Scale(type="log", nice=False), axis=alt.Axis(format=".0%", values=[0.02, 0.05, 0.1, 0.2, 0.5, 1]),
                    title="Provisão / carteira"),
        )
        pontos = base.mark_circle(size=110, opacity=1).encode(
            color=cor_acao(topo["acao"]),
            tooltip=[
                alt.Tooltip("ordem_prioridade:Q", title="Posição na fila"),
                alt.Tooltip("nome_municipio:N", title="Município"),
                alt.Tooltip("acao:N", title="Ação"),
                alt.Tooltip("razao_provisao_t0:Q", title="Provisão / carteira", format=".2%"),
                alt.Tooltip("prob_deterioracao:Q", title="Prob. de deterioração", format=".1%"),
                alt.Tooltip("carteira_credito_reais:Q", title="Carteira (R$)", format=",.0f"),
            ],
        )
        regra = alt.Chart(pd.DataFrame({"x": [limiar]})).mark_rule(color=TINTA_2, strokeDash=[4, 4]).encode(x="x:Q")
        st.altair_chart(estilo(regra + pontos, altura=max(220, 24 * len(topo))), width="stretch")

        st.subheader(f"Fila completa ({len(fila)} municípios)")
        tabela = fila.assign(
            razao_provisao_t0=fila["razao_provisao_t0"] * 100,
            prob_deterioracao=fila["prob_deterioracao"] * 100,
            carteira_credito_reais=fila["carteira_credito_reais"] / 1e6,
        )[["ordem_prioridade", "nome_municipio", "mesorregiao", "acao", "razao_provisao_t0", "prob_deterioracao",
           "carteira_credito_reais", "pib_per_capita_reais"]]
        st.dataframe(
            tabela,
            hide_index=True,
            column_config={
                "ordem_prioridade": "#",
                "nome_municipio": "Município",
                "mesorregiao": "Mesorregião",
                "acao": "Ação recomendada",
                "razao_provisao_t0": st.column_config.NumberColumn("Risco atual", format="%.2f%%"),
                "prob_deterioracao": st.column_config.ProgressColumn(
                    "Prob. de deterioração", format="%.1f%%", min_value=0, max_value=100),
                "carteira_credito_reais": st.column_config.NumberColumn("Carteira (R$ mi)", format="%.1f"),
                "pib_per_capita_reais": st.column_config.NumberColumn("PIB per capita (R$)", format="%.0f"),
            },
        )
        st.download_button("Baixar a fila filtrada (CSV)", tabela.to_csv(index=False).encode("utf-8-sig"),
                           "fila_prioridade.csv", "text/csv")

with aba_riqueza:
    riqueza = decisao["riqueza"]
    st.subheader("A riqueza não explica o nível de risco")
    st.caption(
        f"Cada ponto é um município no t0. Correlação de Spearman entre PIB per capita e provisão/carteira: "
        f"{num(riqueza['spearman_pib_pc_vs_razao_dez2024'])}. A linha marca o limiar de risco alto ({pct(limiar, 2)}). Eixos em escala logarítmica."
    )
    dispersao = alt.Chart(ranking.dropna(subset=["pib_per_capita_reais", "razao_provisao_t0"])).mark_circle(
        size=70, opacity=0.85, stroke="#fcfcfb", strokeWidth=1
    ).encode(
        x=alt.X("pib_per_capita_reais:Q", scale=alt.Scale(type="log", nice=False), axis=alt.Axis(format=",.0f", values=[5e3, 1e4, 2e4, 5e4, 1e5, 2e5, 5e5]),
                title="PIB per capita (R$)"),
        y=alt.Y("razao_provisao_t0:Q", scale=alt.Scale(type="log"), axis=alt.Axis(format=".1%", values=[0.001, 0.01, 0.1, 1]),
                title="Provisão / carteira"),
        color=cor_acao(ranking["acao"]),
        tooltip=[
            alt.Tooltip("nome_municipio:N", title="Município"),
            alt.Tooltip("mesorregiao:N", title="Mesorregião"),
            alt.Tooltip("acao:N", title="Ação"),
            alt.Tooltip("pib_per_capita_reais:Q", title="PIB per capita (R$)", format=",.0f"),
            alt.Tooltip("razao_provisao_t0:Q", title="Provisão / carteira", format=".2%"),
        ],
    )
    regra_y = alt.Chart(pd.DataFrame({"y": [limiar]})).mark_rule(color=TINTA_2, strokeDash=[4, 4]).encode(y="y:Q")
    st.altair_chart(estilo(regra_y + dispersao, altura=420), width="stretch")

    st.subheader("Por quintil de PIB per capita")
    st.caption("Q1 = quintil mais pobre, Q5 = mais rico (a faixa de PIB per capita aparece ao passar o mouse). Observações rotuladas da base ML-Ready (todas as datas de corte).")
    quintis = pd.DataFrame(riqueza["quintis"])
    quintis["rotulo"] = "Q" + quintis["quintil"].astype(str)
    quintis["faixa"] = [f"R$ {a:,.0f} a R$ {b:,.0f}".replace(",", ".")
                        for a, b in zip(quintis["pib_pc_min"], quintis["pib_pc_max"])]
    c1, c2 = st.columns(2)
    for coluna, campo, titulo in [(c1, "taxa_alto_risco", "% em risco alto"), (c2, "taxa_deterioracao", "% que deteriorou")]:
        barras = alt.Chart(quintis).mark_bar(color=AZUL, cornerRadiusTopLeft=4, cornerRadiusTopRight=4, size=36).encode(
            x=alt.X("rotulo:N", sort=None, title="Quintil de PIB per capita", axis=alt.Axis(labelAngle=0)),
            y=alt.Y(f"{campo}:Q", axis=alt.Axis(format=".0%"), title=titulo),
            tooltip=[alt.Tooltip("rotulo:N", title="Quintil"), alt.Tooltip("faixa:N", title="PIB per capita"),
                     alt.Tooltip(f"{campo}:Q", title=titulo, format=".1%"),
                     alt.Tooltip("observacoes:Q", title="Observações")],
        )
        coluna.altair_chart(estilo(barras, altura=260), width="stretch")
    st.caption(
        f"Controlando pelas demais variáveis, o PIB per capita não altera a chance de deterioração "
        f"(odds ratio de {num(riqueza['modelo']['odds_ratio_por_1_desvio'])} por desvio-padrão)."
    )

with aba_evolucao:
    nomes = ranking.set_index("cod_ibge_municipio")["nome_municipio"].sort_values()
    escolhido = st.selectbox("Comparar o estado com o município", nomes.index, format_func=nomes.get)

    com_dado = fato[fato["possui_dado_estban"]]
    estado = com_dado.groupby("data_competencia", as_index=False)[["provisao_reais", "carteira_credito_reais"]].sum()
    estado = estado.assign(razao=estado["provisao_reais"] / estado["carteira_credito_reais"], serie="Pará (agregado)")
    municipio = com_dado[com_dado["cod_ibge_municipio"] == escolhido].assign(
        razao=lambda d: d["razao_provisao"], serie=nomes[escolhido])
    series = pd.concat([estado[["data_competencia", "razao", "serie"]], municipio[["data_competencia", "razao", "serie"]]])

    st.subheader("Provisão sobre a carteira (ESTBAN)")
    st.caption(f"Série mensal de 2019 a 2024. A linha tracejada marca o limiar de risco alto ({pct(limiar, 2)}).")
    linhas = alt.Chart(series).mark_line(strokeWidth=2).encode(
        x=alt.X("data_competencia:T", title=None),
        y=alt.Y("razao:Q", axis=alt.Axis(format=".1%"), title="Provisão / carteira"),
        color=alt.Color("serie:N", scale=alt.Scale(domain=["Pará (agregado)", nomes[escolhido]], range=[AZUL, LARANJA])),
        tooltip=[alt.Tooltip("serie:N", title="Série"), alt.Tooltip("data_competencia:T", title="Competência", format="%m/%Y"),
                 alt.Tooltip("razao:Q", title="Provisão / carteira", format=".2%")],
    )
    st.altair_chart(estilo(regra_y + linhas), width="stretch")

    st.subheader("Inadimplência no Pará por segmento (SCR.data)")
    st.caption("Contexto estadual: o SCR não tem abertura por município.")
    segmentos = {"inadimplencia_pf_pa": "Pessoa física", "inadimplencia_pj_pa": "Pessoa jurídica",
                 "inadimplencia_rural_pa": "Rural"}
    scr_longo = scr.melt("data_competencia", list(segmentos), "segmento", "inadimplencia")
    scr_longo["segmento"] = scr_longo["segmento"].map(segmentos)
    linhas_scr = alt.Chart(scr_longo).mark_line(strokeWidth=2).encode(
        x=alt.X("data_competencia:T", title=None),
        y=alt.Y("inadimplencia:Q", axis=alt.Axis(format=".1%"), title="Inadimplência"),
        color=alt.Color("segmento:N", scale=alt.Scale(domain=list(segmentos.values()), range=[AZUL, LARANJA, VERDE_AGUA])),
        tooltip=[alt.Tooltip("segmento:N", title="Segmento"), alt.Tooltip("data_competencia:T", title="Competência", format="%m/%Y"),
                 alt.Tooltip("inadimplencia:Q", title="Inadimplência", format=".2%")],
    )
    st.altair_chart(estilo(linhas_scr), width="stretch")

with aba_modelo:
    temporal = ml["cenarios"]["temporal"]
    resultados = pd.DataFrame([
        {"modelo": MODELOS.get(nome, nome) + (" (escolhido)" if nome == ml["modelo_escolhido"] else ""),
         "ap": r["average_precision"], "ic_min": r["ic95_ap"][0], "ic_max": r["ic95_ap"][1]}
        for nome, r in temporal["resultados"].items() if "average_precision" in r
    ]).sort_values("ap", ascending=False)

    st.subheader("Alerta de deterioração: modelo contra baselines")
    st.caption(
        f"Average precision no teste temporal ({temporal['n_teste']} observações, prevalência de "
        f"{pct(modelo['ap_prevalencia'])}). A barra fina é o intervalo de 95% por bootstrap."
    )
    eixo_modelo = alt.Y("modelo:N", sort=None, title=None, axis=alt.Axis(labelLimit=260))
    barras_ap = alt.Chart(resultados).mark_bar(color=AZUL, cornerRadiusTopRight=4, cornerRadiusBottomRight=4, size=18).encode(
        y=eixo_modelo, x=alt.X("ap:Q", title="Average precision (teste)"),
        tooltip=[alt.Tooltip("modelo:N", title="Modelo"), alt.Tooltip("ap:Q", title="AP", format=".3f"),
                 alt.Tooltip("ic_min:Q", title="IC95 mín.", format=".3f"), alt.Tooltip("ic_max:Q", title="IC95 máx.", format=".3f")],
    )
    intervalos = alt.Chart(resultados).mark_rule(color=TINTA, strokeWidth=1.5).encode(y=eixo_modelo, x="ic_min:Q", x2="ic_max:Q")
    st.altair_chart(estilo(barras_ap + intervalos, altura=40 * len(resultados) + 40), width="stretch")

    m1, m2 = st.columns(2)
    with m1:
        st.subheader("Custo por limiar (validação)")
        st.caption(f"Limiar escolhido: {pct(ml['limiar_escolhido'])} de probabilidade (linha tracejada).")
        curva = pd.DataFrame(ml["curva_custo_validacao"])
        linha_custo = alt.Chart(curva).mark_line(color=AZUL, strokeWidth=2).encode(
            x=alt.X("limiar:Q", axis=alt.Axis(format=".0%"), title="Limiar de probabilidade"),
            y=alt.Y("custo:Q", title="Custo (unidades de carteira)"),
            tooltip=[alt.Tooltip("limiar:Q", title="Limiar", format=".1%"), alt.Tooltip("custo:Q", title="Custo", format=".3f"),
                     alt.Tooltip("sinalizados:Q", title="Sinalizados")],
        )
        regra_limiar = alt.Chart(pd.DataFrame({"x": [ml["limiar_escolhido"]]})).mark_rule(
            color=TINTA_2, strokeDash=[4, 4]).encode(x="x:Q")
        st.altair_chart(estilo(linha_custo + regra_limiar, altura=280), width="stretch")
    with m2:
        st.subheader("O que mais pesa no alerta")
        st.caption("Importância por permutação: queda média de AP ao embaralhar a variável.")
        importancia = pd.DataFrame(temporal["importancia_permutacao"]).nlargest(10, "queda_ap_media")
        barras_imp = alt.Chart(importancia).mark_bar(color=AZUL, cornerRadiusTopRight=4, cornerRadiusBottomRight=4, size=16).encode(
            y=alt.Y("feature:N", sort=None, title=None, axis=alt.Axis(labelLimit=220)),
            x=alt.X("queda_ap_media:Q", title="Queda média de AP"),
            tooltip=[alt.Tooltip("feature:N", title="Variável"), alt.Tooltip("queda_ap_media:Q", title="Queda de AP", format=".4f"),
                     alt.Tooltip("desvio:Q", title="Desvio", format=".4f")],
        )
        st.altair_chart(estilo(barras_imp, altura=280), width="stretch")

    st.subheader("Regra de renegociação no teste")
    st.caption(trilho["observacao"])
    st.dataframe(
        pd.DataFrame(
            {"Previsto: risco alto": [trilho["vp"], trilho["fp"]], "Previsto: não": [trilho["fn"], trilho["vn"]]},
            index=["Real: risco alto", "Real: não"],
        )
    )
    custo, nao_agir = modelo["custo_teste"], modelo["custo_nao_agir_teste"]
    st.caption(
        f"No trilho de concessão, o modelo sinalizou {custo['sinalizados']} observações no teste "
        f"({custo['vp']} acerto, {custo['fp']} falsos positivos, {custo['fn']} falsos negativos). Custo total de "
        f"{reais(custo['custo_total_reais'])}, contra {reais(nao_agir['custo_total_reais'])} sem agir: economia de "
        f"{reais(valores['economia_teste_reais'])}."
    )
