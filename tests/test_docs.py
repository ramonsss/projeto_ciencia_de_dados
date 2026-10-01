import importlib

import pytest

from credito_pa.config import PROJECT_ROOT
from credito_pa.silver.contracts import SILVER_CONTRACTS

DICT = PROJECT_ROOT / "docs" / "dicionario_dados.md"


def _contracts():
    contracts = list(SILVER_CONTRACTS)
    try:
        contracts += importlib.import_module("credito_pa.gold.contracts").GOLD_CONTRACTS
    except ModuleNotFoundError:
        pass
    return contracts


@pytest.mark.parametrize("contract", _contracts(), ids=lambda c: c.name)
def test_dicionario_cobre_todas_as_colunas(contract):
    text = DICT.read_text(encoding="utf-8")
    assert f"`{contract.layer}.{contract.name}`" in text, "rode: python scripts/gerar_docs.py"
    faltando = [c.name for c in contract.columns if f"| `{c.name}` |" not in text]
    assert faltando == [], f"colunas sem documentação em {contract.name}: {faltando}"


def test_numeros_de_decisao_batem_com_o_pipeline():
    import json

    rel = PROJECT_ROOT / "reports" / "decisao.json"
    if not rel.exists():
        pytest.skip("rode o pipeline (--stage all) para gerar reports/decisao.json")
    d = json.loads(rel.read_text(encoding="utf-8"))
    doc = (PROJECT_ROOT / "docs" / "decisao.md").read_text(encoding="utf-8")

    def pct(v, casas=1):
        return f"{v * 100:.{casas}f}%".replace(".", ",")

    t1, m, v = d["trilho_renegociacao"], d["modelo"], d["valores"]
    esperados = [
        f"{d['contagem_acoes'].get('renegociar_priorizar', 0)} dos {d['municipios_elegiveis']} municípios",
        pct(v["participacao_provisao_risco_alto"]), pct(t1["precisao"]), pct(t1["recall"]),
        pct(d["limiar_deterioracao"]), f"{m['ap_teste']:.3f}".replace(".", ","),
        f"R$ {v['economia_teste_reais']:,.0f}".replace(",", "."),
    ]
    faltando = [e for e in esperados if e not in doc]
    assert faltando == [], f"docs/decisao.md desatualizado em relação ao pipeline: {faltando}"


def test_relatorio_de_cruzamento_tem_secoes_obrigatorias():
    text = (PROJECT_ROOT / "docs" / "relatorio_cruzamento.md").read_text(encoding="utf-8")
    for secao in ["Chave de cruzamento", "casados", "Órfãos", "Tratamento", "riqueza da base"]:
        assert secao.lower() in text.lower()
