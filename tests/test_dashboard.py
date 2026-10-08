import pytest

from credito_pa.config import PROJECT_ROOT, load_settings

APP = PROJECT_ROOT / "dashboard" / "app.py"


def test_dashboard_renderiza_sem_erro():
    testing = pytest.importorskip("streamlit.testing.v1")
    settings = load_settings()
    saidas = [settings.data_dir / "gold" / "ranking_priorizacao_municipios.parquet", settings.reports_dir / "decisao.json",
              settings.reports_dir / "ml_resultados.json"]
    if not all(p.exists() for p in saidas):
        pytest.skip("rode o pipeline (--stage all) para gerar a Gold e os relatórios")

    at = testing.AppTest.from_file(str(APP), default_timeout=60).run()

    assert not at.exception, [e.value for e in at.exception]
    assert not at.error
    assert len(at.metric) == 4
    assert len(at.tabs) == 5
