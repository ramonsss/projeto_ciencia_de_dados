import pytest

from credito_pa.pipeline import run_stage


def test_etapa_desconhecida(settings):
    with pytest.raises(ValueError):
        run_stage(settings, "inexistente")


def test_fonte_desconhecida(settings):
    with pytest.raises(ValueError):
        run_stage(settings, "bronze", ["fonte_x"])
