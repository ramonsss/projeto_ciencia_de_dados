import importlib

from credito_pa.common.io import write_text_atomic
from credito_pa.config import PROJECT_ROOT, load_settings
from credito_pa.quality.docs import render_dictionary
from credito_pa.silver.contracts import SILVER_CONTRACTS


def main() -> None:
    layers = {"Silver": SILVER_CONTRACTS}
    try:
        layers["Gold"] = importlib.import_module("credito_pa.gold.contracts").GOLD_CONTRACTS
    except ModuleNotFoundError:
        pass
    path = PROJECT_ROOT / "docs" / "dicionario_dados.md"
    write_text_atomic(render_dictionary(load_settings(), layers), path)
    print(f"Dicionário gerado: {path}")


if __name__ == "__main__":
    main()
