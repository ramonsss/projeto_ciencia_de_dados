import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline.gold import build_gold_summary
from src.pipeline.silver import build_mortalidade_por_estado


if __name__ == "__main__":
    print("Gerando camada Silver...")
    build_mortalidade_por_estado()
    print("Gerando camada Gold...")
    resultado = build_gold_summary()
    print(resultado["resumo"])
