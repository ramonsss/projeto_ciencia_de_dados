import argparse

from credito_pa.config import load_settings
from credito_pa.pipeline import BRONZE_SOURCES, ORDER_ALL, STAGES, run_stage


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline de risco de crédito municipal do Pará")
    parser.add_argument("--stage", required=True, choices=["bronze", "all", *STAGES], help=f"Etapa (all = {' -> '.join(ORDER_ALL)})")
    parser.add_argument("--source", action="append", choices=sorted(BRONZE_SOURCES),
                        help="Fonte(s) da Bronze (padrão: todas). Pode repetir.")
    args = parser.parse_args()
    run_stage(load_settings(), args.stage, args.source)


if __name__ == "__main__":
    main()
