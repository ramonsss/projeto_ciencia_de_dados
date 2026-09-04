from pathlib import Path

import pandas as pd

from config.settings import BRONZE_DIR, SILVER_DIR


def _read_bronze(source_system: str, source_object: str) -> pd.DataFrame:
    path = BRONZE_DIR / source_system / f"{source_object}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Arquivo bronze não encontrado: {path}")
    return pd.read_parquet(path)


def build_mortalidade_por_estado() -> pd.DataFrame:
    """Agrega mortalidade por UF e calcula taxa por 100 mil habitantes."""
    sim_df = _read_bronze("sim", "mortalidade")
    ibge_df = _read_bronze("ibge", "populacao_estimada")

    if sim_df.empty or ibge_df.empty:
        return pd.DataFrame()

    sim_limpo = sim_df.copy()
    sim_limpo["CODMUNRES"] = sim_limpo["CODMUNRES"].astype(str).str.zfill(7)
    sim_limpo["codigo_uf"] = sim_limpo["CODMUNRES"].str[:2].astype(int)

    mortalidade_uf = (
        sim_limpo.groupby(["ano_referencia", "codigo_uf"], dropna=True)
        .size()
        .reset_index(name="mortes")
    )

    pop_por_uf = ibge_df[["codigo_uf", "uf", "populacao"]].copy()
    pop_por_uf["codigo_uf"] = pd.to_numeric(pop_por_uf["codigo_uf"], errors="coerce")
    pop_por_uf["populacao"] = pd.to_numeric(pop_por_uf["populacao"], errors="coerce")

    df = mortalidade_uf.merge(pop_por_uf, on="codigo_uf", how="left")
    df = df.dropna(subset=["uf", "populacao"]).copy()
    df["ano_referencia"] = df["ano_referencia"].astype(str)
    df["taxa_mortalidade_por_100k"] = (df["mortes"] / df["populacao"]) * 100000

    df = df.sort_values(["ano_referencia", "taxa_mortalidade_por_100k"], ascending=[True, False]).reset_index(drop=True)

    output_path = SILVER_DIR / "mortalidade_estado.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    return df
