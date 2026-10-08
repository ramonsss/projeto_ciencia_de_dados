from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

from credito_pa.quality.checks import ContractError, validate_schema
from credito_pa.quality.contracts import TableContract

DIMENSOES = ("completude", "unicidade", "validade", "consistencia", "integridade", "atualidade")
ERROR, WARNING = "ERROR", "WARNING"
PASSOU, ALERTOU, FALHOU = "PASSOU", "ALERTOU", "FALHOU"


@dataclass
class ResultadoRegra:
    regra: str
    dimensao: str
    total: int
    erros: int
    descricao: str = ""
    severidade: str = ERROR
    threshold: float = 0.0

    @property
    def erro_ratio(self) -> float:
        return round(self.erros / self.total, 6) if self.total else 0.0

    @property
    def status(self) -> str:
        if self.erro_ratio <= self.threshold:
            return PASSOU
        return FALHOU if self.severidade == ERROR else ALERTOU

    def as_dict(self) -> dict:
        return {"regra": self.regra, "dimensao": self.dimensao, "severidade": self.severidade, "total": self.total,
                "erros": self.erros, "erro_ratio": self.erro_ratio, "threshold": self.threshold, "status": self.status,
                "descricao": self.descricao}


@dataclass
class ResultadoTabela:
    camada: str
    tabela: str
    linhas: int
    quarentena: int = 0
    linhas_reprovadas: int = 0
    resultados: list[ResultadoRegra] = field(default_factory=list)
    perfil: list[dict] = field(default_factory=list)
    extras: dict = field(default_factory=dict)

    @property
    def status(self) -> str:
        situacoes = {r.status for r in self.resultados}
        return FALHOU if FALHOU in situacoes else ALERTOU if ALERTOU in situacoes else PASSOU

    @property
    def score(self) -> float:
        return round(100 * (1 - self.linhas_reprovadas / self.linhas), 2) if self.linhas else 100.0

    def as_dict(self) -> dict:
        return {"camada": self.camada, "tabela": self.tabela, "linhas": self.linhas, "quarentena": self.quarentena,
                "linhas_reprovadas": self.linhas_reprovadas, "score": self.score, "status": self.status, **self.extras,
                "resultados": [r.as_dict() for r in self.resultados], "perfil": self.perfil}


def avaliar(df: pl.DataFrame, regras: list[tuple[str, str, str, pl.Expr]]) -> tuple[list[ResultadoRegra], int]:
    """Cada regra é (nome, dimensão, descrição, expressão que marca a linha INVÁLIDA)."""
    if not regras or df.is_empty():
        return [ResultadoRegra(nome, dimensao, df.height, 0, descricao) for nome, dimensao, descricao, _ in regras], 0
    invalidas = [expr.fill_null(False).alias(f"r{i}") for i, (*_, expr) in enumerate(regras)]
    marcado = df.select(invalidas)
    contagem = marcado.sum().row(0)
    reprovadas = marcado.select(pl.any_horizontal(pl.all()).sum()).item()
    resultados = [ResultadoRegra(nome, dimensao, df.height, int(n), descricao)
                  for (nome, dimensao, descricao, _), n in zip(regras, contagem)]
    return resultados, int(reprovadas)


def regras_do_contrato(contract: TableContract, colunas: list[str], chaves_dim: list[str] | None = None) -> list[tuple]:
    regras = []
    for col in contract.columns:
        if col.name not in colunas:
            continue
        if not col.nullable:
            regras.append((f"completude::{col.name}", "completude", f"{col.name} deve estar preenchido",
                           pl.col(col.name).is_null()))
        if col.check is not None:
            regras.append((f"validade::{col.name}", "validade", f"{col.name} no domínio: {col.domain or 'ver contrato'}",
                           pl.col(col.name).is_not_null() & ~col.check.fill_null(False)))
    pk = contract.primary_key
    if all(c in colunas for c in pk):
        regras.append((f"unicidade::{'+'.join(pk)}", "unicidade", f"a chave {pk} não pode se repetir",
                       pl.struct(pk).is_duplicated()))
    if chaves_dim is not None and "cod_ibge_municipio" in colunas and contract.name != "dim_municipio":
        regras.append(("integridade::cod_ibge_municipio_em_dim_municipio", "integridade",
                       "cod_ibge_municipio deve existir em silver.dim_municipio",
                       ~pl.col("cod_ibge_municipio").is_in(chaves_dim)))
    return regras


def validar_contrato(df: pl.DataFrame, contract: TableContract, chaves_dim: list[str] | None = None):
    try:
        validate_schema(df, contract)
        schema = ResultadoRegra("consistencia::schema_do_contrato", "consistencia", 1, 0, "colunas e tipos iguais ao contrato")
    except ContractError as erro:
        schema = ResultadoRegra("consistencia::schema_do_contrato", "consistencia", 1, 1, str(erro))
    resultados, reprovadas = avaliar(df, regras_do_contrato(contract, df.columns, chaves_dim))
    return [schema, *resultados], reprovadas


def perfilar(df: pl.DataFrame) -> list[dict]:
    perfil = []
    for nome, dtype in df.schema.items():
        serie = df[nome]
        distintos = serie.n_unique() - (1 if serie.null_count() else 0)
        faixa = dtype.is_numeric() or dtype.is_temporal()
        perfil.append({
            "coluna": nome, "tipo": str(dtype), "nulos": serie.null_count(),
            "nulo_ratio": round(serie.null_count() / df.height, 4) if df.height else 0.0, "distintos": distintos,
            "chave_candidata": bool(df.height and serie.null_count() == 0 and distintos == df.height),
            "minimo": str(serie.min()) if faixa and distintos else None,
            "maximo": str(serie.max()) if faixa and distintos else None,
        })
    return perfil
