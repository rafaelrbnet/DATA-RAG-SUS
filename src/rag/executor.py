"""Execução de SQL em DuckDB sobre a camada canônica data/processed."""

from __future__ import annotations

from pathlib import Path
import re

import duckdb
import pandas as pd


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def _default_data_root() -> Path:
    return _project_root() / "data" / "processed"


def _validate_sql(sql: str) -> str:
    if not isinstance(sql, str):
        raise TypeError("sql must be a string")
    text = sql.strip()
    if not text:
        raise ValueError("sql must not be empty")
    return text


def _validate_identifier(name: str) -> str:
    if not isinstance(name, str):
        raise TypeError("view_name must be a string")
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        raise ValueError("view_name must be a valid SQL identifier")
    return name


def query(
    sql: str,
    *,
    data_root: str | Path | None = None,
    view_name: str = "processed",
) -> pd.DataFrame:
    """
    Executa SQL em DuckDB lendo diretamente data/processed/**/*.parquet.

    A função cria uma view temporária (`processed` por padrão) apontando para
    todos os Parquets da base processada e retorna o resultado como DataFrame.
    """
    text = _validate_sql(sql)
    view = _validate_identifier(view_name)
    base = Path(data_root) if data_root is not None else _default_data_root()
    if not base.is_dir():
        raise FileNotFoundError(f"data root not found: {base}")

    parquet_glob = str((base / "**" / "*.parquet").as_posix())
    safe_glob = parquet_glob.replace("'", "''")

    with duckdb.connect(database=":memory:") as con:
        con.execute(
            f"CREATE OR REPLACE VIEW {view} AS "
            f"SELECT * FROM read_parquet('{safe_glob}', union_by_name=true)"
        )
        return con.execute(text).df()
