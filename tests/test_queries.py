from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.rag.executor import query


def _write_sample_parquet(base: Path) -> None:
    target = base / "ano=2025" / "uf=SP" / "sistema=SIA"
    target.mkdir(parents=True, exist_ok=True)
    file_path = target / "sia_SP_2025_01.parquet"
    with duckdb.connect(database=":memory:") as con:
        con.execute(
            """
            COPY (
              SELECT * FROM (
                VALUES
                  ('SIA', 'SP', 120.0),
                  ('SIA', 'SP', 80.0),
                  ('SIH', 'RJ', 50.0)
              ) AS t(sistema, uf_origem, custo_total)
            ) TO ? (FORMAT PARQUET)
            """,
            [str(file_path)],
        )


def test_query_returns_dataframe(tmp_path: Path) -> None:
    _write_sample_parquet(tmp_path)
    df = query(
        "SELECT COUNT(*) AS n, SUM(custo_total) AS total FROM processed",
        data_root=tmp_path,
    )
    assert df.shape == (1, 2)
    assert int(df.loc[0, "n"]) == 3
    assert float(df.loc[0, "total"]) == 250.0


def test_query_supports_filters(tmp_path: Path) -> None:
    _write_sample_parquet(tmp_path)
    df = query(
        "SELECT SUM(custo_total) AS total FROM processed WHERE sistema = 'SIA'",
        data_root=tmp_path,
    )
    assert df.shape == (1, 1)
    assert float(df.loc[0, "total"]) == 200.0


def test_query_requires_non_empty_sql(tmp_path: Path) -> None:
    _write_sample_parquet(tmp_path)
    with pytest.raises(ValueError):
        query("   ", data_root=tmp_path)


def test_query_requires_existing_data_root(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist"
    with pytest.raises(FileNotFoundError):
        query("SELECT 1", data_root=missing)


def test_query_works_with_schema_mismatch_using_union_by_name(tmp_path: Path) -> None:
    s1 = tmp_path / "ano=2025" / "uf=SP" / "sistema=SIA"
    s2 = tmp_path / "ano=2025" / "uf=SP" / "sistema=SIH"
    s1.mkdir(parents=True, exist_ok=True)
    s2.mkdir(parents=True, exist_ok=True)

    f1 = s1 / "sia_SP_2025_01.parquet"
    f2 = s2 / "old-sih_SP_2025_01.parquet"

    with duckdb.connect(database=":memory:") as con:
        con.execute(
            "COPY (SELECT 'A10' AS main_icd, 100.0 AS custo_total, 202501 AS ano_mes) TO ? (FORMAT PARQUET)",
            [str(f1)],
        )
        con.execute(
            "COPY (SELECT 50.0 AS custo_total, 202501 AS ano_mes) TO ? (FORMAT PARQUET)",
            [str(f2)],
        )

    df = query(
        """
        SELECT
          SUM(CASE WHEN main_icd IS NULL OR TRIM(main_icd) = '' THEN 1 ELSE 0 END) AS nulos_main_icd,
          SUM(CASE WHEN custo_total IS NULL THEN 1 ELSE 0 END) AS nulos_custo_total,
          SUM(CASE WHEN ano_mes IS NULL THEN 1 ELSE 0 END) AS nulos_ano_mes
        FROM processed
        """,
        data_root=tmp_path,
    )
    assert int(df.loc[0, "nulos_main_icd"]) == 1
    assert int(df.loc[0, "nulos_custo_total"]) == 0
    assert int(df.loc[0, "nulos_ano_mes"]) == 0
