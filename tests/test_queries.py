from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.rag.executor import query


def _write_sample_parquet(base: Path) -> None:
    target = base / "ano=2025" / "uf=SP"
    target.mkdir(parents=True, exist_ok=True)
    file_path = target / "sus_SP_2025_01.parquet"
    with duckdb.connect(database=":memory:") as con:
        con.execute(
            """
            COPY (
              SELECT * FROM (
                VALUES
                  (34, 'M', '1', NULL, '00123456789012', 'M', '04', 'B20', '3550308', '3550308', '1234', '030101', 'A10', 120.0, 202501),
                  (51, 'F', '2', NULL, '00123456789012', 'E', '05', 'I10', '3550308', '3550308', '1234', '030101', 'A10', 80.0, 202501),
                  (29, 'M', '1', NULL, '00987654321000', 'M', '04', 'J45', '3304557', '3304557', '5678', '040201', 'E11', 50.0, 202501)
              ) AS t(
                idade_paciente, sexo_paciente, raca_cor_paciente, etnia_paciente,
                cnpj_mantenedora, gestao_responsavel, tipo_financiamento, cid_secundario, cod_munic_residencia,
                cod_munic_estabelecimento, cnes_estabelecimento, cod_procedimento,
                cid_principal, custo_total, competencia_ano_mes
              )
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
        "SELECT SUM(custo_total) AS total FROM processed WHERE sexo_paciente = 'M'",
        data_root=tmp_path,
    )
    assert df.shape == (1, 1)
    assert float(df.loc[0, "total"]) == 170.0


def test_query_requires_non_empty_sql(tmp_path: Path) -> None:
    _write_sample_parquet(tmp_path)
    with pytest.raises(ValueError):
        query("   ", data_root=tmp_path)


def test_query_requires_existing_data_root(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist"
    with pytest.raises(FileNotFoundError):
        query("SELECT 1", data_root=missing)


def test_query_works_with_schema_mismatch_using_union_by_name(tmp_path: Path) -> None:
    s1 = tmp_path / "ano=2025" / "uf=SP"
    s2 = tmp_path / "ano=2025" / "uf=RJ"
    s1.mkdir(parents=True, exist_ok=True)
    s2.mkdir(parents=True, exist_ok=True)

    f1 = s1 / "sus_SP_2025_01.parquet"
    f2 = s2 / "sus_RJ_2025_01.parquet"

    with duckdb.connect(database=":memory:") as con:
        con.execute(
            """
            COPY (
                SELECT
                  30 AS idade_paciente, 'M' AS sexo_paciente, '1' AS raca_cor_paciente,
                  NULL AS etnia_paciente, '00123456789012' AS cnpj_mantenedora, 'M' AS gestao_responsavel,
                  '04' AS tipo_financiamento, 'B20' AS cid_secundario,
                  '3550308' AS cod_munic_residencia, '3550308' AS cod_munic_estabelecimento,
                  '9999' AS cnes_estabelecimento, '030101' AS cod_procedimento,
                  'A10' AS cid_principal, 100.0 AS custo_total, 202501 AS competencia_ano_mes
            ) TO ? (FORMAT PARQUET)
            """,
            [str(f1)],
        )
        con.execute(
            """
            COPY (
                SELECT
                  50.0 AS custo_total, 202501 AS competencia_ano_mes,
                  45 AS idade_paciente, 'F' AS sexo_paciente, '2' AS raca_cor_paciente,
                  NULL AS etnia_paciente, '00123456789012' AS cnpj_mantenedora, 'E' AS gestao_responsavel,
                  '05' AS tipo_financiamento, NULL AS cid_secundario,
                  '3304557' AS cod_munic_residencia, '3304557' AS cod_munic_estabelecimento,
                  '8888' AS cnes_estabelecimento, '040201' AS cod_procedimento
            ) TO ? (FORMAT PARQUET)
            """,
            [str(f2)],
        )

    df = query(
        """
        SELECT
          SUM(CASE WHEN cid_principal IS NULL OR TRIM(cid_principal) = '' THEN 1 ELSE 0 END) AS nulos_cid_principal,
          SUM(CASE WHEN custo_total IS NULL THEN 1 ELSE 0 END) AS nulos_custo_total,
          SUM(CASE WHEN competencia_ano_mes IS NULL THEN 1 ELSE 0 END) AS nulos_competencia_ano_mes
        FROM processed
        """,
        data_root=tmp_path,
    )
    assert int(df.loc[0, "nulos_cid_principal"]) == 1
    assert int(df.loc[0, "nulos_custo_total"]) == 0
    assert int(df.loc[0, "nulos_competencia_ano_mes"]) == 0


def test_query_fails_when_canonical_columns_are_missing(tmp_path: Path) -> None:
    target = tmp_path / "ano=2025" / "uf=SP"
    target.mkdir(parents=True, exist_ok=True)
    file_path = target / "sus_SP_2025_01.parquet"
    with duckdb.connect(database=":memory:") as con:
        con.execute(
            "COPY (SELECT 1 AS only_col) TO ? (FORMAT PARQUET)",
            [str(file_path)],
        )
    with pytest.raises(ValueError):
        query("SELECT 1", data_root=tmp_path)
