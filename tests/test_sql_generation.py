"""Testes do gerador de SQL — sem chamadas reais à API da OpenAI."""

from __future__ import annotations

import pytest

from src.rag.sql_generator import _extract_sql, _validate_select_only


# ---------------------------------------------------------------------------
# _extract_sql
# ---------------------------------------------------------------------------

def test_extract_sql_from_fenced_block():
    text = "Aqui está o SQL:\n```sql\nSELECT COUNT(*) FROM processed\n```"
    assert _extract_sql(text) == "SELECT COUNT(*) FROM processed"


def test_extract_sql_from_bare_select():
    text = "SELECT uf_origem, COUNT(*) FROM processed GROUP BY 1"
    assert _extract_sql(text).startswith("SELECT")


def test_extract_sql_strips_whitespace():
    text = "```sql\n  SELECT 1  \n```"
    assert _extract_sql(text) == "SELECT 1"


def test_extract_sql_raises_when_no_sql():
    with pytest.raises(ValueError, match="Não foi possível extrair SQL"):
        _extract_sql("Desculpe, não entendi a pergunta.")


# ---------------------------------------------------------------------------
# _validate_select_only
# ---------------------------------------------------------------------------

def test_validate_passes_for_select():
    _validate_select_only("SELECT COUNT(*) FROM processed WHERE sistema = 'SIH'")


def test_validate_raises_for_delete():
    with pytest.raises(ValueError, match="DELETE"):
        _validate_select_only("DELETE FROM processed WHERE 1=1")


def test_validate_raises_for_drop():
    with pytest.raises(ValueError, match="DROP"):
        _validate_select_only("SELECT 1; DROP TABLE processed")


def test_validate_raises_for_non_select():
    with pytest.raises(ValueError, match="Somente SELECT"):
        _validate_select_only("UPDATE processed SET custo_total = 0")


def test_validate_raises_for_insert():
    with pytest.raises(ValueError, match="INSERT"):
        _validate_select_only("INSERT INTO processed VALUES (1)")
