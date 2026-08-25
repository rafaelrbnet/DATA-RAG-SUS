"""Testes do gerador de SQL — sem chamadas reais à API da OpenAI."""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest
from openai import RateLimitError

from src.rag.sql_generator import _extract_sql, _invoke_with_retry, _validate_select_only


def _rate_limit_error() -> RateLimitError:
    request = httpx.Request("POST", "https://api.openai.com/v1/chat/completions")
    response = httpx.Response(429, request=request)
    return RateLimitError("rate limit exceeded", response=response, body=None)


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


# ---------------------------------------------------------------------------
# _invoke_with_retry
# ---------------------------------------------------------------------------

def test_invoke_with_retry_succeeds_first_try():
    llm = MagicMock()
    llm.invoke.return_value = "ok"
    assert _invoke_with_retry(llm, []) == "ok"
    assert llm.invoke.call_count == 1


def test_invoke_with_retry_recovers_after_rate_limit(monkeypatch):
    monkeypatch.setattr("src.rag.sql_generator.time.sleep", lambda _: None)
    llm = MagicMock()
    llm.invoke.side_effect = [_rate_limit_error(), _rate_limit_error(), "ok"]
    assert _invoke_with_retry(llm, []) == "ok"
    assert llm.invoke.call_count == 3


def test_invoke_with_retry_raises_after_exhausting_attempts(monkeypatch):
    monkeypatch.setattr("src.rag.sql_generator.time.sleep", lambda _: None)
    llm = MagicMock()
    llm.invoke.side_effect = _rate_limit_error()
    with pytest.raises(RateLimitError):
        _invoke_with_retry(llm, [])
    from src.rag.sql_generator import _MAX_RATE_LIMIT_ATTEMPTS
    assert llm.invoke.call_count == _MAX_RATE_LIMIT_ATTEMPTS
