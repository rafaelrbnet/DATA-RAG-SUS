"""Geração de SQL DuckDB a partir de pergunta em português via LLM (OpenAI ou Ollama local)."""

from __future__ import annotations

import os
import re

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from .prompts import SCHEMA_CONTEXT, SYSTEM_PROMPT

load_dotenv()

_FORBIDDEN = re.compile(
    r"\b(DELETE|UPDATE|INSERT|DROP|CREATE|ALTER|TRUNCATE|EXEC|EXECUTE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)
_SQL_BLOCK = re.compile(r"```sql\s*(.*?)```", re.DOTALL | re.IGNORECASE)
_SQL_BARE_BLOCK = re.compile(r"```\s*(SELECT\b.*?)```", re.DOTALL | re.IGNORECASE)


def _extract_sql(text: str) -> str:
    m = _SQL_BLOCK.search(text)
    if m:
        return m.group(1).strip()
    m = _SQL_BARE_BLOCK.search(text)
    if m:
        return m.group(1).strip()
    stripped = text.strip()
    if re.match(r"(?i)^\s*SELECT\b", stripped):
        return stripped
    raise ValueError(
        f"Não foi possível extrair SQL da resposta do modelo. "
        f"Resposta recebida: {text[:300]}"
    )


def _strip_leading_comments(sql: str) -> str:
    lines = sql.splitlines()
    non_comment = next(
        (l for l in lines if l.strip() and not l.strip().startswith("--")), ""
    )
    return non_comment if not non_comment else sql[sql.index(non_comment):]


def _validate_select_only(sql: str) -> None:
    effective = _strip_leading_comments(sql)
    if not re.match(r"(?i)^\s*SELECT\b", effective):
        raise ValueError(
            f"Somente SELECT é permitido. SQL recebido começava com: {effective[:80]}"
        )
    m = _FORBIDDEN.search(sql)
    if m:
        raise ValueError(
            f"SQL contém operação não permitida '{m.group(0)}'. "
            "Apenas SELECT é aceito."
        )


def _get_llm() -> ChatOpenAI:
    provider = os.getenv("LLM_PROVIDER", "openai").lower()

    if provider == "ollama":
        base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
        model = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:14b")
        return ChatOpenAI(model=model, temperature=0, api_key="ollama", base_url=base_url)

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key or "COLOQUE" in api_key.upper():
        raise RuntimeError(
            "Configure LLM_PROVIDER=ollama no .env para uso local, "
            "ou forneça OPENAI_API_KEY para usar a OpenAI."
        )
    model = os.getenv("OPENAI_MODEL", "gpt-4o")
    return ChatOpenAI(model=model, temperature=0, api_key=api_key)


def generate_sql(question: str) -> str:
    """
    Converte pergunta em português em SQL DuckDB válido via LLM configurado.

    Raises:
        RuntimeError: provedor não configurado corretamente.
        ValueError: SQL extraído não é SELECT, ou contém operações proibidas.
    """
    llm = _get_llm()
    system_content = SYSTEM_PROMPT.format(schema=SCHEMA_CONTEXT)
    messages = [
        SystemMessage(content=system_content),
        HumanMessage(content=question),
    ]
    response = llm.invoke(messages)
    raw: str = response.content
    sql = _extract_sql(raw)
    _validate_select_only(sql)
    return sql
