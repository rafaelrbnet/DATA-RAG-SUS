"""Geração de SQL DuckDB a partir de pergunta em português via LLM (OpenAI ou Ollama local)."""

from __future__ import annotations

import os
import re

from dotenv import load_dotenv
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from .prompts import SCHEMA_CONTEXT, SYSTEM_PROMPT

load_dotenv()

_FORBIDDEN = re.compile(
    r"\b(DELETE|UPDATE|INSERT|DROP|CREATE|ALTER|TRUNCATE|EXEC|EXECUTE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)
_SQL_BLOCK = re.compile(r"```sql\s*(.*?)```", re.DOTALL | re.IGNORECASE)
_SQL_BARE_BLOCK = re.compile(r"```\s*((WITH|SELECT)\b.*?)```", re.DOTALL | re.IGNORECASE)

# SELECT válido = começa com SELECT ou WITH (CTE padrão SQL)
_VALID_START = re.compile(r"(?i)^\s*(SELECT|WITH)\b")


def _extract_sql(text: str) -> str:
    m = _SQL_BLOCK.search(text)
    if m:
        return m.group(1).strip()
    m = _SQL_BARE_BLOCK.search(text)
    if m:
        return m.group(1).strip()
    stripped = text.strip()
    if _VALID_START.match(stripped):
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
    if not _VALID_START.match(effective):
        raise ValueError(
            f"Somente SELECT/WITH é permitido. SQL recebido começava com: {effective[:80]}"
        )
    m = _FORBIDDEN.search(sql)
    if m:
        raise ValueError(
            f"SQL contém operação não permitida '{m.group(0)}'. "
            "Apenas SELECT é aceito."
        )


def _get_llm(temperature: float = 0.0) -> ChatOpenAI:
    provider = os.getenv("LLM_PROVIDER", "openai").lower()

    if provider == "ollama":
        base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
        model = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:14b")
        return ChatOpenAI(model=model, temperature=temperature, api_key="ollama", base_url=base_url)

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key or "COLOQUE" in api_key.upper():
        raise RuntimeError(
            "Configure LLM_PROVIDER=ollama no .env para uso local, "
            "ou forneça OPENAI_API_KEY para usar a OpenAI."
        )
    model = os.getenv("OPENAI_MODEL", "gpt-4o")
    return ChatOpenAI(model=model, temperature=temperature, api_key=api_key)


def generate_sql(question: str, *, temperature: float = 0.0) -> str:
    """
    Converte pergunta em português em SQL DuckDB válido via LLM configurado.

    Raises:
        RuntimeError: provedor não configurado corretamente.
        ValueError: SQL extraído não é SELECT, ou contém operações proibidas.
    """
    llm = _get_llm(temperature=temperature)
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


_VERIFY_INSTRUCTION = (
    "A query executou com sucesso e retornou {n} linha(s) com as colunas {cols}.\n"
    "Amostra do resultado:\n{preview}\n\n"
    "Verifique se o resultado responde à pergunta original de forma completa e com a "
    "estrutura mínima apropriada: as colunas que a pergunta pede presentes, sem colunas "
    "extras desnecessárias, agregação e granularidade corretas, ordenação e LIMIT quando "
    "a pergunta pedir top-N. "
    "Se estiver correto, responda exatamente APROVADO. "
    "Caso contrário, responda apenas com o SQL corrigido em um bloco ```sql```."
)

_REPAIR_INSTRUCTION = (
    "A query falhou no DuckDB com o erro:\n{error}\n\n"
    "Corrija o SQL. Responda apenas com o SQL corrigido em um bloco ```sql```."
)


def generate_sql_with_repair(
    question: str, *, temperature: float = 0.0, max_feedback_rounds: int = 2
) -> tuple[str, int]:
    """
    Gera SQL com loop de autocorreção por feedback de execução (Condição E).

    O feedback usa exclusivamente informação da própria execução (erro do DuckDB
    ou preview do resultado) e a pergunta original — nunca o gold standard.
    Fluxo: gera SQL → executa → se erro, repara; se sucesso, uma rodada de
    verificação estrutural (APROVADO ou SQL corrigido).

    Returns:
        (sql_final, rodadas_de_feedback_usadas)
    """
    from .executor import query as _execute

    llm = _get_llm(temperature=temperature)
    system_content = SYSTEM_PROMPT.format(schema=SCHEMA_CONTEXT)
    messages = [
        SystemMessage(content=system_content),
        HumanMessage(content=question),
    ]
    response = llm.invoke(messages)
    sql = _extract_sql(response.content)
    _validate_select_only(sql)

    rounds = 0
    verified = False
    while rounds < max_feedback_rounds:
        try:
            df = _execute(sql)
        except Exception as exc:
            rounds += 1
            messages += [
                AIMessage(content=f"```sql\n{sql}\n```"),
                HumanMessage(content=_REPAIR_INSTRUCTION.format(error=exc)),
            ]
            response = llm.invoke(messages)
            sql = _extract_sql(response.content)
            _validate_select_only(sql)
            continue

        if verified:
            break

        rounds += 1
        verified = True
        messages += [
            AIMessage(content=f"```sql\n{sql}\n```"),
            HumanMessage(content=_VERIFY_INSTRUCTION.format(
                n=len(df),
                cols=list(df.columns),
                preview=df.head(3).to_string(index=False),
            )),
        ]
        response = llm.invoke(messages)
        content = response.content.strip()
        if "APROVADO" in content.upper()[:40]:
            break
        try:
            new_sql = _extract_sql(content)
            _validate_select_only(new_sql)
            sql = new_sql
        except ValueError:
            break  # resposta não é SQL nem APROVADO — mantém SQL vigente

    return sql, rounds
