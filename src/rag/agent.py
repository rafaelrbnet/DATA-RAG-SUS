"""Orquestração do agente LLM: pergunta → SQL → DuckDB → explicação."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI

from .executor import query as execute_sql
from .prompts import EXPLAIN_PROMPT
from .sql_generator import generate_sql, _get_llm, _invoke_with_retry

load_dotenv()

_MAX_PREVIEW_ROWS = 20


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    """Serializa DataFrame para lista de dicts, convertendo NaN/Inf → None."""
    return json.loads(df.to_json(orient="records", default_handler=str))


def _result_preview(df: pd.DataFrame) -> str:
    if df.empty:
        return "(nenhuma linha retornada)"
    return df.head(_MAX_PREVIEW_ROWS).to_string(index=False)


def _explain(question: str, sql: str, df: pd.DataFrame) -> str:
    llm = _get_llm()
    prompt = EXPLAIN_PROMPT.format(
        question=question,
        sql=sql,
        row_count=len(df),
        result_preview=_result_preview(df),
    )
    response = _invoke_with_retry(llm, [HumanMessage(content=prompt)])
    return response.content.strip()


def run_query(
    question: str,
    *,
    data_root: str | Path | None = None,
) -> dict:
    """
    Pipeline completo: pergunta em português → SQL → execução DuckDB → explicação.

    Returns:
        dict com chaves:
          sql         (str | None)   SQL gerado
          result      (list | None)  linhas como lista de dicts
          explanation (str | None)   resposta em linguagem natural
          row_count   (int)          número de linhas retornadas
          error       (str | None)   mensagem de erro, se houver
    """
    out: dict = {
        "sql": None,
        "result": None,
        "explanation": None,
        "row_count": 0,
        "error": None,
    }

    try:
        out["sql"] = generate_sql(question)
    except Exception as exc:
        out["error"] = f"Falha na geração de SQL: {exc}"
        return out

    try:
        df = execute_sql(out["sql"], data_root=data_root)
        out["result"] = _df_to_records(df)
        out["row_count"] = len(df)
    except Exception as exc:
        out["error"] = f"Falha na execução do SQL: {exc}"
        return out

    try:
        out["explanation"] = _explain(question, out["sql"], df)
    except Exception as exc:
        out["explanation"] = f"(explicação indisponível: {exc})"

    return out
