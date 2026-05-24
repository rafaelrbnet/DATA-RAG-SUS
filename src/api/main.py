"""API FastAPI — POST /query: pergunta em linguagem natural → SQL + resultado + explicação."""

from __future__ import annotations

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

load_dotenv()

app = FastAPI(
    title="SUS Data RAG API",
    description=(
        "Perguntas em linguagem natural sobre dados administrativos de saúde do SUS "
        "(SIH + SIA) via Code-Interpreter RAG: DuckDB + GPT-4o."
    ),
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=5,
        max_length=1000,
        description="Pergunta em português sobre os dados do SUS (SIH ou SIA).",
        examples=["Total de internações por fratura de fêmur (S72) em SP em 2022"],
    )


class QueryResponse(BaseModel):
    sql: str | None = Field(None, description="SQL DuckDB gerado pelo agente.")
    result: list | None = Field(None, description="Linhas do resultado como lista de objetos.")
    explanation: str | None = Field(None, description="Interpretação do resultado em linguagem natural.")
    row_count: int = Field(0, description="Número de linhas retornadas.")
    error: str | None = Field(None, description="Mensagem de erro, se houver.")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", tags=["infra"])
def health() -> dict:
    """Verifica disponibilidade da API."""
    return {"status": "ok"}


@app.post(
    "/query",
    response_model=QueryResponse,
    tags=["rag"],
    summary="Pergunta em linguagem natural sobre os dados do SUS",
    responses={
        200: {"description": "Consulta executada com sucesso (mesmo que sem linhas)."},
        422: {"description": "Pergunta inválida (muito curta, muito longa)."},
        500: {"description": "Erro interno — falha na geração de SQL ou API key ausente."},
    },
)
def query(request: QueryRequest) -> QueryResponse:
    """
    Recebe uma pergunta em português, gera SQL via GPT-4o, executa no DuckDB
    sobre os dados do DATASUS (SIH + SIA) e retorna o resultado explicado.

    **Exemplo de pergunta:** "Quantas internações ortopédicas ocorreram em SP em 2022?"
    """
    from src.rag.agent import run_query  # importação tardia — evita erros de inicialização

    result = run_query(request.question)

    if result["error"] and result["sql"] is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=result["error"],
        )

    return QueryResponse(**result)
