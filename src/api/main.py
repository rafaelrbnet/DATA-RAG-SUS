# API FastAPI: endpoint POST /query (question -> sql, result, explanation).
# TODO: integrar rag.agent e retornar JSON conforme spec em docs/06-etapas-implementacao.md.

from fastapi import FastAPI

app = FastAPI(
    title="SUS Data RAG API",
    description="Perguntas em linguagem natural sobre dados do SUS (Parquet + DuckDB + LLM)",
    version="0.1.0",
)


@app.get("/health")
def health():
    return {"status": "ok"}
