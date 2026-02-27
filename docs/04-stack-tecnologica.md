# 4. Stack Tecnológica

| Componente | Tecnologia |
|------------|------------|
| **Linguagem** | Python 3.11+ |
| **Motor analítico** | DuckDB |
| **Formato de dados** | Parquet |
| **Orquestração LLM** | LangChain ou LlamaIndex |
| **Modelo inicial** | OpenAI ou LLM local (Ollama) |
| **API** | FastAPI |

## Dependências principais

- `duckdb` — consultas analíticas em Parquet
- `pandas` — manipulação de dados
- `pyarrow` — leitura/escrita Parquet
- `datasus-dbc` + `dbfread` — ingestão DATASUS em Python (DBC -> DBF -> Parquet)
- `fastapi` + `uvicorn` — API REST
- `langchain` ou `llamaindex` — orquestração do agente
- `python-dotenv` — variáveis de ambiente

## Observação sobre ingestão

- A ingestão padrão é Python (`python -m src.data.ingestion`).
- Script R (`scripts/r/fallback_download_only.R`) é mantido como fallback pontual, apenas para download.

[← Voltar ao índice](README.md)
