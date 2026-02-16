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
- `fastapi` + `uvicorn` — API REST
- `langchain` ou `llamaindex` — orquestração do agente
- `python-dotenv` — variáveis de ambiente

[← Voltar ao índice](README.md)
