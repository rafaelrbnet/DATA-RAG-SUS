# 4. Stack Tecnológica

| Componente | Tecnologia |
|------------|------------|
| **Linguagem** | Python 3.12 (gerenciado via `uv`) |
| **Motor analítico** | DuckDB |
| **Formato de dados** | Parquet |
| **Orquestração LLM** | LangChain + `langchain-openai` |
| **Modelo padrão** | Ollama local (`qwen2.5-coder:14b`) |
| **Modelo alternativo** | OpenAI (`gpt-4o`) via `OPENAI_API_KEY` |
| **API** | FastAPI |

## Dependências principais

- `duckdb` — consultas analíticas em Parquet
- `pandas` — manipulação de dados
- `pyarrow` — leitura/escrita Parquet
- `datasus-dbc` + `dbfread` — ingestão DATASUS em Python (DBC → DBF → Parquet)
- `fastapi` + `uvicorn` — API REST
- `langchain` + `langchain-openai` — orquestração do agente (cliente compatível com Ollama e OpenAI)
- `python-dotenv` — variáveis de ambiente

## Seleção de provedor LLM

Controlada pela variável `LLM_PROVIDER` em `.env`:

| `LLM_PROVIDER` | Variáveis necessárias | Observação |
|---|---|---|
| `ollama` (padrão) | `OLLAMA_BASE_URL`, `OLLAMA_MODEL` | Requer Ollama rodando localmente |
| `openai` | `OPENAI_API_KEY`, `OPENAI_MODEL` | Requer chave de API da OpenAI |

## Observação sobre ingestão

- A ingestão padrão é Python (`python -m src.data.ingestion`).
- Script R (`scripts/r/fallback_download_only.R`) é mantido como fallback pontual, apenas para download.

[← Voltar ao índice](README.md)
