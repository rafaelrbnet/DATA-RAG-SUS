# 6. Etapas de Implementação

## ETAPA 1 — Setup do ambiente

- Criar projeto Python com `pyproject.toml`
- Instalar: `duckdb`, `pandas`, `pyarrow`, `fastapi`, `uvicorn`, `langchain` (ou `llamaindex`), `python-dotenv`

## ETAPA 2 — Pipeline de dados

- **ingest.py**: ler CSV do DATASUS → Parquet particionado por ano, UF, sistema (SIH, SIM, SIA)
- **transform.py**: padronizar tipos; colunas derivadas (`custo_total`, `idade_grupo`, `cid_capitulo`)

## ETAPA 3 — Camada DuckDB

- Função `query(sql: str) -> pd.DataFrame`
- Ler diretamente `data/processed/*.parquet`
- Sem banco externo; resposta em milissegundos

## ETAPA 4 — Agente LLM

- Recebe pergunta em português
- Analisa schema disponível
- Gera SQL seguro
- Executa no DuckDB
- Retorna resposta explicada
- Restrições: nunca inventar colunas; nunca estimar valores; sempre SQL executável

## ETAPA 5 — API

- Endpoint `POST /query`
- Entrada: `{ "question": "..." }`
- Saída: `{ "sql": "...", "result": "...", "explanation": "..." }`

[← Voltar ao índice](README.md)
