# 6. Etapas de Implementação

## ETAPA 1 — Setup do ambiente

- Criar projeto Python com `pyproject.toml`
- Instalar: `duckdb`, `pandas`, `pyarrow`, `fastapi`, `uvicorn`, `langchain` (ou `llamaindex`), `python-dotenv`

## ETAPA 2 — Pipeline de dados

- **Script R** (`scripts/r/analise_ortopedia.R`): única fonte de download do DATASUS (microdatasus); grava Parquets em `data/downloaded/` (ex.: `sih_SP_2021_01.parquet`, `sia_SP_2021_01.parquet`).
- **ingest.py**: lê `data/downloaded/` (lista `.parquet`), move cada arquivo para `data/raw/ano=X/uf=Y/sistema=SIH|SIA/` (particionado). Não baixa do DATASUS; um arquivo por vez, controle de memória.
- **transform.py**: lê `data/raw/` (particionado), padroniza tipos e adiciona colunas derivadas (`custo_total`, `idade_grupo`, `cid_capitulo`), grava em `data/processed/` com a mesma estrutura de partições. Um arquivo por vez para controle de memória (ex.: 16 GB RAM).

**Logs:** R e Python usam o mesmo arquivo `logs/erros.log`. Formato de cada linha: `quando (ISO) | quem (Script R ou Python) | onde (componente/caminho) | o que aconteceu`.

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
