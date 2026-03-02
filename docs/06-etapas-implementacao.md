# 6. Etapas de Implementação

## ETAPA 1 — Setup do ambiente

**Status:** concluída.

- Criar projeto Python com `pyproject.toml`
- Instalar: `duckdb`, `pandas`, `pyarrow`, `fastapi`, `uvicorn`, `langchain` (ou `llamaindex`), `python-dotenv`

## ETAPA 2 — Pipeline de dados: ingestão

**Status:** concluída.

- **ingestion.py** (`python -m src.data.ingestion`): ingestão principal 100% Python. Faz diff da grade esperada com `data/raw/`, baixa DBC do DATASUS (FTP nativo com fallback S3), descompacta DBC->DBF, filtra em chunks e grava Parquet particionado em `data/raw/ano=X/uf=Y/sistema=SIH|SIA/`.
- **Retentativa e resiliência:** pode incluir alvos vindos de `logs/erros.log`; aplica retries, backoff/jitter, circuit breaker para timeout e ignora arquivos com muitas falhas recorrentes no log.
- **Fallback R (pontual):** quando o arquivo não existe no FTP/S3, o `ingestion.py` pode acionar `scripts/r/fallback_download_only.R` (somente download), e concluir o processamento no Python.
- **Logs:** domínio operacional da ingestão (observabilidade e retentativas), usando `logs/erros.log`. O arquivo é compartilhado por R e Python (inclusive transformação). Formato de cada linha: `quando (ISO) | quem (Script R ou Python) | onde (componente/caminho) | o que aconteceu`.

## ETAPA 3 — Pipeline de dados: transformação

**Status:** em andamento.

- **transform.py** (`python -m src.data.transform`): lê `data/raw/` (origem bruta), normaliza o schema e grava em `data/processed/` uma flat table única para SIA e SIH com domínio amplo (colunas comuns + específicas + derivadas unificadas). Os arquivos em raw permanecem; reprocessamentos partem sempre de raw. Um arquivo por vez para controle de memória.


**Demais informações relevantes (3.3):** criar primeiro uma visualização bruta dos dados para entender como estão; em seguida fazer perguntas e, com base nelas, escolher o tipo de análise. Como os dados alimentam o RAG, é o próprio RAG que decidirá qual cálculo estatístico é mais adequado para cada pergunta.

### Domínio de dados canônico em `data/processed/*.parquet`

A camada analítica canônica passa a ser `data/processed/**/*.parquet`, com schema normalizado no `transform.py`:

- **Colunas comuns:** `16`
- **Colunas específicas SIA:** `33`
- **Colunas específicas SIH:** `46`
- **Colunas derivadas unificadas:** `10`
- **Total do domínio canônico:** `105`
- **Modelo de saída:** flat table única para SIA e SIH, com o mesmo schema em todos os arquivos.
- **Tratamento de ausência:** campos sem equivalente na origem permanecem `null`.

Para a lista completa das colunas canônicas, ver [06.1-dominio-colunas-completas.md](06.1-dominio-colunas-completas.md).


**Ao final do processamento — data/processed/**/*.parquet - o que você deve ver:**
| Estágio | Diretório | Conteúdo |
|---------|-----------|----------|
| **Origem bruta** | `data/raw/` | A ingestão Python grava direto em `ano=X/uf=Y/sistema=SIH|SIA/`. Arquivos não são removidos pelo transform. |
| **Fonte de verdade analítica** | `data/processed/` | Flat table canônica usada por DuckDB/RAG, unificando SIA+SIH em um parquet por `ano/uf/mês` (sem pasta `sistema=`). |

Fluxo: **ingestion (Python) -> data/raw/** (já particionado) **-> transform -> data/processed/**.

## ETAPA 4 — Camada DuckDB

**Status:** concluída.

- **executor.py** (`src/rag/executor.py`): implementada função `query(sql: str) -> pd.DataFrame`.
- **Leitura direta de Parquet:** a função cria view temporária `processed` usando `read_parquet('data/processed/**/*.parquet')`, incluindo estrutura unificada sem pasta `sistema=`.
- **Sem banco externo:** execução em DuckDB `:memory:` por chamada.
- **Validações básicas:** SQL vazio inválido; `data_root` inexistente gera erro explícito; ausência de arquivos Parquet em `data/processed` gera erro; schema canônico exige presença das 10 colunas derivadas unificadas.
- **Testes da etapa:** `tests/test_queries.py` cobre agregação, filtro e cenários de erro básicos.
- **Guia prático (Etapa 4.1):** passo a passo e exemplos em [06.3-consultas-duckdb.md](06.3-consultas-duckdb.md).

## ETAPA 5 — Agente LLM

- Recebe pergunta em português
- Analisa schema disponível
- Gera SQL seguro
- Executa no DuckDB
- Retorna resposta explicada
- Restrições: nunca inventar colunas; nunca estimar valores; sempre SQL executável

## ETAPA 6 — API

- Endpoint `POST /query`
- Entrada: `{ "question": "..." }`
- Saída: `{ "sql": "...", "result": "...", "explanation": "..." }`

[← Voltar ao índice](README.md)
