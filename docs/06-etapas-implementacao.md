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

**Status:** concluída.

- **transform.py** (`python -m src.data.transform`): lê `data/raw/` (origem bruta), normaliza o schema e grava em `data/processed/` uma flat table única para SIA e SIH com domínio amplo (colunas comuns + específicas + derivadas unificadas). Os arquivos em raw permanecem; reprocessamentos partem sempre de raw. Um arquivo por vez para controle de memória.


**Demais informações relevantes (3.3):** criar primeiro uma visualização bruta dos dados para entender como estão; em seguida fazer perguntas e, com base nelas, escolher o tipo de análise. Como os dados alimentam o RAG, é o próprio RAG que decidirá qual cálculo estatístico é mais adequado para cada pergunta.

### Domínio de dados canônico em `data/processed/*.parquet`

A camada analítica canônica passa a ser `data/processed/**/*.parquet`, com schema normalizado no `transform.py`:

- **Colunas comuns:** `17`
- **Colunas específicas SIA:** `33`
- **Colunas específicas SIH:** `45`
- **Colunas derivadas unificadas:** `15`
- **Total do domínio canônico:** `110`
- **Modelo de saída:** flat table única para SIA e SIH, com o mesmo schema em todos os arquivos.
- **Tratamento de ausência:** campos sem equivalente na origem permanecem `null`.

Para a fonte única de domínio e normalização das colunas canônicas, ver [06.1-dominio-colunas-completas.md](06.1-dominio-colunas-completas.md).


**Ao final do processamento — data/processed/**/*.parquet - o que você deve ver:**
| Estágio | Diretório | Conteúdo |
|---------|-----------|----------|
| **Origem bruta** | `data/raw/` | A ingestão Python grava direto em `ano=X/uf=Y/sistema=SIH|SIA/`. Arquivos não são removidos pelo transform. |
| **Fonte de verdade analítica** | `data/processed/` | Flat table canônica usada por DuckDB/RAG, unificando SIA+SIH em um parquet por `ano/uf/mês` (sem pasta `sistema=`). |

Fluxo: **ingestion (Python) -> data/raw/** (já particionado) **-> transform -> data/processed/**.

**Observação operacional:** após mudanças no schema técnico da camada `processed` (por exemplo, inclusão/correção de `row_id`), é necessário reprocessar os arquivos legados para alinhar todos os parquets ao contrato canônico atual.

## ETAPA 4 — Enriquecimento clínico determinístico

**Status:** concluída.

- **Decisão de arquitetura:** implementar como **nova etapa do pipeline** (não dentro de `transform.py`).
- **Objetivo:** criar colunas inferidas para suporte a caso clínico e sugestão de templates, preservando `data/processed/` como camada canônica estável.
- **Entrada:** `data/processed/**/*.parquet`.
- **Saída:** `data/enriched/**/*.parquet` (mesmas partições `ano/uf/mês`).
- **Chave de ligação entre camadas:** `row_id` (identificador técnico único por linha, gerado no `transform.py`).
- **Implementação atual:** `src/data/clinical_inference.py` com motor de regras determinísticas versionadas.
- **Execução da etapa:** `python -m src.data.clinical_inference`.
- **Template adotado:** `AHEN` (*Assistive Health Event Narrative*), centrado em evento assistencial (não em caso clínico individual).
- **Versão de inferência atual:** `ahen_v1.0.0`.
- **Rastreabilidade:** para cada inferência, gravar:
  - `row_id` (FK para `processed`)
  - `clinical_template_id`
  - `clinical_template_label`
  - `clinical_inference_version`
  - `clinical_inference_rule_id`
  - `clinical_inference_confidence` (quando aplicável)
  - `clinical_inference_reason` (resumo curto da regra aplicada)
- **Saída mínima do enriquecimento:** `row_id` + narrativa clínica (`clinical_event_narrative`) + metadados de inferência.
- **Pré-requisito operacional:** reprocessar `data/processed` após mudanças no `transform.py` para garantir `row_id` válido antes da execução desta etapa.
- **Validação:** testes unitários das regras e testes de regressão por amostra.

Fluxo recomendado: **ingestion -> transform -> enriched(clinical_inference) -> DuckDB -> LLM -> API**.

## ETAPA 5 — Camada DuckDB

**Status:** concluída.

- **executor.py** (`src/rag/executor.py`): implementada função `query(sql: str) -> pd.DataFrame`.
- **Leitura direta de Parquet:** a função cria view temporária `processed` usando `read_parquet('data/processed/**/*.parquet')`, incluindo estrutura unificada sem pasta `sistema=`.
- **Sem banco externo:** execução em DuckDB `:memory:` por chamada.
- **Validações básicas:** SQL vazio inválido; `data_root` inexistente gera erro explícito; ausência de arquivos Parquet em `data/processed` gera erro; schema canônico exige presença das 15 colunas derivadas unificadas.
- **Testes da etapa:** `tests/test_queries.py` cobre agregação, filtro e cenários de erro básicos.
- **Guia prático (Etapa 5.1) — SIA/SIH (`processed`):** passo a passo em [06.3-consultas-duckdb-processed.md](06.3-consultas-duckdb-processed.md).
- **Fonte de verdade dos exemplos SQL:** usar os notebooks [exploration.ipynb](../notebooks/exploration.ipynb) para `processed` e [event-narrative.ipynb](../notebooks/event-narrative.ipynb) para `enriched` e joins por `row_id`.

## ETAPA 6 — Agente LLM

**Status:** concluída.

**Componentes implementados:**

| Arquivo | Responsabilidade |
|---------|-----------------|
| `src/rag/prompts.py` | `SCHEMA_CONTEXT` (110 colunas canônicas compactas), `SYSTEM_PROMPT` (role + schema + 13 regras), `EXPLAIN_PROMPT` (interpretação pós-execução) |
| `src/rag/sql_generator.py` | `generate_sql(question) -> str` — chama GPT-4o, extrai SQL do bloco ```sql```, valida SELECT-only |
| `src/rag/agent.py` | `run_query(question, *, data_root) -> dict` — orquestra generate_sql → executor.query → _explain |

**Saída de `run_query`:**
```python
{
  "sql":         str | None,   # SQL DuckDB gerado
  "result":      list | None,  # linhas como lista de dicts (NaN → None)
  "explanation": str | None,   # resposta em português
  "row_count":   int,          # número de linhas
  "error":       str | None,   # presente somente em falha
}
```

**Restrições de segurança (sql_generator.py):**
- Somente SELECT permitido — bloqueia DELETE, UPDATE, INSERT, DROP, CREATE, ALTER, TRUNCATE, EXEC, GRANT, REVOKE
- Nunca inventar colunas — apenas as listadas em `SCHEMA_CONTEXT`
- Nunca estimar valores — resultados vêm exclusivamente do DuckDB
- SQL deve ser executável sem modificações na view `processed`

**Pré-requisito:** `OPENAI_API_KEY` configurada em `.env` (nunca commitar).

**Execução programática:**
```python
from src.rag.agent import run_query
resultado = run_query("Total de internações por fratura de fêmur em SP em 2022")
print(resultado["sql"])
print(resultado["explanation"])
```

**Testes:** `tests/test_sql_generation.py` — 9 testes unitários (sem chamada real à API).

## ETAPA 7 — API

**Status:** concluída.

- **`src/api/main.py`** — FastAPI com CORS habilitado.
- Endpoint `POST /query` — entrada `{ "question": "..." }`, saída `{ "sql", "result", "explanation", "row_count", "error" }`.
- Endpoint `GET /health` — verificação de disponibilidade.
- Documentação interativa em `/docs` (Swagger) e `/redoc`.

**Execução:**
```bash
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```

**Exemplo de chamada:**
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"question": "Quantas internações ortopédicas ocorreram em SP em 2022?"}'
```

[← Voltar ao índice](README.md)
