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

## ETAPA 4 — Enriquecimento clínico determinístico

**Status:** executando.

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

- Recebe pergunta em português
- Analisa schema disponível
- Gera SQL seguro
- Executa no DuckDB
- Retorna resposta explicada
- Restrições: nunca inventar colunas; nunca estimar valores; sempre SQL executável

## ETAPA 7 — API

- Endpoint `POST /query`
- Entrada: `{ "question": "..." }`
- Saída: `{ "sql": "...", "result": "...", "explanation": "..." }`

[← Voltar ao índice](README.md)
