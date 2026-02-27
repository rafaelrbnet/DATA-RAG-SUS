# 6. Etapas de Implementação

## ETAPA 1 — Setup do ambiente

**Status:** concluída.

- Criar projeto Python com `pyproject.toml`
- Instalar: `duckdb`, `pandas`, `pyarrow`, `fastapi`, `uvicorn`, `langchain` (ou `llamaindex`), `python-dotenv`

## ETAPA 2 — Pipeline de dados

**Status:** concluída.

- **ingestion.py** (`python -m src.data.ingestion`): ingestão principal 100% Python. Faz diff da grade esperada com `data/raw/`, baixa DBC do DATASUS (FTP nativo com fallback S3), descompacta DBC->DBF, filtra em chunks e grava Parquet particionado em `data/raw/ano=X/uf=Y/sistema=SIH|SIA/`.
- **Retentativa e resiliência:** pode incluir alvos vindos de `logs/erros.log`; aplica retries, backoff/jitter, circuit breaker para timeout e ignora arquivos com muitas falhas recorrentes no log.
- **Fallback R (pontual):** quando o arquivo não existe no FTP/S3, o `ingestion.py` pode acionar `scripts/r/fallback_download_only.R` (somente download), e concluir o processamento no Python.
- **transform.py** (`python -m src.data.transform`): lê `data/raw/` (origem bruta), aplica deduplicação/normalização de schema e grava em `data/processed/` (fonte de verdade analítica). Os arquivos em raw permanecem; reprocessamentos partem sempre de raw. Um arquivo por vez para controle de memória. Para estender, adicionar nova função `(df) -> df` em `TRANSFORM_STEPS`.

**Colunas derivadas e padronizações:**
- **custo_total:** normalização (não é soma). SIH e SIA têm colunas de valor com nomes diferentes; escolhemos a coluna apropriada por sistema e expomos como `custo_total` para visibilidade.
- **idade_grupo:** faixas 0-17, 18-59, 60+ a partir de pa_idade/idade. Pendente: artigo de referência para classificação dos grupos etários.
- **cid_capitulo:** primeiro caractere do CID (capítulo CID-10). Os grupos clínicos (`icd_group`) são derivados no pipeline de ingestão/transformação e devem ser referenciados no dicionário de dados/documentação.
- **ano_mes:** YYYYMM para competência. **Tipos:** colunas de valor/quantidade convertidas para numérico quando string. **UF:** padronizadas para 2 letras maiúsculas. **Strings:** trim e "nan"/"None" → vazio.

**Demais informações relevantes (4.3):** criar primeiro uma visualização bruta dos dados para entender como estão; em seguida fazer perguntas e, com base nelas, escolher o tipo de análise. Como os dados alimentam o RAG, é o próprio RAG que decidirá qual cálculo estatístico é mais adequado para cada pergunta.

**Logs:** R e Python usam o mesmo arquivo `logs/erros.log`. Formato de cada linha: `quando (ISO) | quem (Script R ou Python) | onde (componente/caminho) | o que aconteceu`.

### Domínio de dados canônico em `data/processed/*.parquet`

A camada analítica canônica passa a ser `data/processed/**/*.parquet`, com schema normalizado no `transform.py`:

- **Colunas padrão comuns (SIA e SIH):** `16`
- **Colunas padrão SIA (incluindo comuns):** `58`
- **Colunas padrão SIH (incluindo comuns):** `71`
- **Colunas derivadas comuns:** `4` (`custo_total`, `idade_grupo`, `cid_capitulo`, `ano_mes`)
- **Total final por arquivo SIA (padrão + derivadas):** `62`
- **Total final por arquivo SIH (padrão + derivadas):** `75`

Para a lista completa das colunas padrão e derivadas (com exemplos de linha), ver [06.1-dominio-colunas-completas.md](06.1-dominio-colunas-completas.md).


**Ao final do processamento — data/processed/**/*.parquet - o que você deve ver:**
| Estágio | Diretório | Conteúdo |
|---------|-----------|----------|
| **Origem bruta** | `data/raw/` | A ingestão Python grava direto em `ano=X/uf=Y/sistema=SIH|SIA/`. Arquivos não são removidos pelo transform. |
| **Fonte de verdade analítica** | `data/processed/` | Camada normalizada usada por DuckDB/RAG, com schema canônico, deduplicado e colunas derivadas padronizadas. |

Fluxo: **ingestion (Python) -> data/raw/** (já particionado) **-> transform -> data/processed/**.

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
