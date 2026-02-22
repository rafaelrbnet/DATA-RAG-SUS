# 6. Etapas de Implementação

## ETAPA 1 — Setup do ambiente

- Criar projeto Python com `pyproject.toml`
- Instalar: `duckdb`, `pandas`, `pyarrow`, `fastapi`, `uvicorn`, `langchain` (ou `llamaindex`), `python-dotenv`

## ETAPA 2 — Pipeline de dados

- **Script R** (`scripts/r/analise_ortopedia.R`): única fonte de download do DATASUS (microdatasus); grava Parquets em `data/downloaded/` (ex.: `sih_SP_2021_01.parquet`, `sia_SP_2021_01.parquet`).
- **ingest.py**: lê `data/downloaded/` (lista `.parquet`), move cada arquivo para `data/raw/ano=X/uf=Y/sistema=SIH|SIA/` (particionado). Não baixa do DATASUS; um arquivo por vez, controle de memória.
- **transform.py**: lê `data/raw/` (particionado), padroniza tipos e adiciona colunas derivadas, grava em `data/processed/` com a mesma estrutura de partições. Um arquivo por vez para controle de memória (ex.: 16 GB RAM).

**Colunas derivadas e padronizações:**
- **custo_total:** normalização (não é soma). SIH e SIA têm colunas de valor com nomes diferentes; escolhemos a coluna apropriada por sistema e expomos como `custo_total` para visibilidade.
- **idade_grupo:** faixas 0-17, 18-59, 60+ a partir de pa_idade/idade. Pendente: artigo de referência para classificação dos grupos etários.
- **cid_capitulo:** primeiro caractere do CID (capítulo CID-10). Os grupos clínicos (`icd_group`) vêm do R (regex em `analise_ortopedia.R`); referenciar dicionário de dados ou documentação/artigo.
- **ano_mes:** YYYYMM para competência. **Tipos:** colunas de valor/quantidade convertidas para numérico quando string. **UF:** padronizadas para 2 letras maiúsculas. **Strings:** trim e "nan"/"None" → vazio.

**Demais informações relevantes (4.3):** criar primeiro uma visualização bruta dos dados para entender como estão; em seguida fazer perguntas e, com base nelas, escolher o tipo de análise. Como os dados alimentam o RAG, é o próprio RAG que decidirá qual cálculo estatístico é mais adequado para cada pergunta.

**Logs:** R e Python usam o mesmo arquivo `logs/erros.log`. Formato de cada linha: `quando (ISO) | quem (Script R ou Python) | onde (componente/caminho) | o que aconteceu`.

**Ao final do processamento — o que você deve ver:**
| Estágio | Diretório | Conteúdo |
|---------|-----------|----------|
| **Entrada do pipeline** | `data/downloaded/` | Parquets gravados pelo R (ex.: `sih_SP_2021_01.parquet`). Após o ingest, este diretório fica vazio (arquivos foram *movidos* para raw). |
| **Após ingest** | `data/raw/` | Mesmos arquivos, reorganizados em `ano=X/uf=Y/sistema=SIH|SIA/`. São a *entrada* do transform. |
| **Após transform** | `data/processed/` | Um .parquet por arquivo de raw, na mesma estrutura de partições, com colunas padronizadas e derivadas (`custo_total`, `idade_grupo`, `cid_capitulo`, `ano_mes`). São a *entrada* do DuckDB/RAG. |

No log: ao final do **ingest** aparece uma linha tipo `Concluído: N movidos, M ignorados. data/raw/: K arquivos .parquet.` Ao final do **transform** aparece `Concluído: N processados, M falhas. data/processed/: K arquivos .parquet.` Assim você confere quantos dados entraram (downloaded → raw) e quantos foram processados (raw → processed).

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
