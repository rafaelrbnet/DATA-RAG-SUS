# SUS Data RAG

Sistema de **RAG baseado em dados estruturados do SUS** (foco em **dados ortopédicos** — SIH e SIA): Parquet + DuckDB + LLM. O modelo atua como agente gerador de SQL; as consultas são executadas em DuckDB sobre arquivos Parquet, com respostas precisas, auditáveis e reproduzíveis.

---

## Índice

- [Resumo do projeto](#resumo-do-projeto)
- [Princípios](#princípios)
- [Estrutura do repositório](#estrutura-do-repositório)
- [Stack](#stack)
- [Fluxo](#fluxo)
- [Documentação](#documentação)
- [Como começar](#como-começar)
- [Repositório](#repositório)
- [Créditos](#créditos)
- [Licença](#licença)

---

## Créditos

- **Rafael Baena Neto**
- **Denise Ribeiro**

---

## Resumo do projeto

| Item | Descrição |
|------|------------|
| **Objetivo** | Responder perguntas clínicas (ortopedia), assistenciais e financeiras sobre dados do SUS via linguagem natural, com SQL gerado por LLM e executado em DuckDB. |
| **Arquitetura** | **Data RAG** (Code-Interpreter RAG): dados em Parquet, motor DuckDB, LLM para interpretar pergunta, gerar SQL e explicar resultado. |
| **Schema analítico** | Camada canônica em `data/processed`: **SIA = 62 colunas** (58 padrão + 4 derivadas) e **SIH = 75 colunas** (71 padrão + 4 derivadas). |
| **Finalidades** | Científica (pesquisa em saúde digital), tecnológica (arquitetura Data RAG), social (apoio à decisão em saúde pública). |

O LLM **nunca lê os dados crus**; apenas gera SQL, recebe o resultado e produz a explicação.

---

## Princípios

- **Fazer:** dados em Parquet colunar, DuckDB como motor analítico, LLM para interpretação + SQL + explicação.
- **Não fazer:** vetorizar milhões de linhas, converter tabelas em texto para o LLM, depender de embeddings para cálculos.

---

## Estrutura do repositório

```
datas-rag-sus/
├── README.md
├── pyproject.toml
├── data/raw | data/processed | data/schemas
├── scripts/r/     # Scripts R do pipeline de dados
├── docs/          # Documentação modular (índice em docs/README.md)
├── src/rag/       # agent, sql_generator, executor, prompts
├── src/data/      # transform, dictionary
├── src/api/       # main (FastAPI)
├── notebooks/
└── tests/
```

Detalhes em [docs/03-estrutura-repositorio.md](docs/03-estrutura-repositorio.md).

---

## Stack

- **Python** 3.12 (gerenciado via `uv`)
- **DuckDB** (consultas em Parquet)
- **Parquet** (dados)
- **LangChain** + `langchain-openai` (orquestração)
- **Ollama** (padrão local) ou **OpenAI** (alternativa via API)
- **FastAPI** (API)

---

## Fluxo

```
Pergunta → LLM interpreta → Gera SQL → DuckDB em Parquet → Resultado → LLM explica
```

---

## Documentação

A documentação está em **modular** em `docs/`:

| Documento | Assunto |
|-----------|---------|
| [docs/README.md](docs/README.md) | Índice da documentação |
| [01-objetivo](docs/01-objetivo.md) | Objetivo e finalidades |
| [02-arquitetura](docs/02-arquitetura.md) | Princípios de arquitetura |
| [03-estrutura-repositorio](docs/03-estrutura-repositorio.md) | Estrutura de pastas |
| [04-stack-tecnologica](docs/04-stack-tecnologica.md) | Stack e dependências |
| [05-fluxo-funcionamento](docs/05-fluxo-funcionamento.md) | Fluxo da pergunta ao resultado |
| [06-etapas-implementacao](docs/06-etapas-implementacao.md) | Etapas de implementação |
| [06.1-dominio-colunas-completas](docs/06.1-dominio-colunas-completas.md) | Domínio canônico em `data/processed` (padrão + derivadas) |
| [07-prompt-agente](docs/07-prompt-agente.md) | Prompt do agente SQL |
| [08-criterios-qualidade](docs/08-criterios-qualidade.md) | Critérios de qualidade científica |
| [09-roadmap](docs/09-roadmap.md) | Roadmap futuro |
| [10-licenca](docs/10-licenca.md) | Licença |
| [11-enriquecimento-orphacode](docs/11-enriquecimento-orphacode.md) | Plano de integração com Orphanet — doenças raras (**planejado**) |

---

## Como começar

1. Clonar o repositório e entrar na pasta do projeto.

2. Instalar o [Ollama](https://ollama.com/download) (instalador oficial, não Homebrew) e baixar o modelo:
   ```bash
   ollama pull qwen2.5-coder:14b
   ```

3. Copiar `.env.example` para `.env` — a configuração padrão já aponta para Ollama local:
   ```
   LLM_PROVIDER=ollama
   OLLAMA_BASE_URL=http://localhost:11434/v1
   OLLAMA_MODEL=qwen2.5-coder:14b
   ```
   Para usar OpenAI em vez de Ollama, substitua por `LLM_PROVIDER=openai` e forneça `OPENAI_API_KEY`.

4. Criar ambiente virtual e instalar dependências (requer [`uv`](https://docs.astral.sh/uv/)):
   ```bash
   uv venv --python 3.12
   source .venv/bin/activate   # ou .venv\Scripts\activate no Windows
   uv pip install -e .
   ```

5. Pipeline de dados (ingestão Python → transform → enriquecimento clínico):
   ```bash
   uv run python -m src.data.ingestion       # data/raw/  (DBC → Parquet particionado)
   uv run python -m src.data.transform       # data/processed/  (schema canônico)
   uv run python -m src.data.clinical_inference  # data/enriched/  (AHEN)
   ```
   Observação: a ingestão tenta Python primeiro e usa fallback via script R apenas quando o arquivo não é encontrado no FTP/S3.

6. Rodar a API:
   ```bash
   uv run uvicorn src.api.main:app --reload
   ```

7. Testar a integração:
   ```bash
   curl -X POST http://localhost:8000/query \
     -H "Content-Type: application/json" \
     -d '{"question": "Quantos procedimentos foram realizados em 2023?"}'
   ```

---

## Repositório

Projeto conectado ao repositório remoto:

**https://github.com/rafaelrbnet/DATA-RAG-SUS.git**

---

## Licença

MIT ou Apache-2.0 — ver [docs/10-licenca.md](docs/10-licenca.md) e arquivo `LICENSE` na raiz.
