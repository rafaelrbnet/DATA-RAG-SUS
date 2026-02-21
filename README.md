# SUS Data RAG

Sistema de **RAG baseado em dados estruturados do SUS**: Parquet + DuckDB + LLM. O modelo atua como agente gerador de SQL; as consultas são executadas em DuckDB sobre arquivos Parquet, com respostas precisas, auditáveis e reproduzíveis.

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
| **Objetivo** | Responder perguntas clínicas, epidemiológicas e financeiras sobre o SUS via linguagem natural, com SQL gerado por LLM e executado em DuckDB. |
| **Arquitetura** | **Data RAG** (Code-Interpreter RAG): dados em Parquet, motor DuckDB, LLM para interpretar pergunta, gerar SQL e explicar resultado. |
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
├── src/data/      # ingest, transform, dictionary
├── src/api/       # main (FastAPI)
├── notebooks/
└── tests/
```

Detalhes em [docs/03-estrutura-repositorio.md](docs/03-estrutura-repositorio.md).

---

## Stack

- **Python** 3.11+
- **DuckDB** (consultas em Parquet)
- **Parquet** (dados)
- **LangChain** ou **LlamaIndex** (orquestração)
- **OpenAI** ou **Ollama** (modelo)
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
| [07-prompt-agente](docs/07-prompt-agente.md) | Prompt do agente SQL |
| [08-criterios-qualidade](docs/08-criterios-qualidade.md) | Critérios de qualidade científica |
| [09-roadmap](docs/09-roadmap.md) | Roadmap futuro |
| [10-licenca](docs/10-licenca.md) | Licença |

---

## Como começar

1. Clonar o repositório e entrar na pasta do projeto.
2. Copiar `.env.example` para `.env` e configurar (ex.: chave OpenAI ou endpoint Ollama).
3. Criar ambiente virtual e instalar dependências:
   ```bash
   python -m venv .venv
   source .venv/bin/activate   # ou .venv\Scripts\activate no Windows
   pip install -e .
   ```
4. Colocar dados em `data/raw` (ou seguir o pipeline em `src/data/`).
5. Rodar a API: `uvicorn src.api.main:app --reload`.

**Próximo passo:** definir se o primeiro agente usará **OpenAI** ou **LLM local (Ollama)** para implementar a geração de SQL e a explicação.

---

## Repositório

Projeto conectado ao repositório remoto:

**https://github.com/rafaelrbnet/DATA-RAG-SUS.git**

---

## Licença

MIT ou Apache-2.0 — ver [docs/10-licenca.md](docs/10-licenca.md) e arquivo `LICENSE` na raiz.
