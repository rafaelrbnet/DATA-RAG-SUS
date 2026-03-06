# Documentação — SUS Data RAG

Índice da documentação modular do projeto.

| # | Documento | Conteúdo |
|---|-----------|----------|
| 01 | [Objetivo do Projeto](01-objetivo.md) | Visão, finalidades e caracterização do sistema |
| 02 | [Arquitetura](02-arquitetura.md) | Princípios: o que fazer e o que não fazer |
| 03 | [Estrutura do Repositório](03-estrutura-repositorio.md) | Árvore de pastas e arquivos |
| 04 | [Stack Tecnológica](04-stack-tecnologica.md) | Linguagem, DuckDB, Parquet, LLM, FastAPI |
| 05 | [Fluxo de Funcionamento](05-fluxo-funcionamento.md) | Da pergunta ao resultado explicado |
| 06 | [Etapas de Implementação](06-etapas-implementacao.md) | Setup, pipeline, DuckDB, agente, API |
| 06.1 | [Domínio e Normalização de Colunas (SIA e SIH)](06.1-dominio-colunas-completas.md) | Fonte única do domínio canônico e regras de normalização em `data/processed` |
| 06.2 | [Estatísticas da Base Processada](06.2-estatisticas-base-processada.md) | Volume, cobertura temporal, qualidade e métricas principais da `data/processed` |
| 06.3 | [Consultas DuckDB em `processed` (Etapa 5.1)](06.3-consultas-duckdb-processed.md) | Passo a passo da consulta e exemplos de SQL sobre SIA/SIH na camada canônica |
| 07 | [Prompt do Agente SQL](07-prompt-agente.md) | Papel, tarefas e restrições do LLM |
| 08 | [Critérios de Qualidade](08-criterios-qualidade.md) | Reprodutibilidade, auditabilidade, transparência |
| 09 | [Roadmap](09-roadmap.md) | Curto, médio e longo prazo |
| 10 | [Licença](10-licenca.md) | MIT ou Apache-2.0 |

---

Exemplos de SQL: os notebooks [exploration.ipynb](../notebooks/exploration.ipynb) e [event-narrative.ipynb](../notebooks/event-narrative.ipynb) sao a fonte de verdade para consultas DuckDB.

Para visão geral e como começar, veja o [README principal](../README.md) na raiz do repositório.
