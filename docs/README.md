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
| 06.1 | [Domínio Completo de Colunas (SIA e SIH)](06.1-dominio-colunas-completas.md) | Domínio canônico em `data/processed` (colunas padrão + derivadas) |
| 06.2 | [Estatísticas da Base Processada](06.2-estatisticas-base-processada.md) | Volume, cobertura temporal, qualidade e métricas principais da `data/processed` |
| 07 | [Prompt do Agente SQL](07-prompt-agente.md) | Papel, tarefas e restrições do LLM |
| 08 | [Critérios de Qualidade](08-criterios-qualidade.md) | Reprodutibilidade, auditabilidade, transparência |
| 09 | [Roadmap](09-roadmap.md) | Curto, médio e longo prazo |
| 10 | [Licença](10-licenca.md) | MIT ou Apache-2.0 |

---

Para visão geral e como começar, veja o [README principal](../README.md) na raiz do repositório.
