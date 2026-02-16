# 1. Objetivo do Projeto

## Visão geral

Criar um sistema de **RAG baseado em dados estruturados do SUS** onde:

- Os dados permanecem em **Parquet** (sem vetorizar linha a linha).
- O LLM atua como **agente gerador de código SQL/Python**.
- As consultas são executadas via **DuckDB** com alta performance.
- O sistema responde perguntas clínicas, epidemiológicas e financeiras sobre o SUS de forma **precisa, auditável e reproduzível**.

## Finalidades

| Área | Descrição |
|------|-----------|
| **Científica** | Pesquisa em saúde digital |
| **Tecnológica** | Arquitetura de Data RAG (Code-Interpreter RAG) |
| **Social** | Apoio à decisão em saúde pública |

## Caracterização

O sistema se caracteriza como um **Code-Interpreter RAG** (ou **Data RAG**): o LLM interpreta a pergunta, gera SQL executável e explica o resultado, sem ler os dados crus.

[← Voltar ao índice](README.md)
