# 2. Princípios de Arquitetura

## ❌ NÃO FAZER

- **Não** vetorizar milhões de linhas do SUS.
- **Não** converter tabelas em texto corrido para o LLM.
- **Não** depender de embeddings para cálculos.

## ✅ FAZER

- Manter dados em **Parquet colunar**.
- Usar **DuckDB como motor analítico local**.
- Usar o LLM apenas para:
  - interpretar a pergunta do usuário;
  - gerar SQL compatível com DuckDB;
  - explicar o resultado em linguagem natural.

## Conceito

> **Code-Interpreter RAG** (ou **Data RAG**): dados em formato analítico (Parquet), motor de consulta (DuckDB) e LLM como orquestrador de código (SQL), não como leitor de dados brutos.

[← Voltar ao índice](README.md)
