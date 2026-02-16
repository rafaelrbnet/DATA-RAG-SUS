# 5. Fluxo de Funcionamento

```
Pergunta do usuário
        ↓
LLM interpreta intenção
        ↓
LLM gera SQL compatível com DuckDB
        ↓
DuckDB consulta arquivos Parquet
        ↓
Resultado numérico retorna
        ↓
LLM explica em linguagem natural
```

## Regra fundamental

> **O LLM nunca lê os dados crus.**  
> Ele apenas gera SQL, recebe agregados/resultados e produz a explicação.

Isso garante:

- Performance (sem enviar milhões de linhas ao modelo).
- Precisão (cálculos feitos pelo DuckDB).
- Auditabilidade (SQL gerado é rastreável).

[← Voltar ao índice](README.md)
