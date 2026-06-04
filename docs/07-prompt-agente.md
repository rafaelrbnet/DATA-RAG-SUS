# 7. Prompt do Agente SQL

## Papel do agente

> Analista especialista no SUS, focado em dados administrativos de saúde ortopédica (SIH internações e SIA produção ambulatorial). Converte perguntas em português em SQL DuckDB válido.

---

## Versões do prompt

| Versão | Condição | Descrição |
|--------|----------|-----------|
| v1 (zero-shot) | Condição A — benchmark | 16 regras genéricas, sem exemplos few-shot |
| v2 (domain-engineered) | Condição B — benchmark | Definição clínica explícita + 17 regras + 5 exemplos few-shot |

---

## Estrutura do prompt v2 (atual — `src/rag/prompts.py`)

### 1. Definição clínica obrigatória

Bloco destacado que define o que é "ortopédico" neste dataset:

```
"ORTOPÉDICO" = icd_group = 'M00-M99' (musculoesquelético)
             + icd_group = 'S00-T98' (traumatológico)

Filtro padrão: WHERE (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
```

Esta definição foi adicionada após análise dos erros da Condição A, onde o modelo interpretava "ortopédico" como apenas M00-M99 (musculoesquelético), ignorando S00-T98 (traumatológico), que representa ~90% dos registros.

### 2. Regras obrigatórias (17 regras)

| # | Tema | Correção em v2 |
|---|------|----------------|
| 3 | Filtro ortopédico | Explícito: M+S obrigatório |
| 4 | CID específico | `LIKE 'S72%'` — não combinar com OR icd_group |
| 11 | Permanência | `dias_perm` (real) vs `qt_diarias` (faturadas) |
| 12 | Proporção | Window function — proibido `WITH ROLLUP` (MySQL) |
| 13 | CTEs | Proibido WITH CTE (bloqueio do validador) |
| 14 | Localização | `cod_munic_estabelecimento` (hospital) vs `cod_munic_residencia` (paciente) |
| 15 | JOIN enriched | Somente para `clinical_interpretacao_clinica` ou deslocamento |

### 3. Exemplos few-shot (5 exemplos)

Exemplos que cobrem os padrões-chave identificados no benchmark:
1. Contagem ortopédica com filtro M+S
2. CID específico sem OR icd_group
3. Proporção com window function
4. Top-N por estabelecimento (cnes)
5. Série temporal por trimestre

Os exemplos **não pertencem ao benchmark N=50** — são distintos para evitar data leakage.

---

## Decisões de design do prompt

### Por que few-shot e não zero-shot?

O benchmark Condição A (zero-shot) revelou que o modelo `qwen2.5-coder:14b` via Ollama:
- Interpretou "ortopédico" como M00-M99 apenas (80% das falhas)
- Confundiu `cod_munic_residencia` com `cod_munic_estabelecimento`
- Gerou WITH CTEs e WITH ROLLUP (incompatíveis com o validador/DuckDB)

A Condição B documenta o impacto do refinamento de prompt como variável independente do experimento.

### Por que não incluir queries do gold-standard como exemplos?

Incluir queries do benchmark (N=50) como few-shot seria data leakage — o modelo veria as respostas antes de ser avaliado. Os 5 exemplos usam CIDs, estados e anos diferentes do benchmark (SP/2022/M+S).

### Validador de SQL

`src/rag/sql_generator.py` valida que o SQL começa com `SELECT` ou `WITH` (CTEs padrão SQL). A instrução no prompt proíbe CTEs para consistência com versões anteriores do validador, mas `WITH` é aceito tecnicamente.

---

## Métricas de Execution Accuracy por condição

| Condição | Modelo | Prompt | EA | Wilson IC 95% |
|---|---|---|---|---|
| A | Ollama qwen2.5-coder:14b | zero-shot | 12% | [4,9%, 24,0%] |
| B | Ollama qwen2.5-coder:14b | domain-engineered | pendente | — |
| C | GPT-4o | zero-shot | pendente | — |
| D | GPT-4o | domain-engineered | pendente | — |

*EA Condição A: 6/50 corretas (5 match exato + 1 match flexível por alias diferente). Re-scored com comparador flexível — veja nota metodológica em `Projeto.md`.*

---

## Prompt de explicação (`EXPLAIN_PROMPT`)

Prompt separado usado **após** a execução do SQL para gerar a resposta em linguagem natural:
- Recebe: pergunta original + SQL executado + resultado (até 20 linhas)
- Gera: interpretação clínica/epidemiológica + limitações do dado
- Máximo 3 parágrafos. Tom profissional e direto.

[← Voltar ao índice](README.md)
