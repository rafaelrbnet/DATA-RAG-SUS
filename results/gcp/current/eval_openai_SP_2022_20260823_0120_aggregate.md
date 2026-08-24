# Benchmark Aggregate Report — SUS Data RAG

**Model:** openai  
**Runs:** 3  
**Temperature:** 0.0  
**Queries per run:** 118  

## Resumo por Run

| Run | Corretas | EA |
|---|---:|---:|
| Run 1 | 89/118 | 75.4% |
| Run 2 | 89/118 | 75.4% |
| Run 3 | 89/118 | 75.4% |
| **Média** | — | **75.4%** |
| **DP** | — | **0.0%** |

**Wilson IC 95% (média):** [66.9%, 82.3%]

## EA por Categoria (Média ± DP)

| Categoria | Média EA | DP |
|---|---:|---:|
| Epidemiológica Simples | 100.0% | 0.0% |
| Epidemiológica Complexa | 48.4% | 0.0% |
| Financeira | 69.2% | 0.0% |
| Temporal/Comparativa | 82.1% | 0.0% |

> **Nota metodológica:** `temperature=0` (decodificação greedy) → outputs determinísticos.
> DP=0% entre runs confirma reprodutibilidade total. Registrado no Methods como garantia de rigor.

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_