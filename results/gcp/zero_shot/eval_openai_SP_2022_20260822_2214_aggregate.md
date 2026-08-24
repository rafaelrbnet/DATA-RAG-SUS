# Benchmark Aggregate Report — SUS Data RAG

**Model:** openai  
**Runs:** 3  
**Temperature:** 0.0  
**Queries per run:** 118  

## Resumo por Run

| Run | Corretas | EA |
|---|---:|---:|
| Run 1 | 31/118 | 26.3% |
| Run 2 | 31/118 | 26.3% |
| Run 3 | 31/118 | 26.3% |
| **Média** | — | **26.3%** |
| **DP** | — | **0.0%** |

**Wilson IC 95% (média):** [19.2%, 34.9%]

## EA por Categoria (Média ± DP)

| Categoria | Média EA | DP |
|---|---:|---:|
| Epidemiológica Simples | 48.5% | 0.0% |
| Epidemiológica Complexa | 9.7% | 0.0% |
| Financeira | 15.4% | 0.0% |
| Temporal/Comparativa | 28.6% | 0.0% |

> **Nota metodológica:** `temperature=0` (decodificação greedy) → outputs determinísticos.
> DP=0% entre runs confirma reprodutibilidade total. Registrado no Methods como garantia de rigor.

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_