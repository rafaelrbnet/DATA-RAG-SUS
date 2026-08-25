# Benchmark Aggregate Report — SUS Data RAG

**Model:** openai  
**Runs:** 3  
**Temperature:** 0.0  
**Queries per run:** 118  

## Resumo por Run

| Run | Corretas | EA |
|---|---:|---:|
| Run 1 | 95/118 | 80.5% |
| Run 2 | 93/118 | 78.8% |
| Run 3 | 94/118 | 79.7% |
| **Média** | — | **79.7%** |
| **DP** | — | **0.8%** |

**Wilson IC 95% (média):** [71.5%, 85.9%]

## EA por Categoria (Média ± DP)

| Categoria | Média EA | DP |
|---|---:|---:|
| Epidemiológica Simples | 99.0% | 1.7% |
| Epidemiológica Complexa | 53.8% | 1.9% |
| Financeira | 80.8% | 0.0% |
| Temporal/Comparativa | 84.5% | 4.1% |

> **Nota metodológica:** `temperature=0` (decodificação greedy) → outputs determinísticos.
> DP=0% entre runs confirma reprodutibilidade total. Registrado no Methods como garantia de rigor.

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_