# Benchmark Aggregate Report — SUS Data RAG

**Model:** openai  
**Runs:** 3  
**Temperature:** 0.0  
**Queries per run:** 50  

## Resumo por Run

| Run | Corretas | EA |
|---|---:|---:|
| Run 1 | 35/50 | 70.0% |
| Run 2 | 35/50 | 70.0% |
| Run 3 | 35/50 | 70.0% |
| **Média** | — | **70.0%** |
| **DP** | — | **0.0%** |

**Wilson IC 95% (média):** [56.2%, 80.9%]

## EA por Categoria (Média ± DP)

| Categoria | Média EA | DP |
|---|---:|---:|
| Epidemiológica Simples | 100.0% | 0.0% |
| Epidemiológica Complexa | 46.7% | 0.0% |
| Financeira | 50.0% | 0.0% |
| Temporal/Comparativa | 80.0% | 0.0% |

> **Nota metodológica:** `temperature=0` (decodificação greedy) → outputs determinísticos.
> DP=0% entre runs confirma reprodutibilidade total. Registrado no Methods como garantia de rigor.

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_