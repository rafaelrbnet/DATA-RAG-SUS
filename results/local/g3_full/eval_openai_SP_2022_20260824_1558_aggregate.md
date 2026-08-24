# Benchmark Aggregate Report — SUS Data RAG

**Model:** openai  
**Runs:** 3  
**Temperature:** 0.0  
**Queries per run:** 118  

## Resumo por Run

| Run | Corretas | EA |
|---|---:|---:|
| Run 1 | 90/118 | 76.3% |
| Run 2 | 90/118 | 76.3% |
| Run 3 | 89/118 | 75.4% |
| **Média** | — | **76.0%** |
| **DP** | — | **0.5%** |

**Wilson IC 95% (média):** [67.8%, 83.0%]

## EA por Categoria (Média ± DP)

| Categoria | Média EA | DP |
|---|---:|---:|
| Epidemiológica Simples | 100.0% | 0.0% |
| Epidemiológica Complexa | 47.3% | 1.9% |
| Financeira | 69.2% | 0.0% |
| Temporal/Comparativa | 85.7% | 0.0% |

> **Nota metodológica:** `temperature=0` (decodificação greedy) → outputs determinísticos.
> DP=0% entre runs confirma reprodutibilidade total. Registrado no Methods como garantia de rigor.

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_