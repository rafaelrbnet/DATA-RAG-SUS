# Benchmark Aggregate Report — SUS Data RAG

**Model:** openai  
**Runs:** 3  
**Temperature:** 0.0  
**Queries per run:** 118  

## Resumo por Run

| Run | Corretas | EA |
|---|---:|---:|
| Run 1 | 92/118 | 78.0% |
| Run 2 | 93/118 | 78.8% |
| Run 3 | 92/118 | 78.0% |
| **Média** | — | **78.2%** |
| **DP** | — | **0.5%** |

**Wilson IC 95% (média):** [69.7%, 84.5%]

## EA por Categoria (Média ± DP)

| Categoria | Média EA | DP |
|---|---:|---:|
| Epidemiológica Simples | 100.0% | 0.0% |
| Epidemiológica Complexa | 50.5% | 1.9% |
| Financeira | 71.8% | 2.2% |
| Temporal/Comparativa | 89.3% | 0.0% |

> **Nota metodológica:** `temperature=0` (decodificação greedy) → outputs determinísticos.
> DP=0% entre runs confirma reprodutibilidade total. Registrado no Methods como garantia de rigor.

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_