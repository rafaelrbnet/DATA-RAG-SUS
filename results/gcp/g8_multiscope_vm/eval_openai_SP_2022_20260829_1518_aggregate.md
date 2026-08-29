# Benchmark Aggregate Report — SUS Data RAG

**Model:** openai  
**Runs:** 3  
**Temperature:** 0.0  
**Queries per run:** 118  

## Resumo por Run

| Run | Corretas | EA |
|---|---:|---:|
| Run 1 | 103/118 | 87.3% |
| Run 2 | 102/118 | 86.4% |
| Run 3 | 103/118 | 87.3% |
| **Média** | — | **87.0%** |
| **DP** | — | **0.5%** |

**Wilson IC 95% (média):** [80.1%, 92.1%]

## EA por Categoria (Média ± DP)

| Categoria | Média EA | DP |
|---|---:|---:|
| Epidemiológica Simples | 100.0% | 0.0% |
| Epidemiológica Complexa | 71.0% | 0.0% |
| Financeira | 80.8% | 0.0% |
| Temporal/Comparativa | 95.2% | 2.1% |

> **Nota metodológica:** `temperature=0` (decodificação greedy) → outputs determinísticos.
> DP=0% entre runs confirma reprodutibilidade total. Registrado no Methods como garantia de rigor.

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_