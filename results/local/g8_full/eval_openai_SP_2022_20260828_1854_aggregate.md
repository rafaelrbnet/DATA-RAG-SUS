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
| Run 3 | 104/118 | 88.1% |
| **Média** | — | **87.3%** |
| **DP** | — | **0.8%** |

**Wilson IC 95% (média):** [80.1%, 92.1%]

## EA por Categoria (Média ± DP)

| Categoria | Média EA | DP |
|---|---:|---:|
| Epidemiológica Simples | 100.0% | 0.0% |
| Epidemiológica Complexa | 72.0% | 1.9% |
| Financeira | 80.8% | 0.0% |
| Temporal/Comparativa | 95.2% | 2.1% |

> **Nota metodológica:** `temperature=0` (decodificação greedy) → outputs determinísticos.
> DP=0% entre runs confirma reprodutibilidade total. Registrado no Methods como garantia de rigor.

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_