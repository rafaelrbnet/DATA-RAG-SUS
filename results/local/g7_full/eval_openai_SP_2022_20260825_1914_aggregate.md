# Benchmark Aggregate Report — SUS Data RAG

**Model:** openai  
**Runs:** 3  
**Temperature:** 0.0  
**Queries per run:** 118  

## Resumo por Run

| Run | Corretas | EA |
|---|---:|---:|
| Run 1 | 95/118 | 80.5% |
| Run 2 | 96/118 | 81.4% |
| Run 3 | 95/118 | 80.5% |
| **Média** | — | **80.8%** |
| **DP** | — | **0.5%** |

**Wilson IC 95% (média):** [72.4%, 86.6%]

## EA por Categoria (Média ± DP)

| Categoria | Média EA | DP |
|---|---:|---:|
| Epidemiológica Simples | 100.0% | 0.0% |
| Epidemiológica Complexa | 58.1% | 0.0% |
| Financeira | 80.8% | 0.0% |
| Temporal/Comparativa | 83.3% | 2.1% |

> **Nota metodológica:** `temperature=0` (decodificação greedy) → outputs determinísticos.
> DP=0% entre runs confirma reprodutibilidade total. Registrado no Methods como garantia de rigor.

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_