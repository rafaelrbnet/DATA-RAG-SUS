# Benchmark Aggregate Report — SUS Data RAG

**Model:** openai  
**Runs:** 3  
**Temperature:** 0.0  
**Queries per run:** 118  

## Resumo por Run

| Run | Corretas | EA |
|---|---:|---:|
| Run 1 | 97/118 | 82.2% |
| Run 2 | 97/118 | 82.2% |
| Run 3 | 97/118 | 82.2% |
| **Média** | — | **82.2%** |
| **DP** | — | **0.0%** |

**Wilson IC 95% (média):** [74.3%, 88.1%]

## EA por Categoria (Média ± DP)

| Categoria | Média EA | DP |
|---|---:|---:|
| Epidemiológica Simples | 100.0% | 0.0% |
| Epidemiológica Complexa | 54.8% | 0.0% |
| Financeira | 80.8% | 0.0% |
| Temporal/Comparativa | 92.9% | 0.0% |

> **Nota metodológica:** `temperature=0` (decodificação greedy) → outputs determinísticos.
> DP=0% entre runs confirma reprodutibilidade total. Registrado no Methods como garantia de rigor.

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_