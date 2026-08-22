# Benchmark Aggregate Report — SUS Data RAG

**Model:** ollama  
**Runs:** 3  
**Temperature:** 0.3  
**Queries per run:** 50  

## Resumo por Run

| Run | Corretas | EA |
|---|---:|---:|
| Run 1 | 12/50 | 24.0% |
| Run 2 | 14/50 | 28.0% |
| Run 3 | 19/50 | 38.0% |
| **Média** | — | **30.0%** |
| **DP** | — | **7.2%** |

**Wilson IC 95% (média):** [19.1%, 43.8%]

## EA por Categoria (Média ± DP)

| Categoria | Média EA | DP |
|---|---:|---:|
| Epidemiológica Simples | 46.7% | 6.7% |
| Epidemiológica Complexa | 17.8% | 7.7% |
| Financeira | 20.0% | 10.0% |
| Temporal/Comparativa | 33.3% | 15.3% |

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_