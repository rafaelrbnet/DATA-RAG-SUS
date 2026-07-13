# Benchmark Aggregate Report — SUS Data RAG

**Model:** ollama  
**Runs:** 3  
**Temperature:** 0.3  
**Queries per run:** 50  

## Resumo por Run

| Run | Corretas | EA |
|---|---:|---:|
| Run 1 | 10/50 | 20.0% |
| Run 2 | 17/50 | 34.0% |
| Run 3 | 18/50 | 36.0% |
| **Média** | — | **30.0%** |
| **DP** | — | **8.7%** |

**Wilson IC 95% (média):** [19.1%, 43.8%]

## EA por Categoria (Média ± DP)

| Categoria | Média EA | DP |
|---|---:|---:|
| Epidemiológica Simples | 42.2% | 7.7% |
| Epidemiológica Complexa | 22.2% | 3.8% |
| Financeira | 26.7% | 11.5% |
| Temporal/Comparativa | 26.7% | 23.1% |

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_