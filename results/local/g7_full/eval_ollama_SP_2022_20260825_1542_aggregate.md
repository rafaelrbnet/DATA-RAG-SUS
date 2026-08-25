# Benchmark Aggregate Report — SUS Data RAG

**Model:** ollama  
**Runs:** 3  
**Temperature:** 0.3  
**Queries per run:** 118  

## Resumo por Run

| Run | Corretas | EA |
|---|---:|---:|
| Run 1 | 83/118 | 70.3% |
| Run 2 | 85/118 | 72.0% |
| Run 3 | 85/118 | 72.0% |
| **Média** | — | **71.5%** |
| **DP** | — | **1.0%** |

**Wilson IC 95% (média):** [62.4%, 78.6%]

## EA por Categoria (Média ± DP)

| Categoria | Média EA | DP |
|---|---:|---:|
| Epidemiológica Simples | 85.9% | 1.7% |
| Epidemiológica Complexa | 50.5% | 1.9% |
| Financeira | 71.8% | 2.2% |
| Temporal/Comparativa | 77.4% | 2.1% |

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_