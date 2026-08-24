# Benchmark Aggregate Report — SUS Data RAG

**Model:** ollama  
**Runs:** 3  
**Temperature:** 0.3  
**Queries per run:** 118  

## Resumo por Run

| Run | Corretas | EA |
|---|---:|---:|
| Run 1 | 76/118 | 64.4% |
| Run 2 | 80/118 | 67.8% |
| Run 3 | 77/118 | 65.3% |
| **Média** | — | **65.8%** |
| **DP** | — | **1.8%** |

**Wilson IC 95% (média):** [57.2%, 74.0%]

## EA por Categoria (Média ± DP)

| Categoria | Média EA | DP |
|---|---:|---:|
| Epidemiológica Simples | 76.8% | 1.7% |
| Epidemiológica Complexa | 43.0% | 1.9% |
| Financeira | 70.5% | 2.2% |
| Temporal/Comparativa | 73.8% | 2.1% |

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_