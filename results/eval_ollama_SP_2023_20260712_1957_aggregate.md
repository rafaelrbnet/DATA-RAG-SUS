# Benchmark Aggregate Report — SUS Data RAG

**Model:** ollama  
**Runs:** 3  
**Temperature:** 0.3  
**Queries per run:** 50  

## Resumo por Run

| Run | Corretas | EA |
|---|---:|---:|
| Run 1 | 12/50 | 24.0% |
| Run 2 | 24/50 | 48.0% |
| Run 3 | 21/50 | 42.0% |
| **Média** | — | **38.0%** |
| **DP** | — | **12.5%** |

**Wilson IC 95% (média):** [25.9%, 51.8%]

## EA por Categoria (Média ± DP)

| Categoria | Média EA | DP |
|---|---:|---:|
| Epidemiológica Simples | 46.7% | 11.5% |
| Epidemiológica Complexa | 22.2% | 10.2% |
| Financeira | 33.3% | 11.5% |
| Temporal/Comparativa | 53.3% | 28.9% |

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_