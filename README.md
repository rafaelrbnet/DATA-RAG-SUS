# SUS Data RAG

**An open-source Code-Interpreter RAG framework for natural language access to Brazilian public health administrative data (DATASUS SIA + SIH).**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Paper: Under Review](https://img.shields.io/badge/paper-under%20review-orange.svg)](https://jmir.org/medical-informatics)

---

## Overview

SUS Data RAG translates natural language questions into SQL, executes them in-memory over Apache Parquet files derived from DATASUS, and returns exact, auditable results. The LLM generates SQL; it never reads raw data or estimates values.

**Primary research question:** Is a Code-Interpreter RAG architecture over DATASUS SIA/SIH data viable as a natural language interface for orthopedic epidemiological and health management queries?

**Secondary research question (data sovereignty):** Does local LLM deployment (Ollama) remain viable under Brazil's LGPD data protection law compared to cloud deployment (GPT-4o)?

### Benchmark Results (N=50 queries, São Paulo, 2022)

| Condition | Model | Prompt | EA (mean) | Wilson 95% CI | TTA |
|---|---|---|---|---|---|
| A | qwen2.5-coder:14b (Ollama) | Zero-shot | 12.0% | [4.9%, 24.0%] | ~32.7 s |
| B | qwen2.5-coder:14b (Ollama) | Domain-engineered | **68.7% ± 1.2%** | [54.2%, 79.2%] | 8.9 s |
| D | GPT-4o (OpenAI) | Domain-engineered | **70.0% ± 0.0%** | [56.2%, 80.9%] | 5.4 s |

**Data sovereignty cost: +1.3 pp** (GPT-4o minus Ollama; overlapping CIs — exploratory comparison).
**Prompt engineering gain: +56.7 pp** (zero-shot → domain-engineered, Conditions A→B).

EA = Execution Accuracy: proportion of queries returning the same result set as gold-standard SQL.
TTA = mean time-to-answer (SQL generation + DuckDB execution). 3 independent runs per condition.

---

## Architecture

```
Natural language question
        │
        ▼
┌───────────────────┐
│   LLM Agent       │  GPT-4o (cloud) or qwen2.5-coder:14b via Ollama (local)
│   sql_generator   │  Domain-engineered prompt → generates SQL
└────────┬──────────┘
         │  SQL (SELECT only — write operations blocked)
         ▼
┌───────────────────┐
│   DuckDB          │  In-memory analytical engine over Apache Parquet
│   executor.py     │  Hive-partitioned: partition pruning per state/year
└────────┬──────────┘
         │  Exact result set
         ▼
┌───────────────────┐
│   LLM Agent       │  Natural language explanation
└────────┬──────────┘
         │
         ▼
    Response (SQL + result + explanation)
```

### Why Code-Interpreter RAG, not Vector RAG?

DATASUS SIA and SIH are structured tabular records — not text documents. SQL execution over Parquet provides:

- **Numerical exactness**: counts, costs, and time series are computed by the engine; the LLM cannot fabricate values.
- **Full traceability**: the generated SQL is returned alongside every result for independent verification.
- **Hallucination containment**: LLM errors are limited to schema-level mistakes (wrong column name, wrong filter) — observable and correctable.

### Five-Stage Pipeline

| Stage | Module | Input → Output |
|---|---|---|
| 1. Ingestion | `src/data/ingestion.py` | DATASUS FTP (DBC) → `data/raw/` (Parquet, Hive-partitioned) |
| 2. Transformation | `src/data/transform.py` | `data/raw/` → `data/processed/` (110-column canonical schema) |
| 3. Clinical enrichment | `src/data/clinical_inference.py` | `data/processed/` → `data/enriched/` (AHEN v1.0.0) |
| 4. DuckDB query layer | `src/rag/executor.py` | `data/processed/` → in-memory DuckDB views |
| 5. LLM agent | `src/rag/sql_generator.py` | Natural language → SQL → result → explanation |

---

## Dataset

| Metric | Value |
|---|---|
| Source | DATASUS SIA (outpatient) + SIH (inpatient) |
| Federative units | 27 (all Brazil) |
| Year range | 2021–2025 |
| ICD-10 scope | M00–M99 (musculoskeletal) + S00–T98 (traumatisms) |
| Total records | 21,464,771 (SIA: 15,621,400 · SIH: 5,843,371) |
| Storage | 2.1 GB enriched Parquet · 1.5 GB processed Parquet |
| Ingestion duration | 5 days continuous execution (2026-05-23 to 2026-05-28) |
| Ingestion success rate | 99.1% (3,238 / 3,268 files) |

**Benchmark validation scope (São Paulo, 2022):**

| Metric | Value |
|---|---|
| SIH hospitalizations | 66,011 |
| SIA outpatient procedures | 105,016 |
| Femur fractures (S72) | 29,359 (65.1% aged ≥60) |
| In-hospital deaths | 2,223 (crude rate: 3.4%) |
| Hospitalization cost (SIH) | R$ 175,283,428.77 |
| Inter-municipality displacements | 56.4% of hospitalizations |

Data is publicly available from the Brazilian Ministry of Health at `ftp.datasus.gov.br`. **Parquet files are not included in this repository** (gitignored). Run the ingestion pipeline to reproduce them.

---

## Canonical Schema (110 columns)

SIA and SIH records are unified into a single flat table — no joins required.

| Group | N | Example fields |
|---|---|---|
| Common (SIA + SIH) | 17 | `ano_cmpt`, `mes_cmpt`, `sistema`, `uf_origem`, `icd_group`, `opm_flag` |
| SIH-specific | 45 | `n_aih`, `dias_perm`, `val_sh`, `val_sp`, `val_ortp`, `morte`, `dt_inter` |
| SIA-specific | 33 | `pa_cnsmed`, `nome_proced`, `pa_qtdpro`, `pa_valpro`, `pa_catend` |
| Derived (unified) | 15 | `cid_principal`, `custo_total`, `cnes_estabelecimento`, `competencia_ano_mes`, `row_id` |

The 15 derived columns normalize system-specific field names into a single canonical form (e.g., `cid_principal` unifies `cid_princ` from SIH and `main_icd` from SIA), allowing the LLM prompt to reference a stable schema regardless of the underlying system.

### Clinical Enrichment Layer (AHEN v1.0.0)

A deterministic inference layer adds structured clinical fields to each record, stored in `data/enriched/` and linked to the canonical layer via `row_id`:

| Field | Description |
|---|---|
| `clinical_interpretacao_clinica` | ICD-10 group → clinical category (e.g., *doenças osteomusculares*, *lesões traumáticas*) |
| `clinical_tipo_atendimento` | System → care modality (*produção ambulatorial* / *episódio de internação*) |
| `clinical_deslocamento_territorial` | Municipality codes → displacement status (none / inter-municipal / inter-state) |
| `clinical_event_narrative` | Free-text event narrative generated deterministically from structured fields |

The current SQL pipeline queries `data/processed/` only. The enriched layer is pre-computed to support a future hybrid SQL + vector search retrieval mode over clinical narratives, without reprocessing source data.

---

## Quick Start

### Requirements

- Python 3.11+
- [`uv`](https://docs.astral.sh/uv/) (recommended package manager)
- [Ollama](https://ollama.com/download) for local inference **or** an OpenAI API key

### Installation

```bash
git clone https://github.com/rafaelrbnet/DATA-RAG-SUS.git
cd DATA-RAG-SUS

uv venv --python 3.12
source .venv/bin/activate        # Windows: .venv\Scripts\activate
uv pip install -e .
```

### Configuration

```bash
cp .env.example .env
```

**Option A — Local Ollama (default, no data leaves your machine):**

```env
LLM_PROVIDER=ollama
OLLAMA_BASE_URL=http://localhost:11434/v1
OLLAMA_MODEL=qwen2.5-coder:14b
```

Pull the model once:

```bash
ollama pull qwen2.5-coder:14b
```

**Option B — OpenAI GPT-4o:**

```env
LLM_PROVIDER=openai
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o
```

### Data Pipeline

```bash
# Stage 1 — Ingest from DATASUS FTP → data/raw/
uv run python -m src.data.ingestion

# Stage 2 — Transform to canonical schema → data/processed/
uv run python -m src.data.transform

# Stage 3 — Clinical enrichment (AHEN v1.0.0) → data/enriched/
uv run python -m src.data.clinical_inference
```

> **Note:** Full ingestion (27 states, 2021–2025) takes approximately 5 days. For a quick test, scope the ingestion to a single state and year by editing the parameters in `src/data/ingestion.py`.

### Start the API

```bash
uv run uvicorn src.api.main:app --reload
```

Health check: `GET http://localhost:8000/health`

### Query

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"question": "How many orthopedic hospitalizations occurred in São Paulo in 2022?"}'
```

**Response:**

```json
{
  "question": "How many orthopedic hospitalizations occurred in São Paulo in 2022?",
  "sql": "SELECT COUNT(*) AS total FROM processed WHERE sistema = 'SIH' AND uf_origem = '35' AND ano_cmpt = 2022",
  "result": [{"total": 66011}],
  "explanation": "There were 66,011 orthopedic hospitalizations in São Paulo in 2022."
}
```

---

## Benchmark Evaluation

```bash
# Ollama — 3 runs, temperature=0.3
uv run python scripts/evaluate_benchmark.py --runs 3 --temperature 0.3 --model ollama

# GPT-4o — 3 runs, temperature=0.0 (deterministic)
uv run python scripts/evaluate_benchmark.py --runs 3 --temperature 0.0 --model openai
```

Add `--direct` to bypass the REST API (faster; no server required):

```bash
uv run python scripts/evaluate_benchmark.py --direct --runs 3 --temperature 0.0 --model openai
```

Results are written to `results/eval_{model}_{timestamp}_r{n}.json/.md` plus an aggregate report.

Benchmark results for this paper are committed in `results/`:

| File | Description |
|---|---|
| `eval_ollama_20260606_1028_aggregate.md` | Ollama 3-run aggregate (Condition B) |
| `eval_openai_20260712_1146_aggregate.md` | GPT-4o 3-run aggregate (Condition D) |
| `eval_*.json` | Per-query results with generated SQL, LLM output, gold standard, and scoring reason |

---

## Repository Structure

```
DATA-RAG-SUS/
├── src/
│   ├── data/
│   │   ├── ingestion.py           # DATASUS FTP → Parquet (Stage 1)
│   │   ├── transform.py           # Raw → 110-column canonical schema (Stage 2)
│   │   ├── clinical_inference.py  # AHEN v1.0.0 enrichment (Stage 3)
│   │   └── dictionary.py          # Field name and code mappings
│   ├── rag/
│   │   ├── executor.py            # DuckDB query layer (Stage 4)
│   │   ├── sql_generator.py       # LLM → SQL (Stage 5)
│   │   ├── prompts.py             # Domain-engineered prompt template
│   │   └── agent.py               # End-to-end orchestration
│   └── api/
│       └── main.py                # FastAPI: POST /query · GET /health
├── scripts/
│   └── evaluate_benchmark.py      # Gold-standard EA evaluation (N=50)
├── tests/
│   ├── test_transform.py
│   ├── test_clinical_inference.py
│   ├── test_sql_generation.py
│   └── test_queries.py
├── results/                       # Benchmark outputs (JSON + Markdown)
├── docs/                          # Extended documentation (Portuguese)
├── data/
│   ├── raw/                       # Ingested Parquet — gitignored
│   ├── processed/                 # Canonical schema Parquet — gitignored
│   └── enriched/                  # AHEN enriched Parquet — gitignored
├── .env.example
├── pyproject.toml
└── LICENSE
```

---

## FAIR Compliance

Designed to conform to the [FAIR data principles](https://doi.org/10.1038/sdata.2016.18) (Wilkinson et al., 2016):

| Principle | Implementation |
|---|---|
| **Findable** | Zenodo DOI (pending assignment on public release) |
| **Accessible** | MIT License; DATASUS source data publicly available at `ftp.datasus.gov.br` |
| **Interoperable** | Apache Parquet; standard REST API; ICD-10 coding; schema documented in `src/data/transform.py` |
| **Reusable** | All benchmark queries, evaluation scripts, and results included; generalizable to any ICD-defined clinical domain and any Brazilian state |

---

## Citation

If you use SUS Data RAG in your research, please cite:

```bibtex
@article{BaenaNeto2026,
  author  = {Baena Neto, R and Ribeiro, D and Sablon, VIB},
  title   = {{SUS Data RAG}: A Code-Interpreter Framework for Natural Language
             Access to Brazilian Orthopedic Health Administrative Data ---
             A Proof of Concept},
  journal = {JMIR Medical Informatics},
  year    = {2026},
  note    = {Under review}
}
```

---

## Authors

| Name | Role |
|---|---|
| **Rafael Baena Neto** | Principal investigator, sole technical executor |
| **Denise Ribeiro** | Co-author |
| **Dr. Vicente Idalberto Becerra Sablon** | Research advisor (last author) |

Affiliation: Universidade São Francisco (USF), Master's Program in Health Sciences.

---

## License

MIT License — see [LICENSE](LICENSE).

Source data (DATASUS SIA/SIH) is publicly available from the Brazilian Ministry of Health and is not subject to redistribution restrictions. This research uses exclusively anonymized public administrative data. No informed consent or IRB review is required under Brazilian regulations (CNS Resolution 510/2016; Law 14,874/2024).
