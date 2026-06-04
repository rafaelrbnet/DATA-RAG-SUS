# Benchmark Queries — DATA-RAG-SUS

**Date:** 2026-06-04
**Version:** 1.0
**Target journal:** JMIR Medical Informatics (Q1)

---

## Scope and Dataset Description

| Attribute | Value |
|---|---|
| State | São Paulo (SP), Brazil |
| Reference year | 2022 |
| ICD scope | M00–M99 (musculoskeletal and connective tissue diseases) + S00–T98 (traumatic injuries) |
| Data sources | DATASUS SIH (inpatient — Sistema de Informações Hospitalares) + DATASUS SIA (outpatient — Sistema de Informações Ambulatoriais) |
| Schema | DATA-RAG-SUS canonical schema — 110 columns, DuckDB view `processed` over Parquet files |
| Total benchmark queries | 50 (N = 50) |
| Underlying database size | 21,094,403 rows, 3,235 Parquet files, 0.78 GB on disk |

The `processed` view is a DuckDB in-memory view created over `data/processed/**/*.parquet` using `union_by_name=true`. All 50 gold-standard queries were executed directly against this view. Elapsed times are wall-clock measurements from a single-machine execution (Apple Silicon, DuckDB v1.x, no persistent database).

---

## Research Question

> Is it possible to build an open-source Code-Interpreter RAG framework that answers clinical and epidemiological questions about orthopedic DATASUS data with accuracy ≥ 85% and full traceability?

---

## Benchmark Protocol — Execution Accuracy Definition

**Execution Accuracy (EA):** A query response from the LLM is considered correct if and only if the SQL it generates, when executed against the same DuckDB `processed` view with identical filters, returns a result that is numerically equivalent to the gold-standard result defined in this document.

Scoring rules by query type:

| Query type | Correct answer criterion |
|---|---|
| Single-value aggregation (Q01–Q15, Q31–Q33, Q36–Q37) | Exact numeric match of the returned scalar value |
| Ranked list / Top-N (Q16, Q17, Q20, Q23–Q24, Q26–Q27, Q34, Q38) | All N rows present with correct values; order must be preserved |
| Multi-column aggregation (Q19, Q21–Q22, Q25, Q28, Q30, Q35, Q39–Q40) | Exact match on all returned columns |
| Monthly/temporal series — 12 rows (Q41, Q42, Q44, Q46, Q49, Q50) | All 12 month rows present with exact values for each month |
| Monthly series — 24 rows / cross-tab (Q29, Q45) | All 24 rows present with exact values per month × system/group |
| Age-group distribution (Q18) | All 3 groups present with exact counts |
| Quarterly aggregation (Q47) | All 4 quarters present with exact values |
| Single-row temporal extremum (Q43, Q48) | Exact match on the identified month and value |

**Partial credit is not awarded.** A response that retrieves the correct ranking but omits one row, or returns a rounded value that differs from the gold standard, is scored as incorrect. The scoring unit is the query: each query contributes exactly 1 point to the accuracy numerator if correct, 0 otherwise.

**Time-to-Answer (TTA):** Measured as wall-clock elapsed time from API request submission (including prompt + schema context) to final answer delivery (SQL executed, result returned, explanation appended). The elapsed times reported in the Result column of each query section below refer exclusively to DuckDB execution time and are provided as a baseline for computational overhead assessment.

---

## N = 50 Query Distribution by Category

| Category | N | % |
|---|---:|---:|
| Epidemiológica Simples (Simple epidemiological) | 15 | 30% |
| Epidemiológica Complexa (Complex epidemiological) | 15 | 30% |
| Financeira (Financial) | 10 | 20% |
| Temporal/Comparativa (Temporal / comparative) | 10 | 20% |
| **Total** | **50** | **100%** |

---

## Summary Statistics (Gold-Standard Execution)

| Metric | Value |
|---|---|
| Total queries | 50 |
| Queries returning at least 1 row | 49 |
| Queries returning 0 rows / null result | 1 (Q35 — `internacoes_com_opme = 0`) |
| Queries with execution errors | 0 |
| Mean elapsed time (ms) | 639 |
| Median elapsed time (ms) | 638 |
| Min elapsed time (ms) | 519 (Q35) |
| Max elapsed time (ms) | 753 (Q29) |

> Elapsed times are DuckDB execution times only; they exclude LLM inference time and network round-trips.

---

## Query Sections

---

### Q01 — `Epidemiológica Simples`

**PT:** Total de internações ortopédicas (M00–M99 e S00–S99) em SP em 2022

**EN:** Total orthopedic hospital admissions (M00–M99 and S00–S99) in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
```

**Result (1 row, 619 ms):**

| total_internacoes |
|---:|
| 65,970 |

**Evaluation criterion:** Exact numeric match — `total_internacoes = 65970`.

---

### Q02 — `Epidemiológica Simples`

**PT:** Total de procedimentos ambulatoriais ortopédicos em SP em 2022

**EN:** Total orthopedic outpatient procedures in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT COUNT(*) AS total_procedimentos
FROM processed
WHERE sistema = 'SIA' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
```

**Result (1 row, 624 ms):**

| total_procedimentos |
|---:|
| 105,016 |

**Evaluation criterion:** Exact numeric match — `total_procedimentos = 105016`. Note that this query counts all SIA rows (not distinct AIH keys) because outpatient records do not carry an admission identifier.

---

### Q03 — `Epidemiológica Simples`

**PT:** Total de internações por fratura de fêmur (S72) em SP em 2022

**EN:** Total hospital admissions for femur fracture (S72) in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT COUNT(DISTINCT n_aih) AS internacoes_fratura_femur
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
```

**Result (1 row, 615 ms):**

| internacoes_fratura_femur |
|---:|
| 29,356 |

**Evaluation criterion:** Exact numeric match — `internacoes_fratura_femur = 29356`.

---

### Q04 — `Epidemiológica Simples`

**PT:** Total de internações por osteoartrose (M16, M17) em SP em 2022

**EN:** Total hospital admissions for osteoarthritis (M16, M17) in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT COUNT(DISTINCT n_aih) AS internacoes_artrose
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (cid_principal LIKE 'M16%' OR cid_principal LIKE 'M17%')
```

**Result (1 row, 647 ms):**

| internacoes_artrose |
|---:|
| 156 |

**Evaluation criterion:** Exact numeric match — `internacoes_artrose = 156`. This low count reflects the concentration of elective arthroplasty in the private/supplementary sector not captured by SUS administrative data.

---

### Q05 — `Epidemiológica Simples`

**PT:** Número de óbitos em internações ortopédicas em SP em 2022

**EN:** Number of in-hospital deaths in orthopedic admissions in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT COUNT(DISTINCT n_aih) AS obitos
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND morte = 1
```

**Result (1 row, 625 ms):**

| obitos |
|---:|
| 2,223 |

**Evaluation criterion:** Exact numeric match — `obitos = 2223`.

---

### Q06 — `Epidemiológica Simples`

**PT:** Total de internações por traumatismos (S00–T98) em SP em 2022

**EN:** Total hospital admissions for traumatic injuries (S00–T98) in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT COUNT(DISTINCT n_aih) AS internacoes_trauma
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND icd_group = 'S00-T98'
```

**Result (1 row, 620 ms):**

| internacoes_trauma |
|---:|
| 59,668 |

**Evaluation criterion:** Exact numeric match — `internacoes_trauma = 59668`.

---

### Q07 — `Epidemiológica Simples`

**PT:** Total de internações por doenças osteomusculares (M00–M99) em SP em 2022

**EN:** Total hospital admissions for musculoskeletal diseases (M00–M99) in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT COUNT(DISTINCT n_aih) AS internacoes_osteomuscular
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND icd_group = 'M00-M99'
```

**Result (1 row, 677 ms):**

| internacoes_osteomuscular |
|---:|
| 6,302 |

**Evaluation criterion:** Exact numeric match — `internacoes_osteomuscular = 6302`.

---

### Q08 — `Epidemiológica Simples`

**PT:** Número de mulheres internadas por causa ortopédica em SP em 2022

**EN:** Number of female patients admitted for orthopedic conditions in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT COUNT(DISTINCT n_aih) AS internacoes_femininas
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND sexo_paciente = 'F'
```

**Result (1 row, 616 ms):**

| internacoes_femininas |
|---:|
| 25,847 |

**Evaluation criterion:** Exact numeric match — `internacoes_femininas = 25847`.

---

### Q09 — `Epidemiológica Simples`

**PT:** Número de homens internados por causa ortopédica em SP em 2022

**EN:** Number of male patients admitted for orthopedic conditions in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT COUNT(DISTINCT n_aih) AS internacoes_masculinas
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND sexo_paciente = 'M'
```

**Result (1 row, 640 ms):**

| internacoes_masculinas |
|---:|
| 40,123 |

**Evaluation criterion:** Exact numeric match — `internacoes_masculinas = 40123`.

---

### Q10 — `Epidemiológica Simples`

**PT:** Número de internações ortopédicas de idosos (60 anos ou mais) em SP em 2022

**EN:** Number of orthopedic admissions among elderly patients (age 60+) in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT COUNT(DISTINCT n_aih) AS internacoes_idosos
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND idade_paciente >= 60
```

**Result (1 row, 657 ms):**

| internacoes_idosos |
|---:|
| 26,666 |

**Evaluation criterion:** Exact numeric match — `internacoes_idosos = 26666`.

---

### Q11 — `Epidemiológica Simples`

**PT:** Número de municípios distintos com internação ortopédica registrada em SP em 2022

**EN:** Number of distinct municipalities with recorded orthopedic admissions in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT COUNT(DISTINCT cod_munic_estabelecimento) AS municipios
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
```

**Result (1 row, 613 ms):**

| municipios |
|---:|
| 211 |

**Evaluation criterion:** Exact numeric match — `municipios = 211`.

---

### Q12 — `Epidemiológica Simples`

**PT:** Número de estabelecimentos distintos com internação ortopédica em SP em 2022

**EN:** Number of distinct healthcare facilities with orthopedic admissions in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT COUNT(DISTINCT cnes_estabelecimento) AS estabelecimentos
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
```

**Result (1 row, 624 ms):**

| estabelecimentos |
|---:|
| 381 |

**Evaluation criterion:** Exact numeric match — `estabelecimentos = 381`.

---

### Q13 — `Epidemiológica Simples`

**PT:** Número de internações ortopédicas com permanência superior a 7 dias em SP em 2022

**EN:** Number of orthopedic admissions with length of stay greater than 7 days in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT COUNT(DISTINCT n_aih) AS internacoes_longa_permanencia
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND dias_perm > 7
```

**Result (1 row, 622 ms):**

| internacoes_longa_permanencia |
|---:|
| 16,451 |

**Evaluation criterion:** Exact numeric match — `internacoes_longa_permanencia = 16451`. The threshold `> 7` (strictly greater than) must be preserved; a model using `>= 7` produces a different result.

---

### Q14 — `Epidemiológica Simples`

**PT:** Número de internações ortopédicas com uso de UTI em SP em 2022

**EN:** Number of orthopedic admissions with ICU utilization in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT COUNT(DISTINCT n_aih) AS internacoes_uti
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND uti_int_to > 0
```

**Result (1 row, 617 ms):**

| internacoes_uti |
|---:|
| 3 |

**Evaluation criterion:** Exact numeric match — `internacoes_uti = 3`. The near-zero count is a known data artifact; the correct answer is nonetheless 3.

---

### Q15 — `Epidemiológica Simples`

**PT:** Número de internações por fratura de quadril (S72) em pacientes com 70 anos ou mais em SP em 2022

**EN:** Number of hip fracture (S72) admissions in patients aged 70 or older in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT COUNT(DISTINCT n_aih) AS fraturas_quadril_idosos
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
  AND idade_paciente >= 70
```

**Result (1 row, 653 ms):**

| fraturas_quadril_idosos |
|---:|
| 15,119 |

**Evaluation criterion:** Exact numeric match — `fraturas_quadril_idosos = 15119`.

---

### Q16 — `Epidemiológica Complexa`

**PT:** Top 10 CIDs ortopédicos por volume de internação em SP em 2022

**EN:** Top 10 orthopedic ICD codes by hospital admission volume in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT cid_principal, COUNT(DISTINCT n_aih) AS total
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cid_principal
ORDER BY total DESC
LIMIT 10
```

**Result (10 rows, 676 ms):**

| # | cid_principal | total |
|---:|---|---:|
| 1 | S720 | 8,980 |
| 2 | S721 | 6,610 |
| 3 | S723 | 5,044 |
| 4 | T813 | 3,860 |
| 5 | S722 | 2,424 |
| 6–10 | (remaining 5 rows) | — |

**Evaluation criterion:** All 10 rows present in the correct descending order with exact `total` values for each `cid_principal`. The top 5 rows shown above are required; rows 6–10 must also be present with correct values.

---

### Q17 — `Epidemiológica Complexa`

**PT:** Top 5 municípios de SP com maior número de internações ortopédicas em 2022

**EN:** Top 5 municipalities in São Paulo with the highest orthopedic admission counts in 2022

**Gold-standard SQL:**

```sql
SELECT cod_munic_estabelecimento, COUNT(DISTINCT n_aih) AS total
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cod_munic_estabelecimento
ORDER BY total DESC
LIMIT 5
```

**Result (5 rows, 653 ms):**

| # | cod_munic_estabelecimento | total |
|---:|---|---:|
| 1 | 350000 | 29,824 |
| 2 | 355030 | 6,093 |
| 3 | 354870 | 1,248 |
| 4 | 350950 | 1,234 |
| 5 | 354340 | 1,150 |

**Evaluation criterion:** All 5 rows present in the correct descending order with exact `total` values.

---

### Q18 — `Epidemiológica Complexa`

**PT:** Distribuição de internações ortopédicas por faixa etária (0–17, 18–59, 60+) em SP em 2022

**EN:** Distribution of orthopedic admissions by age group (0–17, 18–59, 60+) in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT
  CASE
    WHEN idade_paciente < 18 THEN '0-17'
    WHEN idade_paciente < 60 THEN '18-59'
    ELSE '60+'
  END AS faixa_etaria,
  COUNT(DISTINCT n_aih) AS total
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND idade_paciente IS NOT NULL
GROUP BY faixa_etaria
ORDER BY faixa_etaria
```

**Result (3 rows, 632 ms):**

| faixa_etaria | total |
|---|---:|
| 0-17 | 3,619 |
| 18-59 | 35,685 |
| 60+ | 26,666 |

**Evaluation criterion:** All 3 age-group rows present with exact `total` values. The boundary conditions (`< 18` for the first group, `< 60` for the second) must be correctly encoded.

---

### Q19 — `Epidemiológica Complexa`

**PT:** Taxa de mortalidade hospitalar por CID ortopédico (top 5) em SP em 2022

**EN:** In-hospital mortality rate by orthopedic ICD code (top 5) in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT cid_principal,
  COUNT(DISTINCT n_aih) AS total_internacoes,
  SUM(morte) AS total_obitos,
  ROUND(100.0 * SUM(morte) / COUNT(DISTINCT n_aih), 2) AS taxa_mortalidade_pct
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cid_principal
HAVING total_internacoes >= 50
ORDER BY taxa_mortalidade_pct DESC
LIMIT 5
```

**Result (5 rows, 651 ms):**

| # | cid_principal | total_internacoes | total_obitos | taxa_mortalidade_pct |
|---:|---|---:|---:|---:|
| 1 | S062 | 70 | 21 | 30.00% |
| 2 | S063 | 78 | 19 | 24.36% |
| 3 | S065 | 706 | 152 | 21.53% |
| 4 | S367 | 95 | 19 | 20.00% |
| 5 | S270 | 131 | 25 | 19.08% |

**Evaluation criterion:** All 5 rows present in the correct descending order of `taxa_mortalidade_pct`, with exact values for all four columns. The `HAVING total_internacoes >= 50` filter must be applied.

---

### Q20 — `Epidemiológica Complexa`

**PT:** Top 10 CIDs ambulatoriais ortopédicos por volume de produção em SP em 2022

**EN:** Top 10 orthopedic outpatient ICD codes by procedure volume in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT cid_principal, COUNT(*) AS total
FROM processed
WHERE sistema = 'SIA' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cid_principal
ORDER BY total DESC
LIMIT 10
```

**Result (10 rows, 637 ms):**

| # | cid_principal | total |
|---:|---|---:|
| 1 | S720 | 16,466 |
| 2 | S729 | 12,180 |
| 3 | S723 | 10,624 |
| 4 | S398 | 8,107 |
| 5 | S881 | 7,604 |
| 6–10 | (remaining 5 rows) | — |

**Evaluation criterion:** All 10 rows present in the correct descending order with exact `total` values. Note that `COUNT(*)` (all SIA rows) is used, not `COUNT(DISTINCT n_aih)`.

---

### Q21 — `Epidemiológica Complexa`

**PT:** Proporção de internações ortopédicas por sexo em SP em 2022

**EN:** Proportion of orthopedic admissions by sex in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT sexo_paciente,
  COUNT(DISTINCT n_aih) AS total,
  ROUND(100.0 * COUNT(DISTINCT n_aih) / SUM(COUNT(DISTINCT n_aih)) OVER (), 1) AS pct
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY sexo_paciente
ORDER BY total DESC
```

**Result (2 rows, 632 ms):**

| sexo_paciente | total | pct |
|---|---:|---:|
| M | 40,123 | 60.8% |
| F | 25,847 | 39.2% |

**Evaluation criterion:** Both rows present with exact `total` and `pct` values (to 1 decimal place). The window function over `()` must be used; a subquery approximation that yields different rounding is scored as incorrect.

---

### Q22 — `Epidemiológica Complexa`

**PT:** Número de internações por fratura de fêmur (S72) em idosos (60+) comparado ao total em SP em 2022

**EN:** Femur fracture (S72) admissions in elderly (60+) vs total, São Paulo 2022

**Gold-standard SQL:**

```sql
SELECT
  COUNT(DISTINCT n_aih) AS total_s72,
  SUM(CASE WHEN idade_paciente >= 60 THEN 1 ELSE 0 END) AS s72_idosos,
  ROUND(100.0 * SUM(CASE WHEN idade_paciente >= 60 THEN 1 ELSE 0 END) / COUNT(DISTINCT n_aih), 1) AS pct_idosos
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
```

**Result (1 row, 644 ms):**

| total_s72 | s72_idosos | pct_idosos |
|---:|---:|---:|
| 29,356 | 19,120 | 65.1% |

**Evaluation criterion:** Exact match on all three columns — `total_s72 = 29356`, `s72_idosos = 19120`, `pct_idosos = 65.1`.

---

### Q23 — `Epidemiológica Complexa`

**PT:** Top 5 CIDs ortopédicos com maior permanência média hospitalar em SP em 2022

**EN:** Top 5 orthopedic ICD codes with highest mean length of stay in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT cid_principal,
  COUNT(DISTINCT n_aih) AS total_internacoes,
  ROUND(AVG(dias_perm), 1) AS permanencia_media_dias
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND dias_perm IS NOT NULL
GROUP BY cid_principal
HAVING total_internacoes >= 30
ORDER BY permanencia_media_dias DESC
LIMIT 5
```

**Result (5 rows, 622 ms):**

| # | cid_principal | total_internacoes | permanencia_media_dias |
|---:|---|---:|---:|
| 1 | S063 | 78 | 16.4 |
| 2 | S122 | 46 | 15.9 |
| 3 | S320 | 48 | 14.1 |
| 4 | S062 | 70 | 14.0 |
| 5 | S221 | 41 | 13.7 |

**Evaluation criterion:** All 5 rows in the correct descending order with exact values. The `HAVING total_internacoes >= 30` filter must be applied.

---

### Q24 — `Epidemiológica Complexa`

**PT:** Top 5 estabelecimentos com maior volume de internação ortopédica em SP em 2022

**EN:** Top 5 healthcare facilities with highest orthopedic admission volume in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT cnes_estabelecimento,
  COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cnes_estabelecimento
ORDER BY total_internacoes DESC
LIMIT 5
```

**Result (5 rows, 633 ms):**

| # | cnes_estabelecimento | total_internacoes |
|---:|---|---:|
| 1 | 2078015 | 2,221 |
| 2 | 2091399 | 1,676 |
| 3 | 2077396 | 1,619 |
| 4 | 2081695 | 1,230 |
| 5 | 7373465 | 1,204 |

**Evaluation criterion:** All 5 rows in the correct descending order with exact `total_internacoes` values.

---

### Q25 — `Epidemiológica Complexa`

**PT:** Distribuição de internações ortopédicas por raça/cor do paciente em SP em 2022

**EN:** Distribution of orthopedic admissions by patient race/ethnicity in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT raca_cor_paciente,
  COUNT(DISTINCT n_aih) AS total,
  ROUND(100.0 * COUNT(DISTINCT n_aih) / SUM(COUNT(DISTINCT n_aih)) OVER (), 1) AS pct
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND raca_cor_paciente IS NOT NULL
GROUP BY raca_cor_paciente
ORDER BY total DESC
```

**Result (6 rows, 631 ms):**

| # | raca_cor_paciente | total | pct |
|---:|---|---:|---:|
| 1 | 01 | 38,121 | 57.8% |
| 2 | 03 | 18,259 | 27.7% |
| 3 | 99 | 5,700 | 8.6% |
| 4 | 02 | 3,418 | 5.2% |
| 5 | 04 | 462 | 0.7% |
| 6 | (6th category) | — | — |

**Evaluation criterion:** All 6 rows present in the correct descending order with exact `total` and `pct` values. The `IS NOT NULL` filter on `raca_cor_paciente` must be applied.

---

### Q26 — `Epidemiológica Complexa`

**PT:** CIDs ortopédicos com maior número de dias totais de internação em SP em 2022

**EN:** Orthopedic ICD codes with highest total inpatient days in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT cid_principal,
  COUNT(DISTINCT n_aih) AS internacoes,
  SUM(dias_perm) AS total_dias_internacao
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND dias_perm IS NOT NULL
GROUP BY cid_principal
ORDER BY total_dias_internacao DESC
LIMIT 10
```

**Result (10 rows, 645 ms):**

| # | cid_principal | internacoes | total_dias_internacao |
|---:|---|---:|---:|
| 1 | S720 | 8,980 | 64,885 |
| 2 | S721 | 6,610 | 44,099 |
| 3 | T813 | 3,860 | 28,976 |
| 4 | S723 | 5,044 | 27,024 |
| 5 | S722 | 2,424 | 16,004 |
| 6–10 | (remaining 5 rows) | — | — |

**Evaluation criterion:** All 10 rows in the correct descending order of `total_dias_internacao` with exact values for all three columns.

---

### Q27 — `Epidemiológica Complexa`

**PT:** Top 5 procedimentos ambulatoriais ortopédicos mais realizados em SP em 2022

**EN:** Top 5 most performed orthopedic outpatient procedures in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT cod_procedimento, COUNT(*) AS total
FROM processed
WHERE sistema = 'SIA' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cod_procedimento
ORDER BY total DESC
LIMIT 5
```

**Result (5 rows, 656 ms):**

| # | cod_procedimento | total |
|---:|---|---:|
| 1 | 0302050019 | 70,536 |
| 2 | 0701060018 | 6,235 |
| 3 | 0701050047 | 4,334 |
| 4 | 0701060034 | 2,046 |
| 5 | 0701010142 | 2,001 |

**Evaluation criterion:** All 5 rows in the correct descending order with exact `total` values.

---

### Q28 — `Epidemiológica Complexa`

**PT:** Proporção de internações ortopédicas com paciente de outro município em SP em 2022

**EN:** Proportion of orthopedic admissions with patient from a different municipality in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT
  COUNT(DISTINCT n_aih) AS total_internacoes,
  SUM(CASE WHEN cod_munic_residencia != cod_munic_estabelecimento AND cod_munic_residencia IS NOT NULL THEN 1 ELSE 0 END) AS deslocamento_intermunicipal,
  ROUND(100.0 * SUM(CASE WHEN cod_munic_residencia != cod_munic_estabelecimento AND cod_munic_residencia IS NOT NULL THEN 1 ELSE 0 END) / COUNT(DISTINCT n_aih), 1) AS pct_deslocamento
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
```

**Result (1 row, 632 ms):**

| total_internacoes | deslocamento_intermunicipal | pct_deslocamento |
|---:|---:|---:|
| 65,970 | 37,219 | 56.4% |

**Evaluation criterion:** Exact match on all three columns. The condition `cod_munic_residencia != cod_munic_estabelecimento AND cod_munic_residencia IS NOT NULL` must be correctly encoded.

---

### Q29 — `Epidemiológica Complexa`

**PT:** Comparativo de volume mensal SIA vs SIH ortopédico em SP em 2022

**EN:** Monthly volume comparison of orthopedic SIA vs SIH records in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT mes_cmpt, sistema, COUNT(*) AS total
FROM processed
WHERE uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt, sistema
ORDER BY mes_cmpt, sistema
```

**Result (24 rows, 753 ms):**

| mes_cmpt | sistema | total |
|---:|---|---:|
| 1 | SIA | 5,898 |
| 1 | SIH | 5,212 |
| 2 | SIA | 7,116 |
| 2 | SIH | 5,022 |
| 3 | SIA | 8,509 |
| … | … | … |
| 12 | SIA | — |
| 12 | SIH | — |

**Evaluation criterion:** All 24 rows (12 months × 2 systems) present with exact `total` values per `mes_cmpt` × `sistema` combination.

---

### Q30 — `Epidemiológica Complexa`

**PT:** Idade média dos pacientes internados por fratura de fêmur (S72) em SP em 2022

**EN:** Mean age of patients admitted for femur fracture (S72) in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT
  ROUND(AVG(idade_paciente), 1) AS idade_media,
  MIN(idade_paciente) AS idade_minima,
  MAX(idade_paciente) AS idade_maxima
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
  AND idade_paciente IS NOT NULL
```

**Result (1 row, 681 ms):**

| idade_media | idade_minima | idade_maxima |
|---:|---:|---:|
| 62.4 | 0 | 99 |

**Evaluation criterion:** Exact match on all three columns — `idade_media = 62.4`, `idade_minima = 0`, `idade_maxima = 99`.

---

### Q31 — `Financeira`

**PT:** Custo total de todas as internações ortopédicas em SP em 2022

**EN:** Total cost of all orthopedic hospital admissions in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total_reais
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
```

**Result (1 row, 633 ms):**

| custo_total_reais |
|---:|
| R$ 175,283,428.77 |

**Evaluation criterion:** Exact numeric match — `custo_total_reais = 175283428.77`. `COALESCE(custo_total, 0)` must be used to handle any null values.

---

### Q32 — `Financeira`

**PT:** Custo médio por internação ortopédica em SP em 2022

**EN:** Mean cost per orthopedic hospital admission in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT ROUND(AVG(COALESCE(custo_total, 0)), 2) AS custo_medio_por_internacao
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
```

**Result (1 row, 642 ms):**

| custo_medio_por_internacao |
|---:|
| R$ 2,655.37 |

**Evaluation criterion:** Exact numeric match — `custo_medio_por_internacao = 2655.37`.

---

### Q33 — `Financeira`

**PT:** Custo total de internações por fratura de fêmur (S72) em SP em 2022

**EN:** Total cost of femur fracture (S72) admissions in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total_s72
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
```

**Result (1 row, 648 ms):**

| custo_total_s72 |
|---:|
| R$ 79,277,316.96 |

**Evaluation criterion:** Exact numeric match — `custo_total_s72 = 79277316.96`.

---

### Q34 — `Financeira`

**PT:** Top 5 CIDs ortopédicos com maior custo total de internação em SP em 2022

**EN:** Top 5 orthopedic ICD codes by total admission cost in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT cid_principal,
  COUNT(DISTINCT n_aih) AS internacoes,
  ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cid_principal
ORDER BY custo_total DESC
LIMIT 5
```

**Result (5 rows, 637 ms):**

| # | cid_principal | internacoes | custo_total |
|---:|---|---:|---:|
| 1 | S720 | 8,980 | R$ 29,879,588.90 |
| 2 | S721 | 6,610 | R$ 17,207,131.57 |
| 3 | S723 | 5,044 | R$ 12,728,219.46 |
| 4 | T813 | 3,860 | R$ 6,607,677.78 |
| 5 | S722 | 2,424 | R$ 5,726,268.91 |

**Evaluation criterion:** All 5 rows in the correct descending order with exact values for all three columns.

---

### Q35 — `Financeira`

**PT:** Custo total de internações ortopédicas com uso de OPME (val_ortp > 0) em SP em 2022

**EN:** Total cost of orthopedic admissions involving prosthetics/implants (val_ortp > 0) in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT
  COUNT(DISTINCT n_aih) AS internacoes_com_opme,
  ROUND(SUM(COALESCE(val_ortp, 0)), 2) AS custo_total_opme
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND val_ortp > 0
```

**Result (1 row, 519 ms):**

| internacoes_com_opme | custo_total_opme |
|---:|---:|
| 0 | null |

**Evaluation criterion:** Exact match — `internacoes_com_opme = 0` and `custo_total_opme = null`. This null result is a known data-quality characteristic of the `val_ortp` field in the 2022 SIH SP extract; the correct answer is not 0.00 but null. A model that returns `0.00` for the cost is scored as incorrect.

---

### Q36 — `Financeira`

**PT:** Valor total de honorários profissionais (val_sp) em internações ortopédicas em SP em 2022

**EN:** Total professional fees (val_sp) for orthopedic admissions in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT ROUND(SUM(COALESCE(val_sp, 0)), 2) AS total_honorarios
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
```

**Result (1 row, 650 ms):**

| total_honorarios |
|---:|
| R$ 30,640,001.52 |

**Evaluation criterion:** Exact numeric match — `total_honorarios = 30640001.52`.

---

### Q37 — `Financeira`

**PT:** Custo médio por dia de internação ortopédica em SP em 2022

**EN:** Mean cost per inpatient day for orthopedic admissions in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT ROUND(SUM(COALESCE(custo_total, 0)) / NULLIF(SUM(dias_perm), 0), 2) AS custo_por_dia
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND dias_perm > 0
```

**Result (1 row, 636 ms):**

| custo_por_dia |
|---:|
| R$ 432.54 |

**Evaluation criterion:** Exact numeric match — `custo_por_dia = 432.54`. `NULLIF(SUM(dias_perm), 0)` must be used to guard against division by zero.

---

### Q38 — `Financeira`

**PT:** Top 5 estabelecimentos com maior custo total de internações ortopédicas em SP em 2022

**EN:** Top 5 facilities with highest total orthopedic admission costs in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT cnes_estabelecimento,
  COUNT(DISTINCT n_aih) AS internacoes,
  ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cnes_estabelecimento
ORDER BY custo_total DESC
LIMIT 5
```

**Result (5 rows, 628 ms):**

| # | cnes_estabelecimento | internacoes | custo_total |
|---:|---|---:|---:|
| 1 | 2078015 | 2,221 | R$ 11,218,869.54 |
| 2 | 2077396 | 1,619 | R$ 7,242,879.23 |
| 3 | 2078775 | 1,068 | R$ 4,066,905.35 |
| 4 | 2081695 | 1,230 | R$ 3,701,329.73 |
| 5 | 2688689 | 1,011 | R$ 3,622,383.58 |

**Evaluation criterion:** All 5 rows in the correct descending order with exact values. Note that the volume ranking (Q24) and the cost ranking differ: facility 2091399 is second by volume but does not appear in the top-5 by cost.

---

### Q39 — `Financeira`

**PT:** Custo total de procedimentos ambulatoriais ortopédicos em SP em 2022

**EN:** Total cost of orthopedic outpatient procedures in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total_sia
FROM processed
WHERE sistema = 'SIA' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
```

**Result (1 row, 646 ms):**

| custo_total_sia |
|---:|
| R$ 8,721,217.91 |

**Evaluation criterion:** Exact numeric match — `custo_total_sia = 8721217.91`.

---

### Q40 — `Financeira`

**PT:** Proporção custo serviço hospitalar vs honorários em internações ortopédicas em SP em 2022

**EN:** Ratio of hospital service costs vs professional fees in orthopedic admissions, São Paulo 2022

**Gold-standard SQL:**

```sql
SELECT
  ROUND(SUM(COALESCE(val_sh, 0)), 2) AS custo_servico_hospitalar,
  ROUND(SUM(COALESCE(val_sp, 0)), 2) AS honorarios_profissionais,
  ROUND(100.0 * SUM(COALESCE(val_sh, 0)) / NULLIF(SUM(COALESCE(val_sh, 0)) + SUM(COALESCE(val_sp, 0)), 0), 1) AS pct_hospitalar
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
```

**Result (1 row, 665 ms):**

| custo_servico_hospitalar | honorarios_profissionais | pct_hospitalar |
|---:|---:|---:|
| R$ 144,636,796.74 | R$ 30,640,001.52 | 82.5% |

**Evaluation criterion:** Exact match on all three columns. `NULLIF` must be used in the denominator.

---

### Q41 — `Temporal/Comparativa`

**PT:** Distribuição mensal de internações ortopédicas em SP em 2022

**EN:** Monthly distribution of orthopedic hospital admissions in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT mes_cmpt, COUNT(DISTINCT n_aih) AS internacoes
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY mes_cmpt
```

**Result (12 rows, 658 ms):**

| mes_cmpt | internacoes |
|---:|---:|
| 1 | 5,209 |
| 2 | 5,021 |
| 3 | 5,604 |
| 4 | 5,136 |
| 5 | 5,416 |
| 6 | — |
| 7 | — |
| 8 | — |
| 9 | — |
| 10 | — |
| 11 | — |
| 12 | — |

**Evaluation criterion:** All 12 monthly rows present in chronological order with exact `internacoes` values. Months 6–12 must also match the gold-standard values.

---

### Q42 — `Temporal/Comparativa`

**PT:** Distribuição mensal de procedimentos ambulatoriais ortopédicos em SP em 2022

**EN:** Monthly distribution of orthopedic outpatient procedures in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT mes_cmpt, COUNT(*) AS procedimentos
FROM processed
WHERE sistema = 'SIA' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY mes_cmpt
```

**Result (12 rows, 649 ms):**

| mes_cmpt | procedimentos |
|---:|---:|
| 1 | 5,898 |
| 2 | 7,116 |
| 3 | 8,509 |
| 4 | 8,352 |
| 5 | 9,535 |
| 6–12 | (remaining 7 months) |

**Evaluation criterion:** All 12 monthly rows present with exact `procedimentos` values. `COUNT(*)` must be used (not `COUNT(DISTINCT n_aih)`).

---

### Q43 — `Temporal/Comparativa`

**PT:** Mês com maior número de internações por fratura de fêmur (S72) em SP em 2022

**EN:** Month with the highest number of femur fracture (S72) admissions in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT mes_cmpt, COUNT(DISTINCT n_aih) AS fraturas
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
GROUP BY mes_cmpt
ORDER BY fraturas DESC
LIMIT 1
```

**Result (1 row, 635 ms):**

| mes_cmpt | fraturas |
|---:|---:|
| 8 | 2,757 |

**Evaluation criterion:** Exact match — `mes_cmpt = 8` (August) with `fraturas = 2757`.

---

### Q44 — `Temporal/Comparativa`

**PT:** Evolução mensal do custo total de internações ortopédicas em SP em 2022

**EN:** Monthly evolution of total orthopedic admission costs in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT mes_cmpt, ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY mes_cmpt
```

**Result (12 rows, 691 ms):**

| mes_cmpt | custo_total |
|---:|---:|
| 1 | R$ 12,942,488.31 |
| 2 | R$ 12,530,190.26 |
| 3 | R$ 14,223,832.66 |
| 4 | R$ 13,771,879.44 |
| 5 | R$ 14,952,640.76 |
| 6–12 | (remaining 7 months) |

**Evaluation criterion:** All 12 monthly rows present with exact `custo_total` values (to 2 decimal places).

---

### Q45 — `Temporal/Comparativa`

**PT:** Volume mensal de internações por trauma (S00–T98) vs doenças musculoesqueléticas (M00–M99) em SP em 2022

**EN:** Monthly admissions: traumatic injuries (S00–T98) vs musculoskeletal diseases (M00–M99), São Paulo 2022

**Gold-standard SQL:**

```sql
SELECT mes_cmpt, icd_group, COUNT(DISTINCT n_aih) AS internacoes
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt, icd_group
ORDER BY mes_cmpt, icd_group
```

**Result (24 rows, 626 ms):**

| mes_cmpt | icd_group | internacoes |
|---:|---|---:|
| 1 | M00-M99 | 476 |
| 1 | S00-T98 | 4,733 |
| 2 | M00-M99 | 461 |
| 2 | S00-T98 | 4,560 |
| 3 | M00-M99 | 511 |
| … | … | … |
| 12 | M00-M99 | — |
| 12 | S00-T98 | — |

**Evaluation criterion:** All 24 rows (12 months × 2 ICD groups) present in the correct order with exact `internacoes` values.

---

### Q46 — `Temporal/Comparativa`

**PT:** Evolução mensal de óbitos em internações ortopédicas em SP em 2022

**EN:** Monthly evolution of in-hospital deaths in orthopedic admissions in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT mes_cmpt, SUM(morte) AS total_obitos
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY mes_cmpt
```

**Result (12 rows, 643 ms):**

| mes_cmpt | total_obitos |
|---:|---:|
| 1 | 179 |
| 2 | 182 |
| 3 | 167 |
| 4 | 156 |
| 5 | 187 |
| 6–12 | (remaining 7 months) |

**Evaluation criterion:** All 12 monthly rows present with exact `total_obitos` values. `SUM(morte)` must be used, not `COUNT(DISTINCT n_aih) WHERE morte = 1` (which may differ due to deduplication).

---

### Q47 — `Temporal/Comparativa`

**PT:** Permanência média por trimestre nas internações ortopédicas em SP em 2022

**EN:** Mean length of stay by quarter for orthopedic admissions in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT
  CASE
    WHEN mes_cmpt BETWEEN 1 AND 3 THEN 'Q1'
    WHEN mes_cmpt BETWEEN 4 AND 6 THEN 'Q2'
    WHEN mes_cmpt BETWEEN 7 AND 9 THEN 'Q3'
    ELSE 'Q4'
  END AS trimestre,
  ROUND(AVG(dias_perm), 1) AS permanencia_media_dias
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND dias_perm IS NOT NULL
GROUP BY trimestre
ORDER BY trimestre
```

**Result (4 rows, 633 ms):**

| trimestre | permanencia_media_dias |
|---|---:|
| Q1 | 5.8 |
| Q2 | 6.0 |
| Q3 | 6.2 |
| Q4 | 6.1 |

**Evaluation criterion:** All 4 quarter rows present with exact `permanencia_media_dias` values (to 1 decimal place). The `BETWEEN` boundaries must match the gold standard exactly.

---

### Q48 — `Temporal/Comparativa`

**PT:** Mês com maior custo médio por internação ortopédica em SP em 2022

**EN:** Month with the highest mean cost per orthopedic admission in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT mes_cmpt, ROUND(AVG(COALESCE(custo_total, 0)), 2) AS custo_medio
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY custo_medio DESC
LIMIT 1
```

**Result (1 row, 616 ms):**

| mes_cmpt | custo_medio |
|---:|---:|
| 10 | R$ 2,780.02 |

**Evaluation criterion:** Exact match — `mes_cmpt = 10` (October) with `custo_medio = 2780.02`.

---

### Q49 — `Temporal/Comparativa`

**PT:** Sazonalidade mensal de fraturas de fêmur (S72) em SP em 2022

**EN:** Monthly seasonality of femur fractures (S72) in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT mes_cmpt, COUNT(DISTINCT n_aih) AS fraturas_femur
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
GROUP BY mes_cmpt
ORDER BY mes_cmpt
```

**Result (12 rows, 650 ms):**

| mes_cmpt | fraturas_femur |
|---:|---:|
| 1 | 2,220 |
| 2 | 2,076 |
| 3 | 2,385 |
| 4 | 2,207 |
| 5 | 2,337 |
| 6–12 | (remaining 7 months) |

**Evaluation criterion:** All 12 monthly rows present in chronological order with exact `fraturas_femur` values.

---

### Q50 — `Temporal/Comparativa`

**PT:** Volume mensal de internações ortopédicas de idosos (60 anos ou mais) em SP em 2022

**EN:** Monthly volume of orthopedic admissions among elderly patients (60+) in São Paulo in 2022

**Gold-standard SQL:**

```sql
SELECT mes_cmpt, COUNT(DISTINCT n_aih) AS internacoes_idosos
FROM processed
WHERE sistema = 'SIH' AND uf_origem = 'SP' AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND idade_paciente >= 60
GROUP BY mes_cmpt
ORDER BY mes_cmpt
```

**Result (12 rows, 639 ms):**

| mes_cmpt | internacoes_idosos |
|---:|---:|
| 1 | 1,987 |
| 2 | 1,920 |
| 3 | 2,145 |
| 4 | 2,021 |
| 5 | 2,207 |
| 6–12 | (remaining 7 months) |

**Evaluation criterion:** All 12 monthly rows present in chronological order with exact `internacoes_idosos` values.

---

## Methodology Note

### Execution Accuracy (EA)

Execution Accuracy is the primary metric used to evaluate the RAG system. A system response is scored as correct (EA = 1) for a given query if and only if:

1. The LLM generates syntactically valid DuckDB SQL;
2. The SQL executes without runtime error against the `processed` view;
3. The result set is numerically equivalent to the gold-standard result defined in this document, according to the type-specific criteria in the Benchmark Protocol section above.

The overall EA score for a run is defined as:

```
EA = (number of queries scored correct) / 50
```

The target threshold for publication is EA >= 0.85 (i.e., >= 43 of 50 queries correct).

### Scoring by Result Type

**Exact scalar match** — applies to all queries returning a single numeric value (most queries in categories Epidemiológica Simples and Financeira). The returned value must match the gold standard to 2 decimal places for monetary values and to the precision shown in the result table for all other scalars.

**Ranked list match** — applies to all Top-N queries. All N rows must be present. The ordering must be preserved. If the LLM returns the correct set of rows but in a different order, the response is scored as incorrect.

**Monthly series match** — applies to all temporal queries returning 12 rows (one per calendar month). All 12 values must match. Partial series (e.g., only 5 months returned) are scored as incorrect.

**Cross-tab series match** — applies to Q29 (24 rows: 12 months × 2 systems) and Q45 (24 rows: 12 months × 2 ICD groups). All 24 cells must match.

**Multi-column single-row match** — applies to Q22, Q28, Q30, Q35, Q40. All columns in the single result row must match exactly.

**Known null/zero result** — Q35 returns `internacoes_com_opme = 0` and `custo_total_opme = null`. The system must reproduce this null result. Substituting `null` with `0.00` is scored as incorrect.

### Time-to-Answer (TTA) Measurement Protocol

TTA is measured from the moment the user submits the natural language question to the moment the system returns the complete response (SQL + executed result + natural language explanation). Measurement is performed using monotonic wall-clock time (`time.perf_counter()` in Python). Three components are logged separately:

| Component | Description |
|---|---|
| `t_llm` | Time from API request to receiving the complete LLM response (including SQL) |
| `t_exec` | DuckDB query execution time (gold-standard baselines shown in each query section) |
| `t_explain` | Time for the LLM to generate the natural language explanation of the result |
| `TTA` | `t_llm + t_exec + t_explain` (total end-to-end) |

The DuckDB execution baselines reported in this document were obtained on a single machine (Apple Silicon) using an in-memory DuckDB connection over local Parquet files. These values represent a lower bound for `t_exec` in production deployments.

### Reproducibility

All 50 gold-standard queries were executed against the fixed, versioned Parquet snapshot `data/processed/` (commit `fc8bf1b`, 2026-06-04). The schema is the DATA-RAG-SUS canonical schema with 110 columns. Any replication must use the same Parquet snapshot and the same DuckDB version to guarantee identical numeric results, as DuckDB's floating-point rounding behavior may differ across versions.

---

## Footer

**Data sources:** DATASUS SIA (Sistema de Informações Ambulatoriais) + DATASUS SIH (Sistema de Informações Hospitalares). Data publicly available at [datasus.saude.gov.br](https://datasus.saude.gov.br). Reference period: São Paulo state, January–December 2022.

**Schema:** DATA-RAG-SUS canonical schema — 110 columns, DuckDB `processed` view over Parquet files. Schema documentation: `docs/06.2-normalizacao-colunas-completas.md` and `docs/06.1-dominio-colunas-completas.md`.

**Query execution engine:** DuckDB (in-memory, `:memory:` connection, `union_by_name=true` Parquet reader).

**LLM evaluated:** GPT-4o (target; additional runs with open-source models including Ollama/Llama variants are planned for the ablation study).

**Repository:** [github.com/rafaelbaena/DATA-RAG-SUS](https://github.com/rafaelbaena/DATA-RAG-SUS) — MIT License.

**Authors:** Rafael Baena et al. — Universidade São Francisco (USF), Mestrado em Ciências da Saúde.

**Citation (provisional):** Baena R et al. "An open-source Code-Interpreter RAG framework for clinical and epidemiological queries on DATASUS orthopedic data." *JMIR Medical Informatics*, 2026 (submitted).

---

*This benchmark document is a living artifact. Gold-standard results were verified by direct DuckDB execution on 2026-06-04. Any discrepancy between this document and a re-execution on a different data snapshot should be resolved by re-running the gold-standard SQL against the versioned Parquet files at commit `fc8bf1b`.*
