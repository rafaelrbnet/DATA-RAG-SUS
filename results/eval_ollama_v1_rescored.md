# Benchmark Evaluation Report — SUS Data RAG

**Model:** ollama (qwen2.5-coder:14b) — Condição A: zero-shot  
**Run date:** 2026-06-04 19:51 [re-scored 2026-06-04]  
**API:** http://127.0.0.1:8000  

## Summary

| Metric | Value |
|---|---|
| Total queries | 50 |
| Correct (EA numerator) | 6 |
| **Execution Accuracy (EA)** | **12.0%** |
| Wilson IC 95% | [5.6%, 23.8%] |
| Correct via flexible match (alias diferente) | 5 |
| API errors / timeouts | 3 |
| Mean Time-to-Answer | 31.7s |

> **Nota metodológica — Scoring:** A Execution Accuracy avalia se o SQL gerado
> retorna os mesmos **valores** que o gold-standard, independente do nome das colunas.
> Quando o LLM usa um alias diferente mas retorna os dados corretos (match posicional),
> a query é contada como correta e sinalizada com `~`. Esta decisão segue a definição
> de EA em [Lee et al., 2022] — comparação de conjuntos de resultado, não de SQL texto.

## EA by Category

| Category | Correct | Total | EA |
|---|---:|---:|---:|
| Epidemiológica Simples | 4 | 15 | 26.7% |
| Epidemiológica Complexa | 0 | 15 | 0.0% |
| Financeira | 0 | 10 | 0.0% |
| Temporal/Comparativa | 2 | 10 | 20.0% |

## Query Results

### ✅ Q01 — Epidemiológica Simples

**Q:** Total de internações ortopédicas (M00-M99 e S00-S99) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes_ortopedicas
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=65970 exp=65970`
**TTA:** 32726ms

---

### ❌ Q02 — Epidemiológica Simples

**Q:** Total de procedimentos ambulatoriais ortopédicos em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(*) AS total_procedimentos_ortopedicos
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
  AND clinical_interpretacao_clinica = 'doencas osteomusculares e do tecido conjuntivo';
```

**Resultado:** `got=10648 exp=105016`
**TTA:** 27826ms

---

### ❌ Q03 — Epidemiológica Simples

**Q:** Total de internações por fratura de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (cid_principal LIKE 'S72%' OR icd_group = 'S00-T98');
```

**Resultado:** `got=59668 exp=29356`
**TTA:** 31044ms

---

### ✅ Q04 — Epidemiológica Simples

**Q:** Total de internações por osteoartrose (M16, M17) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (cid_principal LIKE 'M16%' OR cid_principal LIKE 'M17%');
```

**Resultado:** `got=156 exp=156`
**TTA:** 29515ms

---

### ❌ Q05 — Epidemiológica Simples

**Q:** Número de óbitos em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS numero_obitos
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND morte = 1
  AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99');
```

**Resultado:** `got=73 exp=2223`
**TTA:** 31627ms

---

### ✅ Q06 — Epidemiológica Simples

**Q:** Total de internações por traumatismos (S00-T98) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (cid_principal LIKE 'S%' OR icd_group = 'S00-T98');
```

**Resultado:** `got=59668 exp=59668`
**TTA:** 30107ms

---

### ✅ Q07 — Epidemiológica Simples

**Q:** Total de internações por doenças osteomusculares (M00-M99) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND icd_group = 'M00-M99';
```

**Resultado:** `got=6302 exp=6302`
**TTA:** 27663ms

---

### ❌ Q08 — Epidemiológica Simples

**Q:** Número de mulheres internadas por causa ortopédica em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS numero_internacoes
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND sexo_paciente = 'F'
  AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99');
```

**Resultado:** `got=2327 exp=25847`
**TTA:** 25570ms

---

### ❌ Q09 — Epidemiológica Simples

**Q:** Número de homens internados por causa ortopédica em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS numero_internacoes
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND sexo_paciente = 'M'
  AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99');
```

**Resultado:** `got=3975 exp=40123`
**TTA:** 28771ms

---

### ❌ Q10 — Epidemiológica Simples

**Q:** Número de internações ortopédicas de idosos (60 anos ou mais) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih)
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND idade_paciente >= 60
  AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99');
```

**Resultado:** `got=1685 exp=26666`
**TTA:** 31169ms

---

### ❌ Q11 — Epidemiológica Simples

**Q:** Número de municípios distintos com internação ortopédica registrada em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT cod_munic_residencia) AS num_municipios_distintos
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99');
```

**Resultado:** `got=496 exp=211`
**TTA:** 30448ms

---

### ❌ Q12 — Epidemiológica Simples

**Q:** Número de estabelecimentos distintos com internação ortopédica em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT cod_munic_estabelecimento) AS num_estabelecimentos_distintos
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99');
```

**Resultado:** `got=158 exp=381`
**TTA:** 26570ms

---

### ❌ Q13 — Epidemiológica Simples

**Q:** Número de internações ortopédicas com permanência superior a 7 dias em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS numero_internacoes
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
  AND dias_perm > 7;
```

**Resultado:** `got=1383 exp=16451`
**TTA:** 26125ms

---

### ❌ Q14 — Epidemiológica Simples

**Q:** Número de internações ortopédicas com uso de UTI em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS numero_internacoes_uti
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
  AND uti_int_to > 0;
```

**Resultado:** `got=0 exp=3`
**TTA:** 31348ms

---

### ❌ Q15 — Epidemiológica Simples

**Q:** Número de internações por fratura de quadril (S72) em pacientes com 70 anos ou mais em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS numero_internacoes
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND idade_paciente >= 70
  AND (cid_principal LIKE 'S72%' OR icd_group = 'S00-T98');
```

**Resultado:** `got=17608 exp=15119`
**TTA:** 24477ms

---

### ❌ Q16 — Epidemiológica Complexa

**Q:** Top 10 CIDs ortopédicos por volume de internação em SP em 2022

**SQL gerado:**
```sql
SELECT 
    cid_principal,
    COUNT(DISTINCT n_aih) AS num_internacoes
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH' 
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    cid_principal
ORDER BY 
    num_internacoes DESC
LIMIT 10;
```

**Resultado:** `Linha 1: got={'cid_principal': 'M861', 'num_internacoes': 912} exp={'cid_principal': 'S720', 'total': 8980}`
**TTA:** 38670ms

---

### ❌ Q17 — Epidemiológica Complexa

**Q:** Top 5 municípios de SP com maior número de internações ortopédicas em 2022

**SQL gerado:**
```sql
SELECT 
    cod_munic_residencia,
    COUNT(DISTINCT n_aih) AS num_internacoes
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH' 
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    cod_munic_residencia
ORDER BY 
    num_internacoes DESC
LIMIT 5;
```

**Resultado:** `Linha 1: got={'cod_munic_residencia': '355030', 'num_internacoes': 1190} exp={'cod_munic_estabelecimento': '350000', 'total': 29824}`
**TTA:** 41621ms

---

### ❌ Q18 — Epidemiológica Complexa

**Q:** Distribuição de internações ortopédicas por faixa etária (0-17, 18-59, 60+) em SP em 2022

**SQL gerado:**
```sql
SELECT 
    CASE 
        WHEN idade_paciente BETWEEN 0 AND 17 THEN '0-17'
        WHEN idade_paciente BETWEEN 18 AND 59 THEN '18-59'
        ELSE '60+'
    END AS faixa_etaria,
    COUNT(DISTINCT n_aih) AS quantidade_internacoes
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH'
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    faixa_etaria;
```

**Resultado:** `Linha não encontrada: {'faixa_etaria': '0-17', 'total': 3619}`
**TTA:** 34432ms

---

### ❌ Q19 — Epidemiológica Complexa

**Q:** Taxa de mortalidade hospitalar por CID ortopédico (top 5) em SP em 2022

**SQL gerado:**
```sql
SELECT 
    cid_principal,
    COUNT(DISTINCT n_aih) AS total_internacoes,
    SUM(morte) AS total_obitos,
    (SUM(morte) * 1.0 / COUNT(DISTINCT n_aih)) * 100 AS taxa_mortalidade
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH'
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    cid_principal
ORDER BY 
    taxa_mortalidade DESC
LIMIT 5;
```

**Resultado:** `Linha 1: got={'cid_principal': 'M895', 'total_internacoes': 2, 'total_obitos': 1.0, 'taxa_mortalidade': 50.0} exp={'cid_principal': 'S062', 'total_internacoes': 70, 'total_obitos': 21, 'taxa_mortalidade_pct': 30.0}`
**TTA:** 37371ms

---

### ❌ Q20 — Epidemiológica Complexa

**Q:** Top 10 CIDs ambulatoriais ortopédicos por volume de produção em SP em 2022

**SQL gerado:**
```sql
SELECT 
    cid_principal,
    COUNT(*) AS volume_producao
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIA' 
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    cid_principal
ORDER BY 
    volume_producao DESC
LIMIT 10;
```

**Resultado:** `Linha 1: got={'cid_principal': 'M216', 'volume_producao': 4743} exp={'cid_principal': 'S720', 'total': 16466}`
**TTA:** 42183ms

---

### ⚠️ Q21 — Epidemiológica Complexa

**Q:** Proporção de internações ortopédicas por sexo em SP em 2022

**SQL gerado:**
```sql
SELECT 
    sexo_paciente,
    COUNT(DISTINCT n_aih) AS quantidade_internacoes,
    (COUNT(DISTINCT n_aih) * 1.0 / total_internacoes) AS proporcao
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH'
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    sexo_paciente
WITH ROLLUP;
```

**Erro:** Falha na execução do SQL: Parser Error: syntax error at or near "WITH"

LINE 16: WITH ROLLUP;
         ^

---

### ⚠️ Q22 — Epidemiológica Complexa

**Q:** Número de internações por fratura de fêmur (S72) em idosos (60+) comparado ao total em SP em 2022

**Erro:** HTTP 500: {"detail":"Falha na geração de SQL: Somente SELECT é permitido. SQL recebido começava com: WITH filtered_data AS (\n    SELECT \n        p.n_aih,\n        COUNT(DISTINCT p.n_"}

---

### ❌ Q23 — Epidemiológica Complexa

**Q:** Top 5 CIDs ortopédicos com maior permanência média hospitalar em SP em 2022

**SQL gerado:**
```sql
SELECT 
    cid_principal,
    AVG(dias_perm) AS media_permanencia
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH' 
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    cid_principal
ORDER BY 
    media_permanencia DESC
LIMIT 5;
```

**Resultado:** `Linha 1: got={'cid_principal': 'M318', 'media_permanencia': 36.0} exp={'cid_principal': 'S063', 'total_internacoes': 78, 'permanencia_media_dias': 16.4}`
**TTA:** 36459ms

---

### ❌ Q24 — Epidemiológica Complexa

**Q:** Top 5 estabelecimentos com maior volume de internação ortopédica em SP em 2022

**SQL gerado:**
```sql
SELECT 
    cod_munic_estabelecimento, 
    COUNT(DISTINCT n_aih) AS num_internacoes
FROM 
    processed
WHERE 
    sistema = 'SIH' 
    AND uf_origem = 'SP' 
    AND ano_cmpt = 2022 
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    cod_munic_estabelecimento
ORDER BY 
    num_internacoes DESC
LIMIT 5;
```

**Resultado:** `Linha 1: got={'cod_munic_estabelecimento': '350000', 'num_internacoes': 2800} exp={'cnes_estabelecimento': '2078015', 'total_internacoes': 2221}`
**TTA:** 34689ms

---

### ❌ Q25 — Epidemiológica Complexa

**Q:** Distribuição de internações ortopédicas por raça/cor do paciente em SP em 2022

**SQL gerado:**
```sql
SELECT 
    raca_cor_paciente,
    COUNT(DISTINCT n_aih) AS num_internacoes
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH'
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    raca_cor_paciente;
```

**Resultado:** `Linha não encontrada: {'raca_cor_paciente': '01', 'total': 38121, 'pct': 57.8}`
**TTA:** 32435ms

---

### ❌ Q26 — Epidemiológica Complexa

**Q:** CIDs ortopédicos com maior número de dias totais de internação em SP em 2022

**SQL gerado:**
```sql
SELECT 
    cid_principal,
    SUM(qt_diarias) AS total_dias_internacao
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH'
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    cid_principal
ORDER BY 
    total_dias_internacao DESC;
```

**Resultado:** `Número de linhas: got=308 exp=10`
**TTA:** 41574ms

---

### ❌ Q27 — Epidemiológica Complexa

**Q:** Top 5 procedimentos ambulatoriais ortopédicos mais realizados em SP em 2022

**SQL gerado:**
```sql
SELECT 
    cod_procedimento,
    COUNT(*) AS quantidade_realizada
FROM 
    processed
WHERE 
    sistema = 'SIA' 
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
GROUP BY 
    cod_procedimento
ORDER BY 
    quantidade_realizada DESC
LIMIT 5;
```

**Resultado:** `Linha 1: got={'cod_procedimento': '0701050047', 'quantidade_realizada': 81645} exp={'cod_procedimento': '0302050019', 'total': 70536}`
**TTA:** 38293ms

---

### ⚠️ Q28 — Epidemiológica Complexa

**Q:** Proporção de internações ortopédicas com paciente de outro município em SP em 2022

**Erro:** HTTP 500: {"detail":"Falha na geração de SQL: Somente SELECT é permitido. SQL recebido começava com: WITH orthopedic_internations AS (\n    SELECT \n        p.n_aih,\n        e.clinica"}

---

### ❌ Q29 — Epidemiológica Complexa

**Q:** Comparativo de volume mensal SIA vs SIH ortopédico em SP em 2022

**SQL gerado:**
```sql
SELECT 
    mes_cmpt,
    SUM(CASE WHEN sistema = 'SIA' THEN 1 ELSE 0 END) AS volume_sia,
    SUM(CASE WHEN sistema = 'SIH' THEN 1 ELSE 0 END) AS volume_sih
FROM 
    processed
WHERE 
    uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    mes_cmpt
ORDER BY 
    mes_cmpt;
```

**Resultado:** `Número de linhas: got=12 exp=24`
**TTA:** 37457ms

---

### ❌ Q30 — Epidemiológica Complexa

**Q:** Idade média dos pacientes internados por fratura de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT AVG(idade_paciente) AS idade_media
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (cid_principal LIKE 'S72%' OR icd_group = 'S00-T98')
```

**Resultado:** `Linha 1: got={'idade_media': 51.991553686} exp={'idade_media': 62.4, 'idade_minima': 0, 'idade_maxima': 99}`
**TTA:** 31293ms

---

### ❌ Q31 — Financeira

**Q:** Custo total de todas as internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT SUM(COALESCE(custo_total, 0)) AS custo_total_internacoes_ortopedicas
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99');
```

**Resultado:** `got=13990374.039999995 exp=175283428.77`
**TTA:** 30766ms

---

### ❌ Q32 — Financeira

**Q:** Custo médio por internação ortopédica em SP em 2022

**SQL gerado:**
```sql
SELECT 
    AVG(COALESCE(custo_total, 0)) AS custo_medio_por_internacao
FROM 
    processed
WHERE 
    sistema = 'SIH'
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99');
```

**Resultado:** `got=2206.6836025237 exp=2655.37`
**TTA:** 28504ms

---

### ❌ Q33 — Financeira

**Q:** Custo total de internações por fratura de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT SUM(COALESCE(custo_total, 0)) AS custo_total_internacoes
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (cid_principal LIKE 'S72%' OR icd_group = 'S00-T98');
```

**Resultado:** `got=161293054.7299998 exp=79277316.96`
**TTA:** 32673ms

---

### ❌ Q34 — Financeira

**Q:** Top 5 CIDs ortopédicos com maior custo total de internação em SP em 2022

**SQL gerado:**
```sql
SELECT 
    cid_principal,
    SUM(custo_total) AS total_custo
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH' 
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    cid_principal
ORDER BY 
    total_custo DESC
LIMIT 5;
```

**Resultado:** `Linha 1: got={'cid_principal': 'M511', 'total_custo': 878799.22} exp={'cid_principal': 'S720', 'internacoes': 8980, 'custo_total': 29879588.9}`
**TTA:** 33292ms

---

### ❌ Q35 — Financeira

**Q:** Custo total de internações ortopédicas com uso de OPME (val_ortp > 0) em SP em 2022

**SQL gerado:**
```sql
SELECT SUM(COALESCE(val_ortp, 0)) AS custo_total_opme
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND val_ortp > 0;
```

**Resultado:** `Linha 1: got={'custo_total_opme': None} exp={'internacoes_com_opme': 0, 'custo_total_opme': None}`
**TTA:** 25633ms

---

### ❌ Q36 — Financeira

**Q:** Valor total de honorários profissionais (val_sp) em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT SUM(COALESCE(val_sp, 0)) AS total_honorarios_profissionais
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99');
```

**Resultado:** `got=2795569.4900000007 exp=30640001.52`
**TTA:** 29727ms

---

### ❌ Q37 — Financeira

**Q:** Custo médio por dia de internação ortopédica em SP em 2022

**SQL gerado:**
```sql
SELECT 
    COALESCE(SUM(custo_total) / SUM(qt_diarias), 0) AS custo_medio_por_dia
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH'
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99');
```

**Resultado:** `got=391.5581875175 exp=432.54`
**TTA:** 28879ms

---

### ❌ Q38 — Financeira

**Q:** Top 5 estabelecimentos com maior custo total de internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT 
    cod_munic_estabelecimento, 
    SUM(COALESCE(custo_total, 0)) AS total_custo
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH' 
    AND uf_origem = 'SP' 
    AND ano_cmpt = 2022 
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    cod_munic_estabelecimento
ORDER BY 
    total_custo DESC
LIMIT 5;
```

**Resultado:** `Linha 1: got={'cod_munic_estabelecimento': '350000', 'total_custo': 6704942.199999999} exp={'cnes_estabelecimento': '2078015', 'internacoes': 2221, 'custo_total': 11218869.54}`
**TTA:** 43399ms

---

### ❌ Q39 — Financeira

**Q:** Custo total de procedimentos ambulatoriais ortopédicos em SP em 2022

**SQL gerado:**
```sql
SELECT SUM(COALESCE(custo_total, 0)) AS custo_total_ortopedia
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99');
```

**Resultado:** `got=1945222.82000001 exp=8721217.91`
**TTA:** 34175ms

---

### ❌ Q40 — Financeira

**Q:** Proporção custo serviço hospitalar vs honorários em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT 
    COALESCE(SUM(val_sh), 0) AS total_val_sh,
    COALESCE(SUM(val_sp), 0) AS total_val_sp,
    (COALESCE(SUM(val_sh), 0) / NULLIF(COALESCE(SUM(val_sp), 0), 0)) AS proporcao_val_sh_vs_val_sp
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH'
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99');
```

**Resultado:** `Linha 1: got={'total_val_sh': 11194804.549999997, 'total_val_sp': 2795569.4899999993, 'proporcao_val_sh_vs_val_sp': 4.0044808723} exp={'custo_servico_hospitalar': 144636796.74, 'honorarios_profissionais': 30640001.52, 'pct_hospitalar': 82.5}`
**TTA:** 35570ms

---

### ❌ Q41 — Temporal/Comparativa

**Q:** Distribuição mensal de internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT 
    mes_cmpt,
    COUNT(DISTINCT n_aih) AS num_internacoes
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH'
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    mes_cmpt
ORDER BY 
    mes_cmpt;
```

**Resultado:** `Linha 1: got={'mes_cmpt': 1, 'num_internacoes': 476} exp={'mes_cmpt': 1, 'internacoes': 5209}`
**TTA:** 38616ms

---

### ❌ Q42 — Temporal/Comparativa

**Q:** Distribuição mensal de procedimentos ambulatoriais ortopédicos em SP em 2022

**SQL gerado:**
```sql
SELECT 
    mes_cmpt,
    COUNT(*) AS quantidade_procedimentos
FROM 
    processed
WHERE 
    sistema = 'SIA' 
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND cod_procedimento LIKE '0408%'
GROUP BY 
    mes_cmpt
ORDER BY 
    mes_cmpt;
```

**Resultado:** `None`
**TTA:** 24371ms

---

### ✅ Q43 — Temporal/Comparativa

**Q:** Mês com maior número de internações por fratura de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt, COUNT(DISTINCT n_aih) AS num_internacoes
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
GROUP BY mes_cmpt
ORDER BY num_internacoes DESC
LIMIT 1;
```

**Resultado:** `got=8 exp=8`
**TTA:** 25205ms

---

### ❌ Q44 — Temporal/Comparativa

**Q:** Evolução mensal do custo total de internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT 
    mes_cmpt,
    SUM(COALESCE(custo_total, 0)) AS custo_total_internacoes
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH'
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    mes_cmpt
ORDER BY 
    mes_cmpt;
```

**Resultado:** `Linha 1: got={'mes_cmpt': 1, 'custo_total_internacoes': 1015180.7699999993} exp={'mes_cmpt': 1, 'custo_total': 12942488.31}`
**TTA:** 42732ms

---

### ❌ Q45 — Temporal/Comparativa

**Q:** Volume mensal de internações por trauma (S00-T98) vs doenças musculoesqueléticas (M00-M99) em SP em 2022

**SQL gerado:**
```sql
SELECT 
    mes_cmpt,
    COUNT(DISTINCT CASE WHEN icd_group = 'S00-T98' THEN n_aih END) AS trauma_internacoes,
    COUNT(DISTINCT CASE WHEN icd_group = 'M00-M99' THEN n_aih END) AS musculoesqueletica_internacoes
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH'
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
GROUP BY 
    mes_cmpt
ORDER BY 
    mes_cmpt;
```

**Resultado:** `Número de linhas: got=12 exp=24`
**TTA:** 38439ms

---

### ❌ Q46 — Temporal/Comparativa

**Q:** Evolução mensal de óbitos em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT 
    mes_cmpt,
    COUNT(DISTINCT n_aih) AS total_obitos
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH'
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND morte = 1
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    mes_cmpt
ORDER BY 
    mes_cmpt;
```

**Resultado:** `Linha 1: got={'mes_cmpt': 1, 'total_obitos': 6} exp={'mes_cmpt': 1, 'total_obitos': 179}`
**TTA:** 36145ms

---

### ❌ Q47 — Temporal/Comparativa

**Q:** Permanência média por trimestre nas internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT 
    AVG(dias_perm) AS media_permanencia,
    competencia_ano_mes / 100 AS ano_trimestre
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH'
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    competencia_ano_mes / 100
ORDER BY 
    ano_trimestre;
```

**Resultado:** `Número de linhas: got=12 exp=4`
**TTA:** 35135ms

---

### ❌ Q48 — Temporal/Comparativa

**Q:** Mês com maior custo médio por internação ortopédica em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt, AVG(custo_total) AS media_custo_internacao
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY mes_cmpt
ORDER BY media_custo_internacao DESC
LIMIT 1;
```

**Resultado:** `got=12 exp=10`
**TTA:** 31541ms

---

### ✅ Q49 — Temporal/Comparativa

**Q:** Sazonalidade mensal de fraturas de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT 
    mes_cmpt,
    COUNT(*) AS quantidade_internacoes
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH'
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND cid_principal LIKE 'S72%'
GROUP BY 
    mes_cmpt
ORDER BY 
    mes_cmpt;
```

**Resultado:** `12 linhas corretas (flexível)`
**TTA:** 34620ms

---

### ❌ Q50 — Temporal/Comparativa

**Q:** Volume mensal de internações ortopédicas de idosos (60 anos ou mais) em SP em 2022

**SQL gerado:**
```sql
SELECT 
    mes_cmpt,
    COUNT(DISTINCT n_aih) AS volume_internacoes
FROM 
    processed
JOIN 
    enriched e ON processed.row_id = e.row_id
WHERE 
    sistema = 'SIH'
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND idade_paciente >= 60
    AND (cid_principal LIKE 'M%' OR icd_group = 'M00-M99')
GROUP BY 
    mes_cmpt
ORDER BY 
    mes_cmpt;
```

**Resultado:** `Linha 1: got={'mes_cmpt': 1, 'volume_internacoes': 118} exp={'mes_cmpt': 1, 'internacoes_idosos': 1987}`
**TTA:** 37413ms

---

## Failures Detail

- **Q02** (Epidemiológica Simples): got=10648 exp=105016
  SQL: `SELECT COUNT(*) AS total_procedimentos_ortopedicos
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE s...`
- **Q03** (Epidemiológica Simples): got=59668 exp=29356
  SQL: `SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE si...`
- **Q05** (Epidemiológica Simples): got=73 exp=2223
  SQL: `SELECT COUNT(DISTINCT n_aih) AS numero_obitos
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistem...`
- **Q08** (Epidemiológica Simples): got=2327 exp=25847
  SQL: `SELECT COUNT(DISTINCT n_aih) AS numero_internacoes
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE s...`
- **Q09** (Epidemiológica Simples): got=3975 exp=40123
  SQL: `SELECT COUNT(DISTINCT n_aih) AS numero_internacoes
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE s...`
- **Q10** (Epidemiológica Simples): got=1685 exp=26666
  SQL: `SELECT COUNT(DISTINCT n_aih)
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = 'SIH'
  AND u...`
- **Q11** (Epidemiológica Simples): got=496 exp=211
  SQL: `SELECT COUNT(DISTINCT cod_munic_residencia) AS num_municipios_distintos
FROM processed
JOIN enriched e ON processed.row_...`
- **Q12** (Epidemiológica Simples): got=158 exp=381
  SQL: `SELECT COUNT(DISTINCT cod_munic_estabelecimento) AS num_estabelecimentos_distintos
FROM processed
WHERE sistema = 'SIH'
...`
- **Q13** (Epidemiológica Simples): got=1383 exp=16451
  SQL: `SELECT COUNT(DISTINCT n_aih) AS numero_internacoes
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE s...`
- **Q14** (Epidemiológica Simples): got=0 exp=3
  SQL: `SELECT COUNT(DISTINCT n_aih) AS numero_internacoes_uti
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHE...`
- **Q15** (Epidemiológica Simples): got=17608 exp=15119
  SQL: `SELECT COUNT(DISTINCT n_aih) AS numero_internacoes
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE s...`
- **Q16** (Epidemiológica Complexa): Linha 1: got={'cid_principal': 'M861', 'num_internacoes': 912} exp={'cid_principal': 'S720', 'total': 8980}
  SQL: `SELECT 
    cid_principal,
    COUNT(DISTINCT n_aih) AS num_internacoes
FROM 
    processed
JOIN 
    enriched e ON proc...`
- **Q17** (Epidemiológica Complexa): Linha 1: got={'cod_munic_residencia': '355030', 'num_internacoes': 1190} exp={'cod_munic_estabelecimento': '350000', 'total': 29824}
  SQL: `SELECT 
    cod_munic_residencia,
    COUNT(DISTINCT n_aih) AS num_internacoes
FROM 
    processed
JOIN 
    enriched e ...`
- **Q18** (Epidemiológica Complexa): Linha não encontrada: {'faixa_etaria': '0-17', 'total': 3619}
  SQL: `SELECT 
    CASE 
        WHEN idade_paciente BETWEEN 0 AND 17 THEN '0-17'
        WHEN idade_paciente BETWEEN 18 AND 59...`
- **Q19** (Epidemiológica Complexa): Linha 1: got={'cid_principal': 'M895', 'total_internacoes': 2, 'total_obitos': 1.0, 'taxa_mortalidade': 50.0} exp={'cid_principal': 'S062', 'total_internacoes': 70, 'total_obitos': 21, 'taxa_mortalidade_pct': 30.0}
  SQL: `SELECT 
    cid_principal,
    COUNT(DISTINCT n_aih) AS total_internacoes,
    SUM(morte) AS total_obitos,
    (SUM(mort...`
- **Q20** (Epidemiológica Complexa): Linha 1: got={'cid_principal': 'M216', 'volume_producao': 4743} exp={'cid_principal': 'S720', 'total': 16466}
  SQL: `SELECT 
    cid_principal,
    COUNT(*) AS volume_producao
FROM 
    processed
JOIN 
    enriched e ON processed.row_id ...`
- **Q21** (Epidemiológica Complexa): Falha na execução do SQL: Parser Error: syntax error at or near "WITH"

LINE 16: WITH ROLLUP;
         ^
  SQL: `SELECT 
    sexo_paciente,
    COUNT(DISTINCT n_aih) AS quantidade_internacoes,
    (COUNT(DISTINCT n_aih) * 1.0 / total...`
- **Q22** (Epidemiológica Complexa): HTTP 500: {"detail":"Falha na geração de SQL: Somente SELECT é permitido. SQL recebido começava com: WITH filtered_data AS (\n    SELECT \n        p.n_aih,\n        COUNT(DISTINCT p.n_"}
- **Q23** (Epidemiológica Complexa): Linha 1: got={'cid_principal': 'M318', 'media_permanencia': 36.0} exp={'cid_principal': 'S063', 'total_internacoes': 78, 'permanencia_media_dias': 16.4}
  SQL: `SELECT 
    cid_principal,
    AVG(dias_perm) AS media_permanencia
FROM 
    processed
JOIN 
    enriched e ON processed...`
- **Q24** (Epidemiológica Complexa): Linha 1: got={'cod_munic_estabelecimento': '350000', 'num_internacoes': 2800} exp={'cnes_estabelecimento': '2078015', 'total_internacoes': 2221}
  SQL: `SELECT 
    cod_munic_estabelecimento, 
    COUNT(DISTINCT n_aih) AS num_internacoes
FROM 
    processed
WHERE 
    sist...`
- **Q25** (Epidemiológica Complexa): Linha não encontrada: {'raca_cor_paciente': '01', 'total': 38121, 'pct': 57.8}
  SQL: `SELECT 
    raca_cor_paciente,
    COUNT(DISTINCT n_aih) AS num_internacoes
FROM 
    processed
JOIN 
    enriched e ON ...`
- **Q26** (Epidemiológica Complexa): Número de linhas: got=308 exp=10
  SQL: `SELECT 
    cid_principal,
    SUM(qt_diarias) AS total_dias_internacao
FROM 
    processed
JOIN 
    enriched e ON proc...`
- **Q27** (Epidemiológica Complexa): Linha 1: got={'cod_procedimento': '0701050047', 'quantidade_realizada': 81645} exp={'cod_procedimento': '0302050019', 'total': 70536}
  SQL: `SELECT 
    cod_procedimento,
    COUNT(*) AS quantidade_realizada
FROM 
    processed
WHERE 
    sistema = 'SIA' 
    A...`
- **Q28** (Epidemiológica Complexa): HTTP 500: {"detail":"Falha na geração de SQL: Somente SELECT é permitido. SQL recebido começava com: WITH orthopedic_internations AS (\n    SELECT \n        p.n_aih,\n        e.clinica"}
- **Q29** (Epidemiológica Complexa): Número de linhas: got=12 exp=24
  SQL: `SELECT 
    mes_cmpt,
    SUM(CASE WHEN sistema = 'SIA' THEN 1 ELSE 0 END) AS volume_sia,
    SUM(CASE WHEN sistema = 'S...`
- **Q30** (Epidemiológica Complexa): Linha 1: got={'idade_media': 51.991553686} exp={'idade_media': 62.4, 'idade_minima': 0, 'idade_maxima': 99}
  SQL: `SELECT AVG(idade_paciente) AS idade_media
FROM processed
JOIN enriched e ON processed.row_id = e.row_id
WHERE sistema = ...`
- **Q31** (Financeira): got=13990374.039999995 exp=175283428.77
  SQL: `SELECT SUM(COALESCE(custo_total, 0)) AS custo_total_internacoes_ortopedicas
FROM processed
WHERE sistema = 'SIH'
  AND u...`
- **Q32** (Financeira): got=2206.6836025237 exp=2655.37
  SQL: `SELECT 
    AVG(COALESCE(custo_total, 0)) AS custo_medio_por_internacao
FROM 
    processed
WHERE 
    sistema = 'SIH'
 ...`
- **Q33** (Financeira): got=161293054.7299998 exp=79277316.96
  SQL: `SELECT SUM(COALESCE(custo_total, 0)) AS custo_total_internacoes
FROM processed
JOIN enriched e ON processed.row_id = e.r...`
- **Q34** (Financeira): Linha 1: got={'cid_principal': 'M511', 'total_custo': 878799.22} exp={'cid_principal': 'S720', 'internacoes': 8980, 'custo_total': 29879588.9}
  SQL: `SELECT 
    cid_principal,
    SUM(custo_total) AS total_custo
FROM 
    processed
JOIN 
    enriched e ON processed.row...`
- **Q35** (Financeira): Linha 1: got={'custo_total_opme': None} exp={'internacoes_com_opme': 0, 'custo_total_opme': None}
  SQL: `SELECT SUM(COALESCE(val_ortp, 0)) AS custo_total_opme
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ...`
- **Q36** (Financeira): got=2795569.4900000007 exp=30640001.52
  SQL: `SELECT SUM(COALESCE(val_sp, 0)) AS total_honorarios_profissionais
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem =...`
- **Q37** (Financeira): got=391.5581875175 exp=432.54
  SQL: `SELECT 
    COALESCE(SUM(custo_total) / SUM(qt_diarias), 0) AS custo_medio_por_dia
FROM 
    processed
JOIN 
    enriche...`
- **Q38** (Financeira): Linha 1: got={'cod_munic_estabelecimento': '350000', 'total_custo': 6704942.199999999} exp={'cnes_estabelecimento': '2078015', 'internacoes': 2221, 'custo_total': 11218869.54}
  SQL: `SELECT 
    cod_munic_estabelecimento, 
    SUM(COALESCE(custo_total, 0)) AS total_custo
FROM 
    processed
JOIN 
    e...`
- **Q39** (Financeira): got=1945222.82000001 exp=8721217.91
  SQL: `SELECT SUM(COALESCE(custo_total, 0)) AS custo_total_ortopedia
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP...`
- **Q40** (Financeira): Linha 1: got={'total_val_sh': 11194804.549999997, 'total_val_sp': 2795569.4899999993, 'proporcao_val_sh_vs_val_sp': 4.0044808723} exp={'custo_servico_hospitalar': 144636796.74, 'honorarios_profissionais': 30640001.52, 'pct_hospitalar': 82.5}
  SQL: `SELECT 
    COALESCE(SUM(val_sh), 0) AS total_val_sh,
    COALESCE(SUM(val_sp), 0) AS total_val_sp,
    (COALESCE(SUM(va...`
- **Q41** (Temporal/Comparativa): Linha 1: got={'mes_cmpt': 1, 'num_internacoes': 476} exp={'mes_cmpt': 1, 'internacoes': 5209}
  SQL: `SELECT 
    mes_cmpt,
    COUNT(DISTINCT n_aih) AS num_internacoes
FROM 
    processed
JOIN 
    enriched e ON processed...`
- **Q42** (Temporal/Comparativa): None
  SQL: `SELECT 
    mes_cmpt,
    COUNT(*) AS quantidade_procedimentos
FROM 
    processed
WHERE 
    sistema = 'SIA' 
    AND u...`
- **Q44** (Temporal/Comparativa): Linha 1: got={'mes_cmpt': 1, 'custo_total_internacoes': 1015180.7699999993} exp={'mes_cmpt': 1, 'custo_total': 12942488.31}
  SQL: `SELECT 
    mes_cmpt,
    SUM(COALESCE(custo_total, 0)) AS custo_total_internacoes
FROM 
    processed
JOIN 
    enriche...`
- **Q45** (Temporal/Comparativa): Número de linhas: got=12 exp=24
  SQL: `SELECT 
    mes_cmpt,
    COUNT(DISTINCT CASE WHEN icd_group = 'S00-T98' THEN n_aih END) AS trauma_internacoes,
    COUN...`
- **Q46** (Temporal/Comparativa): Linha 1: got={'mes_cmpt': 1, 'total_obitos': 6} exp={'mes_cmpt': 1, 'total_obitos': 179}
  SQL: `SELECT 
    mes_cmpt,
    COUNT(DISTINCT n_aih) AS total_obitos
FROM 
    processed
JOIN 
    enriched e ON processed.ro...`
- **Q47** (Temporal/Comparativa): Número de linhas: got=12 exp=4
  SQL: `SELECT 
    AVG(dias_perm) AS media_permanencia,
    competencia_ano_mes / 100 AS ano_trimestre
FROM 
    processed
JOIN...`
- **Q48** (Temporal/Comparativa): got=12 exp=10
  SQL: `SELECT mes_cmpt, AVG(custo_total) AS media_custo_internacao
FROM processed
JOIN enriched e ON processed.row_id = e.row_i...`
- **Q50** (Temporal/Comparativa): Linha 1: got={'mes_cmpt': 1, 'volume_internacoes': 118} exp={'mes_cmpt': 1, 'internacoes_idosos': 1987}
  SQL: `SELECT 
    mes_cmpt,
    COUNT(DISTINCT n_aih) AS volume_internacoes
FROM 
    processed
JOIN 
    enriched e ON proces...`

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_