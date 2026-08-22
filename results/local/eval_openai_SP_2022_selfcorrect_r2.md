# Benchmark Evaluation Report — SUS Data RAG

**Model:** openai  
**Run date:** 2026-07-13 04:06  
**API:** http://127.0.0.1:8000  

## Summary

| Metric | Value |
|---|---|
| Total queries | 118 |
| Correct (EA numerator) | 90 |
| **Execution Accuracy (EA)** | **76.3%** |
| Wilson IC 95% | [67.8%, 83.0%] |
| Correct via flexible match (alias diferente) | 57 |
| API errors / timeouts | 0 |
| Mean Time-to-Answer | 11.8s |

> **Nota metodológica — Scoring:** A Execution Accuracy avalia se o SQL gerado
> retorna os mesmos **valores** que o gold-standard, independente do nome das colunas.
> Quando o LLM usa um alias diferente mas retorna os dados corretos (match posicional),
> a query é contada como correta e sinalizada com `~`. Esta decisão segue a definição
> de EA em [Lee et al., 2022] — comparação de conjuntos de resultado, não de SQL texto.

## EA by Category

| Category | Correct | Total | EA |
|---|---:|---:|---:|
| Epidemiológica Simples | 33 | 33 | 100.0% |
| Epidemiológica Complexa | 15 | 31 | 48.4% |
| Financeira | 19 | 26 | 73.1% |
| Temporal/Comparativa | 23 | 28 | 82.1% |

## Query Results

### ✅ Q01 — Epidemiológica Simples

**Q:** Total de internações ortopédicas (M00-M99 e S00-S99) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=65970 exp=65970`
**TTA:** 15746ms

---

### ✅ Q02 — Epidemiológica Simples

**Q:** Total de procedimentos ambulatoriais ortopédicos em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(*) AS total_procedimentos
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=105016 exp=105016`
**TTA:** 6551ms

---

### ✅ Q03 — Epidemiológica Simples

**Q:** Total de internações por fratura de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
```

**Resultado:** `got=29356 exp=29356`
**TTA:** 14023ms

---

### ✅ Q04 — Epidemiológica Simples

**Q:** Total de internações por osteoartrose (M16, M17) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (cid_principal LIKE 'M16%' OR cid_principal LIKE 'M17%')
```

**Resultado:** `got=156 exp=156`
**TTA:** 8940ms

---

### ✅ Q05 — Epidemiológica Simples

**Q:** Número de óbitos em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_obitos
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND morte = 1;
```

**Resultado:** `got=2223 exp=2223`
**TTA:** 11566ms

---

### ✅ Q06 — Epidemiológica Simples

**Q:** Total de internações por traumatismos (S00-T98) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND icd_group = 'S00-T98';
```

**Resultado:** `got=59668 exp=59668`
**TTA:** 11625ms

---

### ✅ Q07 — Epidemiológica Simples

**Q:** Total de internações por doenças osteomusculares (M00-M99) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND icd_group = 'M00-M99';
```

**Resultado:** `got=6302 exp=6302`
**TTA:** 11733ms

---

### ✅ Q08 — Epidemiológica Simples

**Q:** Número de mulheres internadas por causa ortopédica em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND sexo_paciente = 'F'
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=25847 exp=25847`
**TTA:** 11779ms

---

### ✅ Q09 — Epidemiológica Simples

**Q:** Número de homens internados por causa ortopédica em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_homens_internados
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND sexo_paciente = 'M'
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=40123 exp=40123`
**TTA:** 12314ms

---

### ✅ Q10 — Epidemiológica Simples

**Q:** Número de internações ortopédicas de idosos (60 anos ou mais) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND idade_paciente >= 60
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=26666 exp=26666`
**TTA:** 11178ms

---

### ✅ Q11 — Epidemiológica Simples

**Q:** Número de municípios distintos com internação ortopédica registrada em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT cod_munic_estabelecimento) AS num_municipios
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=211 exp=211`
**TTA:** 11555ms

---

### ✅ Q12 — Epidemiológica Simples

**Q:** Número de estabelecimentos distintos com internação ortopédica em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT cnes_estabelecimento) AS num_estabelecimentos
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=381 exp=381`
**TTA:** 11515ms

---

### ✅ Q13 — Epidemiológica Simples

**Q:** Número de internações ortopédicas com permanência superior a 7 dias em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND dias_perm > 7;
```

**Resultado:** `got=16451 exp=16451`
**TTA:** 12009ms

---

### ✅ Q14 — Epidemiológica Simples

**Q:** Número de internações ortopédicas com uso de UTI em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes_uti
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND uti_int_to > 0;
```

**Resultado:** `got=3 exp=3`
**TTA:** 11552ms

---

### ✅ Q15 — Epidemiológica Simples

**Q:** Número de internações por fratura de quadril (S72) em pacientes com 70 anos ou mais em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
  AND idade_paciente >= 70;
```

**Resultado:** `got=15119 exp=15119`
**TTA:** 11934ms

---

### ✅ Q16 — Epidemiológica Complexa

**Q:** Top 10 CIDs ortopédicos por volume de internação em SP em 2022

**SQL gerado:**
```sql
SELECT cid_principal,
  COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cid_principal
ORDER BY total_internacoes DESC
LIMIT 10;
```

**Resultado:** `10 linhas corretas (flexível)`
**TTA:** 11631ms

---

### ✅ Q17 — Epidemiológica Complexa

**Q:** Top 5 municípios de SP com maior número de internações ortopédicas em 2022

**SQL gerado:**
```sql
SELECT cod_munic_estabelecimento,
  COUNT(DISTINCT n_aih) AS internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cod_munic_estabelecimento
ORDER BY internacoes DESC
LIMIT 5;
```

**Resultado:** `5 linhas corretas (flexível)`
**TTA:** 13152ms

---

### ❌ Q18 — Epidemiológica Complexa

**Q:** Distribuição de internações ortopédicas por faixa etária (0-17, 18-59, 60+) em SP em 2022

**SQL gerado:**
```sql
SELECT 
  CASE
    WHEN idade_paciente < 18 THEN '0-17'
    WHEN idade_paciente BETWEEN 18 AND 59 THEN '18-59'
    ELSE '60+'
  END AS faixa_etaria,
  COUNT(DISTINCT n_aih) AS total_internacoes,
  ROUND(100.0 * COUNT(DISTINCT n_aih) / SUM(COUNT(DISTINCT n_aih)) OVER (), 1) AS pct
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY faixa_etaria
ORDER BY faixa_etaria;
```

**Resultado:** `Linha não encontrada: {'faixa_etaria': '0-17', 'total': 3619}`
**TTA:** 10782ms

---

### ❌ Q19 — Epidemiológica Complexa

**Q:** Taxa de mortalidade hospitalar por CID ortopédico (top 5) em SP em 2022

**SQL gerado:**
```sql
SELECT cid_principal,
  ROUND(100.0 * SUM(morte) / COUNT(DISTINCT n_aih), 2) AS taxa_mortalidade
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cid_principal
ORDER BY taxa_mortalidade DESC
LIMIT 5;
```

**Resultado:** `Linha 1: got={'cid_principal': 'S279', 'taxa_mortalidade': 100.0} exp={'cid_principal': 'S062', 'total_internacoes': 70, 'total_obitos': 21.0, 'taxa_mortalidade_pct': 30.0}`
**TTA:** 11969ms

---

### ✅ Q20 — Epidemiológica Complexa

**Q:** Top 10 CIDs ambulatoriais ortopédicos por volume de produção em SP em 2022

**SQL gerado:**
```sql
SELECT cid_principal,
  COUNT(*) AS total_procedimentos
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cid_principal
ORDER BY total_procedimentos DESC
LIMIT 10;
```

**Resultado:** `10 linhas corretas (flexível)`
**TTA:** 11786ms

---

### ✅ Q21 — Epidemiológica Complexa

**Q:** Proporção de internações ortopédicas por sexo em SP em 2022

**SQL gerado:**
```sql
SELECT sexo_paciente,
  COUNT(DISTINCT n_aih) AS total_internacoes,
  ROUND(100.0 * COUNT(DISTINCT n_aih) / SUM(COUNT(DISTINCT n_aih)) OVER (), 1) AS pct
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY sexo_paciente
ORDER BY total_internacoes DESC;
```

**Resultado:** `2 linhas corretas, ordem livre (flexível)`
**TTA:** 12096ms

---

### ❌ Q22 — Epidemiológica Complexa

**Q:** Número de internações por fratura de fêmur (S72) em idosos (60+) comparado ao total em SP em 2022

**SQL gerado:**
```sql
SELECT
  COUNT(DISTINCT CASE WHEN idade_paciente >= 60 THEN n_aih END) AS internacoes_idosos,
  COUNT(DISTINCT n_aih) AS internacoes_totais
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
```

**Resultado:** `Linha 1: got={'internacoes_idosos': 19117, 'internacoes_totais': 29356} exp={'total_s72': 29356, 's72_idosos': 19120.0, 'pct_idosos': 65.1}`
**TTA:** 11741ms

---

### ❌ Q23 — Epidemiológica Complexa

**Q:** Top 5 CIDs ortopédicos com maior permanência média hospitalar em SP em 2022

**SQL gerado:**
```sql
SELECT cid_principal,
  ROUND(AVG(dias_perm), 1) AS permanencia_media_dias
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cid_principal
ORDER BY permanencia_media_dias DESC
LIMIT 5;
```

**Resultado:** `Linha 1: got={'cid_principal': 'T918', 'permanencia_media_dias': 68.0} exp={'cid_principal': 'S063', 'total_internacoes': 78, 'permanencia_media_dias': 16.4}`
**TTA:** 12289ms

---

### ✅ Q24 — Epidemiológica Complexa

**Q:** Top 5 estabelecimentos com maior volume de internação ortopédica em SP em 2022

**SQL gerado:**
```sql
SELECT cnes_estabelecimento,
  COUNT(DISTINCT n_aih) AS internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cnes_estabelecimento
ORDER BY internacoes DESC
LIMIT 5;
```

**Resultado:** `5 linhas corretas (flexível)`
**TTA:** 11554ms

---

### ✅ Q25 — Epidemiológica Complexa

**Q:** Distribuição de internações ortopédicas por raça/cor do paciente em SP em 2022

**SQL gerado:**
```sql
SELECT raca_cor_paciente,
  COUNT(DISTINCT n_aih) AS total,
  ROUND(100.0 * COUNT(DISTINCT n_aih) / SUM(COUNT(DISTINCT n_aih)) OVER (), 1) AS pct
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY raca_cor_paciente
ORDER BY total DESC;
```

**Resultado:** `6 linhas corretas, ordem livre (exato)`
**TTA:** 12325ms

---

### ❌ Q26 — Epidemiológica Complexa

**Q:** CIDs ortopédicos com maior número de dias totais de internação em SP em 2022

**SQL gerado:**
```sql
SELECT cid_principal,
  SUM(dias_perm) AS total_dias_internacao
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cid_principal
ORDER BY total_dias_internacao DESC
```

**Resultado:** `Número de linhas: got=1015 exp=10`
**TTA:** 11370ms

---

### ✅ Q27 — Epidemiológica Complexa

**Q:** Top 5 procedimentos ambulatoriais ortopédicos mais realizados em SP em 2022

**SQL gerado:**
```sql
SELECT cod_procedimento,
  COUNT(*) AS total_procedimentos
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cod_procedimento
ORDER BY total_procedimentos DESC
LIMIT 5;
```

**Resultado:** `5 linhas corretas (flexível)`
**TTA:** 12229ms

---

### ❌ Q28 — Epidemiológica Complexa

**Q:** Proporção de internações ortopédicas com paciente de outro município em SP em 2022

**SQL gerado:**
```sql
SELECT 
  CASE 
    WHEN e.clinical_deslocamento_territorial = 'deslocamento intermunicipal' THEN 'Outro Município'
    ELSE 'Mesmo Município'
  END AS tipo_deslocamento,
  COUNT(DISTINCT p.n_aih) AS total_internacoes,
  ROUND(100.0 * COUNT(DISTINCT p.n_aih) / SUM(COUNT(DISTINCT p.n_aih)) OVER (), 1) AS pct
FROM processed p
JOIN enriched e ON p.row_id = e.row_id
WHERE p.sistema = 'SIH'
  AND p.uf_origem = 'SP'
  AND p.ano_cmpt = 2022
  AND (p.icd_group = 'M00-M99' OR p.icd_group = 'S00-T98')
GROUP BY tipo_deslocamento
ORDER BY tipo_deslocamento;
```

**Resultado:** `Número de linhas: got=2 exp=1`
**TTA:** 12397ms

---

### ❌ Q29 — Epidemiológica Complexa

**Q:** Comparativo de volume mensal SIA vs SIH ortopédico em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  SUM(CASE WHEN sistema = 'SIA' THEN 1 ELSE 0 END) AS sia_volume,
  SUM(CASE WHEN sistema = 'SIH' THEN 1 ELSE 0 END) AS sih_volume
FROM processed
WHERE uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY mes_cmpt;
```

**Resultado:** `Número de linhas: got=12 exp=24`
**TTA:** 11459ms

---

### ❌ Q30 — Epidemiológica Complexa

**Q:** Idade média dos pacientes internados por fratura de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(AVG(idade_paciente), 1) AS idade_media
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
```

**Resultado:** `Linha 1: got={'idade_media': 62.4} exp={'idade_media': 62.4, 'idade_minima': 0.0, 'idade_maxima': 99.0}`
**TTA:** 11820ms

---

### ✅ Q31 — Financeira

**Q:** Custo total de todas as internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=175283428.77 exp=175283428.77`
**TTA:** 11788ms

---

### ✅ Q32 — Financeira

**Q:** Custo médio por internação ortopédica em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(AVG(COALESCE(custo_total, 0)), 2) AS custo_medio
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=2655.37 exp=2655.37`
**TTA:** 11683ms

---

### ✅ Q33 — Financeira

**Q:** Custo total de internações por fratura de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%';
```

**Resultado:** `got=79277316.96 exp=79277316.96`
**TTA:** 11664ms

---

### ❌ Q34 — Financeira

**Q:** Top 5 CIDs ortopédicos com maior custo total de internação em SP em 2022

**SQL gerado:**
```sql
SELECT cid_principal,
  ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cid_principal
ORDER BY custo_total DESC
LIMIT 5;
```

**Resultado:** `Linha 1: got={'cid_principal': 'S720', 'custo_total': 29879588.9} exp={'cid_principal': 'S720', 'internacoes': 8980, 'custo_total': 29879588.9}`
**TTA:** 11868ms

---

### ❌ Q35 — Financeira

**Q:** Custo total de internações ortopédicas com uso de OPME (val_ortp > 0) em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND val_ortp > 0;
```

**Resultado:** `Linha 1: got={'custo_total': None} exp={'internacoes_com_opme': 0, 'custo_total_opme': None}`
**TTA:** 11733ms

---

### ✅ Q36 — Financeira

**Q:** Valor total de honorários profissionais (val_sp) em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(SUM(COALESCE(val_sp, 0)), 2) AS total_honorarios_profissionais
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=30640001.52 exp=30640001.52`
**TTA:** 11607ms

---

### ✅ Q37 — Financeira

**Q:** Custo médio por dia de internação ortopédica em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(SUM(COALESCE(custo_total, 0)) / NULLIF(SUM(dias_perm), 0), 2) AS custo_medio_por_dia
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=441.95 exp=441.95`
**TTA:** 11714ms

---

### ❌ Q38 — Financeira

**Q:** Top 5 estabelecimentos com maior custo total de internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT cnes_estabelecimento,
  ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cnes_estabelecimento
ORDER BY custo_total DESC
LIMIT 5;
```

**Resultado:** `Linha 1: got={'cnes_estabelecimento': '2078015', 'custo_total': 11218869.54} exp={'cnes_estabelecimento': '2078015', 'internacoes': 2221, 'custo_total': 11218869.54}`
**TTA:** 12043ms

---

### ✅ Q39 — Financeira

**Q:** Custo total de procedimentos ambulatoriais ortopédicos em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=8721217.91 exp=8721217.91`
**TTA:** 11638ms

---

### ❌ Q40 — Financeira

**Q:** Proporção custo serviço hospitalar vs honorários em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT
  ROUND(SUM(COALESCE(val_sh, 0)), 2) AS total_val_sh,
  ROUND(SUM(COALESCE(val_sp, 0)), 2) AS total_val_sp,
  ROUND(100.0 * SUM(COALESCE(val_sh, 0)) / NULLIF(SUM(COALESCE(val_sh, 0)) + SUM(COALESCE(val_sp, 0)), 0), 1) AS pct_val_sh,
  ROUND(100.0 * SUM(COALESCE(val_sp, 0)) / NULLIF(SUM(COALESCE(val_sh, 0)) + SUM(COALESCE(val_sp, 0)), 0), 1) AS pct_val_sp
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `Linha 1: got={'total_val_sh': 144636796.74, 'total_val_sp': 30640001.52, 'pct_val_sh': 82.5, 'pct_val_sp': 17.5} exp={'custo_servico_hospitalar': 144636796.74, 'honorarios_profissionais': 30640001.52, 'pct_hospitalar': 82.5}`
**TTA:** 12271ms

---

### ✅ Q41 — Temporal/Comparativa

**Q:** Distribuição mensal de internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY mes_cmpt;
```

**Resultado:** `12 linhas corretas (flexível)`
**TTA:** 11978ms

---

### ❌ Q42 — Temporal/Comparativa

**Q:** Distribuição mensal de procedimentos ambulatoriais ortopédicos em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  COUNT(*) AS total_procedimentos,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY mes_cmpt;
```

**Resultado:** `Linha 1: got={'mes_cmpt': 1, 'total_procedimentos': 5898, 'pct': 5.6} exp={'mes_cmpt': 1, 'procedimentos': 5898}`
**TTA:** 11787ms

---

### ✅ Q43 — Temporal/Comparativa

**Q:** Mês com maior número de internações por fratura de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
GROUP BY mes_cmpt
ORDER BY total_internacoes DESC
LIMIT 1;
```

**Resultado:** `got=8 exp=8`
**TTA:** 12057ms

---

### ✅ Q44 — Temporal/Comparativa

**Q:** Evolução mensal do custo total de internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY mes_cmpt;
```

**Resultado:** `12 linhas corretas (exato)`
**TTA:** 11759ms

---

### ❌ Q45 — Temporal/Comparativa

**Q:** Volume mensal de internações por trauma (S00-T98) vs doenças musculoesqueléticas (M00-M99) em SP em 2022

**SQL gerado:**
```sql
SELECT 
  mes_cmpt,
  SUM(CASE WHEN icd_group = 'S00-T98' THEN 1 ELSE 0 END) AS internacoes_trauma,
  SUM(CASE WHEN icd_group = 'M00-M99' THEN 1 ELSE 0 END) AS internacoes_musculoesqueleticas
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY mes_cmpt;
```

**Resultado:** `Número de linhas: got=12 exp=24`
**TTA:** 11956ms

---

### ✅ Q46 — Temporal/Comparativa

**Q:** Evolução mensal de óbitos em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  COUNT(*) AS total_obitos
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND morte = 1
GROUP BY mes_cmpt
ORDER BY mes_cmpt;
```

**Resultado:** `12 linhas corretas (exato)`
**TTA:** 12606ms

---

### ✅ Q47 — Temporal/Comparativa

**Q:** Permanência média por trimestre nas internações ortopédicas em SP em 2022

**SQL gerado:**
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
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY trimestre
ORDER BY trimestre;
```

**Resultado:** `4 linhas corretas, ordem livre (exato)`
**TTA:** 11436ms

---

### ✅ Q48 — Temporal/Comparativa

**Q:** Mês com maior custo médio por internação ortopédica em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  ROUND(SUM(COALESCE(custo_total, 0)) / COUNT(DISTINCT n_aih), 2) AS custo_medio
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY custo_medio DESC
LIMIT 1;
```

**Resultado:** `got=10 exp=10`
**TTA:** 11461ms

---

### ✅ Q49 — Temporal/Comparativa

**Q:** Sazonalidade mensal de fraturas de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
GROUP BY mes_cmpt
ORDER BY mes_cmpt;
```

**Resultado:** `12 linhas corretas (flexível)`
**TTA:** 12083ms

---

### ✅ Q50 — Temporal/Comparativa

**Q:** Volume mensal de internações ortopédicas de idosos (60 anos ou mais) em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND idade_paciente >= 60
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY mes_cmpt;
```

**Resultado:** `12 linhas corretas (flexível)`
**TTA:** 14210ms

---

### ✅ Q51 — Epidemiológica Simples

**Q:** Número de internações por fratura de fêmur (S72) em mulheres em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
  AND sexo_paciente = 'F';
```

**Resultado:** `got=15359 exp=15359`
**TTA:** 9416ms

---

### ✅ Q52 — Epidemiológica Simples

**Q:** Número de internações por fratura de fêmur (S72) em homens em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
  AND sexo_paciente = 'M';
```

**Resultado:** `got=13997 exp=13997`
**TTA:** 11598ms

---

### ✅ Q53 — Epidemiológica Simples

**Q:** Número de internações ortopédicas com permanência de 1 dia ou menos em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND dias_perm <= 1;
```

**Resultado:** `got=13928 exp=13928`
**TTA:** 12259ms

---

### ✅ Q54 — Epidemiológica Simples

**Q:** Número de procedimentos ambulatoriais ortopédicos realizados em mulheres em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(*) AS total_procedimentos
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND sexo_paciente = 'F'
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=45579 exp=45579`
**TTA:** 11109ms

---

### ✅ Q55 — Epidemiológica Simples

**Q:** Número de procedimentos ambulatoriais ortopédicos realizados por idosos (60 anos ou mais) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(*) AS total_procedimentos
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND idade_paciente >= 60
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=53666 exp=53666`
**TTA:** 12088ms

---

### ✅ Q56 — Epidemiológica Simples

**Q:** Número de internações por fratura de tíbia e perônio (S82) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S82%';
```

**Resultado:** `got=6261 exp=6261`
**TTA:** 11450ms

---

### ✅ Q57 — Epidemiológica Simples

**Q:** Número de internações por fratura de antebraço (S52) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S52%';
```

**Resultado:** `got=1970 exp=1970`
**TTA:** 11434ms

---

### ✅ Q58 — Epidemiológica Simples

**Q:** Número de internações por fratura de ombro e úmero (S42) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S42%';
```

**Resultado:** `got=1248 exp=1248`
**TTA:** 11697ms

---

### ✅ Q59 — Epidemiológica Simples

**Q:** Número de internações ortopédicas em crianças (menores de 18 anos) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND idade_paciente < 18
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=3619 exp=3619`
**TTA:** 11753ms

---

### ✅ Q60 — Epidemiológica Simples

**Q:** Número de internações ortopédicas com permanência entre 8 e 30 dias em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND dias_perm BETWEEN 8 AND 30;
```

**Resultado:** `got=15489 exp=15489`
**TTA:** 11667ms

---

### ✅ Q61 — Epidemiológica Simples

**Q:** Número de internações por artrose de quadril e joelho (M16 e M17) em idosos (60 anos ou mais) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (cid_principal LIKE 'M16%' OR cid_principal LIKE 'M17%')
  AND idade_paciente >= 60;
```

**Resultado:** `got=89 exp=89`
**TTA:** 11996ms

---

### ✅ Q62 — Epidemiológica Simples

**Q:** Número de internações por fratura de fêmur (S72) em mulheres com 70 anos ou mais em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
  AND sexo_paciente = 'F'
  AND idade_paciente >= 70
```

**Resultado:** `got=10944 exp=10944`
**TTA:** 11658ms

---

### ✅ Q63 — Epidemiológica Simples

**Q:** Número de internações ortopédicas com permanência superior a 30 dias em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND dias_perm > 30;
```

**Resultado:** `got=975 exp=975`
**TTA:** 11629ms

---

### ✅ Q64 — Epidemiológica Simples

**Q:** Número de procedimentos ambulatoriais ortopédicos realizados em homens em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(*) AS total_procedimentos
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND sexo_paciente = 'M'
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=59437 exp=59437`
**TTA:** 11629ms

---

### ✅ Q65 — Epidemiológica Simples

**Q:** Número de internações por traumatismos (grupo S00-T98) em idosos (60 anos ou mais) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND icd_group = 'S00-T98'
  AND idade_paciente >= 60
```

**Resultado:** `got=24981 exp=24981`
**TTA:** 11737ms

---

### ✅ Q66 — Epidemiológica Simples

**Q:** Número de internações por doenças osteomusculares (grupo M00-M99) em mulheres em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND icd_group = 'M00-M99'
  AND sexo_paciente = 'F';
```

**Resultado:** `got=2327 exp=2327`
**TTA:** 12018ms

---

### ✅ Q67 — Epidemiológica Simples

**Q:** Número de óbitos em internações por fratura de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_obitos
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
  AND morte = 1;
```

**Resultado:** `got=1216 exp=1216`
**TTA:** 11638ms

---

### ✅ Q68 — Epidemiológica Simples

**Q:** Número de municípios distintos com registro de procedimentos ambulatoriais ortopédicos em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT cod_munic_estabelecimento) AS num_municipios
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=399 exp=399`
**TTA:** 11491ms

---

### ✅ Q69 — Epidemiológica Complexa

**Q:** Top 5 municípios por volume de procedimentos ambulatoriais ortopédicos em SP em 2022

**SQL gerado:**
```sql
SELECT cod_munic_estabelecimento,
  COUNT(*) AS total_procedimentos
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cod_munic_estabelecimento
ORDER BY total_procedimentos DESC
LIMIT 5;
```

**Resultado:** `5 linhas corretas (flexível)`
**TTA:** 11846ms

---

### ❌ Q70 — Epidemiológica Complexa

**Q:** Distribuição de procedimentos ambulatoriais ortopédicos por faixa etária em SP em 2022

**SQL gerado:**
```sql
SELECT 
  CASE
    WHEN idade_paciente < 18 THEN '0-17'
    WHEN idade_paciente BETWEEN 18 AND 34 THEN '18-34'
    WHEN idade_paciente BETWEEN 35 AND 49 THEN '35-49'
    WHEN idade_paciente BETWEEN 50 AND 64 THEN '50-64'
    ELSE '65+'
  END AS faixa_etaria,
  COUNT(*) AS total,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY faixa_etaria
ORDER BY total DESC;
```

**Resultado:** `Número de linhas: got=5 exp=3`
**TTA:** 12293ms

---

### ❌ Q71 — Epidemiológica Complexa

**Q:** Top 5 diagnósticos com maior número de óbitos em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT cid_principal,
  COUNT(*) AS total_obitos
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND morte = 1
GROUP BY cid_principal
ORDER BY total_obitos DESC
LIMIT 5;
```

**Resultado:** `Linha 1: got={'cid_principal': 'S720', 'total_obitos': 455} exp={'cid_principal': 'S720', 'total_internacoes': 8980, 'total_obitos': 455.0}`
**TTA:** 11662ms

---

### ✅ Q72 — Epidemiológica Complexa

**Q:** Permanência hospitalar média por sexo em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT sexo_paciente,
  ROUND(AVG(dias_perm), 1) AS permanencia_media_dias
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY sexo_paciente
ORDER BY sexo_paciente;
```

**Resultado:** `2 linhas corretas, ordem livre (flexível)`
**TTA:** 12021ms

---

### ✅ Q73 — Epidemiológica Complexa

**Q:** Top 10 estabelecimentos por volume de procedimentos ambulatoriais ortopédicos em SP em 2022

**SQL gerado:**
```sql
SELECT cnes_estabelecimento,
  COUNT(*) AS total_procedimentos
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cnes_estabelecimento
ORDER BY total_procedimentos DESC
LIMIT 10;
```

**Resultado:** `10 linhas corretas (flexível)`
**TTA:** 11835ms

---

### ❌ Q74 — Epidemiológica Complexa

**Q:** Taxa de mortalidade por sexo em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT sexo_paciente,
  ROUND(100.0 * SUM(morte) / COUNT(DISTINCT n_aih), 2) AS taxa_mortalidade
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY sexo_paciente
ORDER BY taxa_mortalidade DESC;
```

**Resultado:** `Linha não encontrada: {'sexo_paciente': 'F', 'total_obitos': 1081.0, 'taxa_mortalidade': 4.18}`
**TTA:** 11947ms

---

### ❌ Q75 — Epidemiológica Complexa

**Q:** Distribuição de internações ortopédicas por grupo diagnóstico (M e S) e faixa etária em SP em 2022

**SQL gerado:**
```sql
SELECT 
  icd_group,
  CASE
    WHEN idade_paciente < 18 THEN '0-17'
    WHEN idade_paciente BETWEEN 18 AND 34 THEN '18-34'
    WHEN idade_paciente BETWEEN 35 AND 49 THEN '35-49'
    WHEN idade_paciente BETWEEN 50 AND 64 THEN '50-64'
    ELSE '65+'
  END AS faixa_etaria,
  COUNT(DISTINCT n_aih) AS total_internacoes,
  ROUND(100.0 * COUNT(DISTINCT n_aih) / SUM(COUNT(DISTINCT n_aih)) OVER (), 1) AS pct
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY icd_group, faixa_etaria
ORDER BY icd_group, faixa_etaria;
```

**Resultado:** `Número de linhas: got=10 exp=6`
**TTA:** 12287ms

---

### ❌ Q76 — Epidemiológica Complexa

**Q:** Top 5 diagnósticos de traumatismo com maior permanência média em SP em 2022 (mínimo 40 internações)

**SQL gerado:**
```sql
SELECT cid_principal,
  ROUND(AVG(dias_perm), 1) AS permanencia_media_dias,
  COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND icd_group = 'S00-T98'
GROUP BY cid_principal
HAVING COUNT(DISTINCT n_aih) >= 40
ORDER BY permanencia_media_dias DESC
LIMIT 5;
```

**Resultado:** `Linha 1: got={'cid_principal': 'S063', 'permanencia_media_dias': 16.4, 'total_internacoes': 78} exp={'cid_principal': 'S063', 'internacoes': 78, 'permanencia_media': 16.4}`
**TTA:** 12156ms

---

### ❌ Q77 — Epidemiológica Complexa

**Q:** Distribuição de internações ortopédicas por faixa etária (0-17, 18-39, 40-59, 60-79, 80+) em SP em 2022

**SQL gerado:**
```sql
SELECT 
  CASE
    WHEN idade_paciente BETWEEN 0 AND 17 THEN '0-17'
    WHEN idade_paciente BETWEEN 18 AND 39 THEN '18-39'
    WHEN idade_paciente BETWEEN 40 AND 59 THEN '40-59'
    WHEN idade_paciente BETWEEN 60 AND 79 THEN '60-79'
    ELSE '80+'
  END AS faixa_etaria,
  COUNT(DISTINCT n_aih) AS total_internacoes,
  ROUND(100.0 * COUNT(DISTINCT n_aih) / SUM(COUNT(DISTINCT n_aih)) OVER (), 1) AS pct
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY faixa_etaria
ORDER BY faixa_etaria;
```

**Resultado:** `Linha não encontrada: {'faixa_etaria': '0-17', 'total': 3619}`
**TTA:** 12259ms

---

### ✅ Q78 — Epidemiológica Complexa

**Q:** Top 5 estabelecimentos com maior volume de internações por fratura de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT cnes_estabelecimento,
  COUNT(DISTINCT n_aih) AS internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
GROUP BY cnes_estabelecimento
ORDER BY internacoes DESC
LIMIT 5;
```

**Resultado:** `5 linhas corretas (exato)`
**TTA:** 11883ms

---

### ❌ Q79 — Epidemiológica Complexa

**Q:** Permanência hospitalar média por faixa etária em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT 
  CASE
    WHEN idade_paciente < 18 THEN '0-17'
    WHEN idade_paciente BETWEEN 18 AND 34 THEN '18-34'
    WHEN idade_paciente BETWEEN 35 AND 49 THEN '35-49'
    WHEN idade_paciente BETWEEN 50 AND 64 THEN '50-64'
    ELSE '65+'
  END AS faixa_etaria,
  ROUND(AVG(dias_perm), 1) AS permanencia_media_dias
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY faixa_etaria
ORDER BY faixa_etaria;
```

**Resultado:** `Número de linhas: got=5 exp=3`
**TTA:** 12275ms

---

### ✅ Q80 — Epidemiológica Complexa

**Q:** Top 5 diagnósticos de doença osteomuscular (M00-M99) por número de internações em SP em 2022

**SQL gerado:**
```sql
SELECT cid_principal,
  COUNT(DISTINCT n_aih) AS internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND icd_group = 'M00-M99'
GROUP BY cid_principal
ORDER BY internacoes DESC
LIMIT 5;
```

**Resultado:** `5 linhas corretas (flexível)`
**TTA:** 11617ms

---

### ✅ Q81 — Epidemiológica Complexa

**Q:** Número de internações ortopédicas com permanência entre 3 e 7 dias em SP em 2022

**SQL gerado:**
```sql
SELECT COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND dias_perm BETWEEN 3 AND 7;
```

**Resultado:** `got=25807 exp=25807`
**TTA:** 12062ms

---

### ✅ Q82 — Epidemiológica Complexa

**Q:** Top 5 diagnósticos ambulatoriais ortopédicos em idosos (60 anos ou mais) em SP em 2022

**SQL gerado:**
```sql
SELECT cid_principal,
  COUNT(*) AS total_procedimentos
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND idade_paciente >= 60
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cid_principal
ORDER BY total_procedimentos DESC
LIMIT 5;
```

**Resultado:** `5 linhas corretas (flexível)`
**TTA:** 12079ms

---

### ✅ Q83 — Epidemiológica Complexa

**Q:** Média de internações ortopédicas por estabelecimento em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(AVG(internacoes), 2) AS media_internacoes
FROM (
  SELECT cnes_estabelecimento,
    COUNT(DISTINCT n_aih) AS internacoes
  FROM processed
  WHERE sistema = 'SIH'
    AND uf_origem = 'SP'
    AND ano_cmpt = 2022
    AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  GROUP BY cnes_estabelecimento
) sub;
```

**Resultado:** `got=173.15 exp=173.1`
**TTA:** 11357ms

---

### ❌ Q84 — Epidemiológica Complexa

**Q:** Distribuição de internações ortopédicas por faixa de permanência em SP em 2022

**SQL gerado:**
```sql
SELECT 
  CASE
    WHEN dias_perm <= 3 THEN '1-3 dias'
    WHEN dias_perm BETWEEN 4 AND 7 THEN '4-7 dias'
    WHEN dias_perm BETWEEN 8 AND 14 THEN '8-14 dias'
    ELSE '15+ dias'
  END AS faixa_permanencia,
  COUNT(DISTINCT n_aih) AS total_internacoes,
  ROUND(100.0 * COUNT(DISTINCT n_aih) / SUM(COUNT(DISTINCT n_aih)) OVER (), 1) AS pct
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY faixa_permanencia
ORDER BY total_internacoes DESC;
```

**Resultado:** `Linha não encontrada: {'faixa_permanencia': '1-3 dias', 'total': 27844}`
**TTA:** 12270ms

---

### ✅ Q85 — Financeira

**Q:** Custo total de internações ortopédicas de idosos (60 anos ou mais) em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND idade_paciente >= 60
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=78009817.26 exp=78009817.26`
**TTA:** 11973ms

---

### ✅ Q86 — Financeira

**Q:** Custo médio por internação por fratura de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(AVG(COALESCE(custo_total, 0)), 2) AS custo_medio
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
```

**Resultado:** `got=2700.27 exp=2700.27`
**TTA:** 11570ms

---

### ✅ Q87 — Financeira

**Q:** Custo total de internações ortopédicas com passagem por UTI em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND uti_int_to > 0;
```

**Resultado:** `got=15904.56 exp=15904.56`
**TTA:** 11741ms

---

### ✅ Q88 — Financeira

**Q:** Custo médio por internação ortopédica por sexo do paciente em SP em 2022

**SQL gerado:**
```sql
SELECT sexo_paciente,
  ROUND(AVG(COALESCE(custo_total, 0)), 2) AS custo_medio
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY sexo_paciente
ORDER BY custo_medio DESC;
```

**Resultado:** `2 linhas corretas, ordem livre (exato)`
**TTA:** 11737ms

---

### ✅ Q89 — Financeira

**Q:** Custo total de internações ortopédicas em crianças (menores de 18 anos) em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND idade_paciente < 18
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=7450877.25 exp=7450877.25`
**TTA:** 12031ms

---

### ✅ Q90 — Financeira

**Q:** Custo médio por internação ortopédica com permanência superior a 7 dias em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(AVG(COALESCE(custo_total, 0)), 2) AS custo_medio
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND dias_perm > 7;
```

**Resultado:** `got=4423.19 exp=4423.19`
**TTA:** 11536ms

---

### ✅ Q91 — Financeira

**Q:** Top 3 municípios com maior custo total em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT cod_munic_estabelecimento,
  ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cod_munic_estabelecimento
ORDER BY custo_total DESC
LIMIT 3;
```

**Resultado:** `3 linhas corretas (exato)`
**TTA:** 12000ms

---

### ✅ Q92 — Financeira

**Q:** Valor total de serviço hospitalar em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(SUM(COALESCE(val_sh, 0)), 2) AS valor_total_servico_hospitalar
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=144636796.74 exp=144636796.74`
**TTA:** 11905ms

---

### ❌ Q93 — Financeira

**Q:** Custo médio por internação ortopédica por faixa etária em SP em 2022

**SQL gerado:**
```sql
SELECT 
  CASE
    WHEN idade_paciente < 18 THEN '0-17'
    WHEN idade_paciente BETWEEN 18 AND 34 THEN '18-34'
    WHEN idade_paciente BETWEEN 35 AND 49 THEN '35-49'
    WHEN idade_paciente BETWEEN 50 AND 64 THEN '50-64'
    ELSE '65+'
  END AS faixa_etaria,
  ROUND(AVG(COALESCE(custo_total, 0)), 2) AS custo_medio
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY faixa_etaria
ORDER BY faixa_etaria;
```

**Resultado:** `Número de linhas: got=5 exp=3`
**TTA:** 12010ms

---

### ✅ Q94 — Financeira

**Q:** Valor total de UTI em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(SUM(COALESCE(val_uti, 0)), 2) AS valor_total_uti
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=37073666.92 exp=37073666.92`
**TTA:** 11711ms

---

### ✅ Q95 — Financeira

**Q:** Custo total de internações por doenças osteomusculares (M00-M99) em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND icd_group = 'M00-M99';
```

**Resultado:** `got=13990374.04 exp=13990374.04`
**TTA:** 11972ms

---

### ✅ Q96 — Financeira

**Q:** Custo total de internações por traumatismos (S00-T98) em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND icd_group = 'S00-T98';
```

**Resultado:** `got=161293054.73 exp=161293054.73`
**TTA:** 11701ms

---

### ❌ Q97 — Financeira

**Q:** Top 5 diagnósticos com maior custo médio por internação ortopédica em SP em 2022 (mínimo 40 internações)

**SQL gerado:**
```sql
SELECT cid_principal,
  ROUND(SUM(COALESCE(custo_total, 0)) / COUNT(DISTINCT n_aih), 2) AS custo_medio
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY cid_principal
HAVING COUNT(DISTINCT n_aih) >= 40
ORDER BY custo_medio DESC
LIMIT 5;
```

**Resultado:** `Linha 1: got={'cid_principal': 'S221', 'custo_medio': 14053.88} exp={'cid_principal': 'S221', 'internacoes': 41, 'custo_medio': 14053.88}`
**TTA:** 11612ms

---

### ✅ Q98 — Financeira

**Q:** Custo médio por procedimento ambulatorial ortopédico em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(AVG(COALESCE(custo_total, 0)), 2) AS custo_medio
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=83.05 exp=83.05`
**TTA:** 11533ms

---

### ❌ Q99 — Financeira

**Q:** Top 5 procedimentos ambulatoriais com maior custo total em SP em 2022

**SQL gerado:**
```sql
SELECT cod_procedimento,
  ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
GROUP BY cod_procedimento
ORDER BY custo_total DESC
LIMIT 5;
```

**Resultado:** `Linha 1: got={'cod_procedimento': '0702100048', 'custo_total': 46893518.21} exp={'cod_procedimento': '0701020369', 'custo_total': 1201460.4}`
**TTA:** 11883ms

---

### ✅ Q100 — Financeira

**Q:** Custo total de internações ortopédicas de idosos (60+) com permanência superior a 7 dias em SP em 2022

**SQL gerado:**
```sql
SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND idade_paciente >= 60
  AND dias_perm > 7
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98');
```

**Resultado:** `got=35943333.49 exp=35943333.49`
**TTA:** 11719ms

---

### ✅ Q101 — Temporal/Comparativa

**Q:** Mês com maior número de óbitos em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  COUNT(*) AS total_obitos
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
  AND morte = 1
GROUP BY mes_cmpt
ORDER BY total_obitos DESC
LIMIT 1;
```

**Resultado:** `1 linhas corretas (exato)`
**TTA:** 12163ms

---

### ✅ Q102 — Temporal/Comparativa

**Q:** Volume mensal de internações por fratura de fêmur (S72) em idosos (60 anos ou mais) em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
  AND idade_paciente >= 60
GROUP BY mes_cmpt
ORDER BY mes_cmpt;
```

**Resultado:** `12 linhas corretas (flexível)`
**TTA:** 11441ms

---

### ✅ Q103 — Temporal/Comparativa

**Q:** Custo mensal de internações ortopédicas de idosos (60 anos ou mais) em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND idade_paciente >= 60
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY mes_cmpt;
```

**Resultado:** `12 linhas corretas (exato)`
**TTA:** 11837ms

---

### ✅ Q104 — Temporal/Comparativa

**Q:** Mês com maior custo total de internações por fratura de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
GROUP BY mes_cmpt
ORDER BY custo_total DESC
LIMIT 1;
```

**Resultado:** `1 linhas corretas (exato)`
**TTA:** 12085ms

---

### ✅ Q105 — Temporal/Comparativa

**Q:** Volume mensal de internações por traumatismos (grupo S00-T98) em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND icd_group = 'S00-T98'
GROUP BY mes_cmpt
ORDER BY mes_cmpt;
```

**Resultado:** `12 linhas corretas (flexível)`
**TTA:** 11592ms

---

### ✅ Q106 — Temporal/Comparativa

**Q:** Volume mensal de procedimentos ambulatoriais ortopédicos por sexo em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  sexo_paciente,
  COUNT(*) AS total_procedimentos
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt, sexo_paciente
ORDER BY mes_cmpt, sexo_paciente;
```

**Resultado:** `24 linhas corretas (flexível)`
**TTA:** 12034ms

---

### ✅ Q107 — Temporal/Comparativa

**Q:** Comparativo trimestral de internações ortopédicas por grupo diagnóstico em SP em 2022

**SQL gerado:**
```sql
SELECT
  CASE
    WHEN mes_cmpt BETWEEN 1 AND 3 THEN 'Q1'
    WHEN mes_cmpt BETWEEN 4 AND 6 THEN 'Q2'
    WHEN mes_cmpt BETWEEN 7 AND 9 THEN 'Q3'
    ELSE 'Q4'
  END AS trimestre,
  icd_group,
  COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY trimestre, icd_group
ORDER BY trimestre, icd_group;
```

**Resultado:** `8 linhas corretas, ordem livre (flexível)`
**TTA:** 11948ms

---

### ✅ Q108 — Temporal/Comparativa

**Q:** Mês com maior permanência hospitalar média em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  ROUND(AVG(dias_perm), 1) AS permanencia_media_dias
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY permanencia_media_dias DESC
LIMIT 1;
```

**Resultado:** `1 linhas corretas (flexível)`
**TTA:** 11817ms

---

### ❌ Q109 — Temporal/Comparativa

**Q:** Evolução mensal da taxa de mortalidade em internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  ROUND(100.0 * SUM(CASE WHEN morte = 1 THEN 1 ELSE 0 END) / COUNT(DISTINCT n_aih), 2) AS taxa_mortalidade
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY mes_cmpt;
```

**Resultado:** `Linha 1: got={'mes_cmpt': 1, 'taxa_mortalidade': 3.44} exp={'mes_cmpt': 1, 'internacoes': 5209, 'obitos': 179.0, 'taxa_mortalidade': 3.44}`
**TTA:** 12058ms

---

### ✅ Q110 — Temporal/Comparativa

**Q:** Trimestre com maior volume de internações por fratura de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT
  CASE
    WHEN mes_cmpt BETWEEN 1 AND 3 THEN 'Q1'
    WHEN mes_cmpt BETWEEN 4 AND 6 THEN 'Q2'
    WHEN mes_cmpt BETWEEN 7 AND 9 THEN 'Q3'
    ELSE 'Q4'
  END AS trimestre,
  COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
GROUP BY trimestre
ORDER BY total_internacoes DESC
LIMIT 1;
```

**Resultado:** `1 linhas corretas (flexível)`
**TTA:** 12079ms

---

### ❌ Q111 — Temporal/Comparativa

**Q:** Comparativo semestral de internações ortopédicas (1º vs 2º semestre) em SP em 2022

**SQL gerado:**
```sql
SELECT
  CASE
    WHEN mes_cmpt BETWEEN 1 AND 6 THEN '1º Semestre'
    ELSE '2º Semestre'
  END AS semestre,
  COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY semestre
ORDER BY semestre;
```

**Resultado:** `Linha não encontrada: {'semestre': '1S', 'total': 31575}`
**TTA:** 13243ms

---

### ✅ Q112 — Temporal/Comparativa

**Q:** Custo médio mensal por internação por fratura de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  ROUND(AVG(COALESCE(custo_total, 0)), 2) AS custo_medio_mensal
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
GROUP BY mes_cmpt
ORDER BY mes_cmpt;
```

**Resultado:** `12 linhas corretas (flexível)`
**TTA:** 10367ms

---

### ✅ Q113 — Temporal/Comparativa

**Q:** Volume mensal de internações ortopédicas por sexo do paciente em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  sexo_paciente,
  COUNT(DISTINCT n_aih) AS total_internacoes
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt, sexo_paciente
ORDER BY mes_cmpt, sexo_paciente;
```

**Resultado:** `24 linhas corretas (flexível)`
**TTA:** 12346ms

---

### ✅ Q114 — Temporal/Comparativa

**Q:** Mês com menor volume de procedimentos ambulatoriais ortopédicos em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  COUNT(*) AS total_procedimentos
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY total_procedimentos ASC
LIMIT 1;
```

**Resultado:** `1 linhas corretas (flexível)`
**TTA:** 11310ms

---

### ✅ Q115 — Temporal/Comparativa

**Q:** Volume trimestral de procedimentos ambulatoriais ortopédicos em SP em 2022

**SQL gerado:**
```sql
SELECT
  CASE
    WHEN mes_cmpt BETWEEN 1 AND 3 THEN 'Q1'
    WHEN mes_cmpt BETWEEN 4 AND 6 THEN 'Q2'
    WHEN mes_cmpt BETWEEN 7 AND 9 THEN 'Q3'
    ELSE 'Q4'
  END AS trimestre,
  COUNT(*) AS total_procedimentos
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY trimestre
ORDER BY trimestre;
```

**Resultado:** `4 linhas corretas, ordem livre (flexível)`
**TTA:** 11927ms

---

### ✅ Q116 — Temporal/Comparativa

**Q:** Custo mensal de procedimentos ambulatoriais ortopédicos em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIA'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY mes_cmpt
ORDER BY mes_cmpt;
```

**Resultado:** `12 linhas corretas (exato)`
**TTA:** 12001ms

---

### ❌ Q117 — Temporal/Comparativa

**Q:** Comparativo semestral do custo total de internações ortopédicas em SP em 2022

**SQL gerado:**
```sql
SELECT
  CASE
    WHEN mes_cmpt BETWEEN 1 AND 6 THEN 'Semestre 1'
    ELSE 'Semestre 2'
  END AS semestre,
  ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
GROUP BY semestre
ORDER BY semestre;
```

**Resultado:** `Linha não encontrada: {'semestre': '1S', 'custo_total': 82207816.93}`
**TTA:** 12117ms

---

### ✅ Q118 — Temporal/Comparativa

**Q:** Evolução mensal da permanência média em internações por fratura de fêmur (S72) em SP em 2022

**SQL gerado:**
```sql
SELECT mes_cmpt,
  ROUND(AVG(dias_perm), 1) AS permanencia_media_dias
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_cmpt = 2022
  AND cid_principal LIKE 'S72%'
GROUP BY mes_cmpt
ORDER BY mes_cmpt;
```

**Resultado:** `12 linhas corretas (flexível)`
**TTA:** 12698ms

---

## Failures Detail

- **Q18** (Epidemiológica Complexa): Linha não encontrada: {'faixa_etaria': '0-17', 'total': 3619}
  SQL: `SELECT 
  CASE
    WHEN idade_paciente < 18 THEN '0-17'
    WHEN idade_paciente BETWEEN 18 AND 59 THEN '18-59'
    ELSE ...`
- **Q19** (Epidemiológica Complexa): Linha 1: got={'cid_principal': 'S279', 'taxa_mortalidade': 100.0} exp={'cid_principal': 'S062', 'total_internacoes': 70, 'total_obitos': 21.0, 'taxa_mortalidade_pct': 30.0}
  SQL: `SELECT cid_principal,
  ROUND(100.0 * SUM(morte) / COUNT(DISTINCT n_aih), 2) AS taxa_mortalidade
FROM processed
WHERE si...`
- **Q22** (Epidemiológica Complexa): Linha 1: got={'internacoes_idosos': 19117, 'internacoes_totais': 29356} exp={'total_s72': 29356, 's72_idosos': 19120.0, 'pct_idosos': 65.1}
  SQL: `SELECT
  COUNT(DISTINCT CASE WHEN idade_paciente >= 60 THEN n_aih END) AS internacoes_idosos,
  COUNT(DISTINCT n_aih) AS...`
- **Q23** (Epidemiológica Complexa): Linha 1: got={'cid_principal': 'T918', 'permanencia_media_dias': 68.0} exp={'cid_principal': 'S063', 'total_internacoes': 78, 'permanencia_media_dias': 16.4}
  SQL: `SELECT cid_principal,
  ROUND(AVG(dias_perm), 1) AS permanencia_media_dias
FROM processed
WHERE sistema = 'SIH'
  AND uf...`
- **Q26** (Epidemiológica Complexa): Número de linhas: got=1015 exp=10
  SQL: `SELECT cid_principal,
  SUM(dias_perm) AS total_dias_internacao
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = '...`
- **Q28** (Epidemiológica Complexa): Número de linhas: got=2 exp=1
  SQL: `SELECT 
  CASE 
    WHEN e.clinical_deslocamento_territorial = 'deslocamento intermunicipal' THEN 'Outro Município'
    ...`
- **Q29** (Epidemiológica Complexa): Número de linhas: got=12 exp=24
  SQL: `SELECT mes_cmpt,
  SUM(CASE WHEN sistema = 'SIA' THEN 1 ELSE 0 END) AS sia_volume,
  SUM(CASE WHEN sistema = 'SIH' THEN ...`
- **Q30** (Epidemiológica Complexa): Linha 1: got={'idade_media': 62.4} exp={'idade_media': 62.4, 'idade_minima': 0.0, 'idade_maxima': 99.0}
  SQL: `SELECT ROUND(AVG(idade_paciente), 1) AS idade_media
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND an...`
- **Q34** (Financeira): Linha 1: got={'cid_principal': 'S720', 'custo_total': 29879588.9} exp={'cid_principal': 'S720', 'internacoes': 8980, 'custo_total': 29879588.9}
  SQL: `SELECT cid_principal,
  ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AN...`
- **Q35** (Financeira): Linha 1: got={'custo_total': None} exp={'internacoes_com_opme': 0, 'custo_total_opme': None}
  SQL: `SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP...`
- **Q38** (Financeira): Linha 1: got={'cnes_estabelecimento': '2078015', 'custo_total': 11218869.54} exp={'cnes_estabelecimento': '2078015', 'internacoes': 2221, 'custo_total': 11218869.54}
  SQL: `SELECT cnes_estabelecimento,
  ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SI...`
- **Q40** (Financeira): Linha 1: got={'total_val_sh': 144636796.74, 'total_val_sp': 30640001.52, 'pct_val_sh': 82.5, 'pct_val_sp': 17.5} exp={'custo_servico_hospitalar': 144636796.74, 'honorarios_profissionais': 30640001.52, 'pct_hospitalar': 82.5}
  SQL: `SELECT
  ROUND(SUM(COALESCE(val_sh, 0)), 2) AS total_val_sh,
  ROUND(SUM(COALESCE(val_sp, 0)), 2) AS total_val_sp,
  ROU...`
- **Q42** (Temporal/Comparativa): Linha 1: got={'mes_cmpt': 1, 'total_procedimentos': 5898, 'pct': 5.6} exp={'mes_cmpt': 1, 'procedimentos': 5898}
  SQL: `SELECT mes_cmpt,
  COUNT(*) AS total_procedimentos,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM pro...`
- **Q45** (Temporal/Comparativa): Número de linhas: got=12 exp=24
  SQL: `SELECT 
  mes_cmpt,
  SUM(CASE WHEN icd_group = 'S00-T98' THEN 1 ELSE 0 END) AS internacoes_trauma,
  SUM(CASE WHEN icd_...`
- **Q70** (Epidemiológica Complexa): Número de linhas: got=5 exp=3
  SQL: `SELECT 
  CASE
    WHEN idade_paciente < 18 THEN '0-17'
    WHEN idade_paciente BETWEEN 18 AND 34 THEN '18-34'
    WHEN ...`
- **Q71** (Epidemiológica Complexa): Linha 1: got={'cid_principal': 'S720', 'total_obitos': 455} exp={'cid_principal': 'S720', 'total_internacoes': 8980, 'total_obitos': 455.0}
  SQL: `SELECT cid_principal,
  COUNT(*) AS total_obitos
FROM processed
WHERE sistema = 'SIH'
  AND uf_origem = 'SP'
  AND ano_c...`
- **Q74** (Epidemiológica Complexa): Linha não encontrada: {'sexo_paciente': 'F', 'total_obitos': 1081.0, 'taxa_mortalidade': 4.18}
  SQL: `SELECT sexo_paciente,
  ROUND(100.0 * SUM(morte) / COUNT(DISTINCT n_aih), 2) AS taxa_mortalidade
FROM processed
WHERE si...`
- **Q75** (Epidemiológica Complexa): Número de linhas: got=10 exp=6
  SQL: `SELECT 
  icd_group,
  CASE
    WHEN idade_paciente < 18 THEN '0-17'
    WHEN idade_paciente BETWEEN 18 AND 34 THEN '18-...`
- **Q76** (Epidemiológica Complexa): Linha 1: got={'cid_principal': 'S063', 'permanencia_media_dias': 16.4, 'total_internacoes': 78} exp={'cid_principal': 'S063', 'internacoes': 78, 'permanencia_media': 16.4}
  SQL: `SELECT cid_principal,
  ROUND(AVG(dias_perm), 1) AS permanencia_media_dias,
  COUNT(DISTINCT n_aih) AS total_internacoes...`
- **Q77** (Epidemiológica Complexa): Linha não encontrada: {'faixa_etaria': '0-17', 'total': 3619}
  SQL: `SELECT 
  CASE
    WHEN idade_paciente BETWEEN 0 AND 17 THEN '0-17'
    WHEN idade_paciente BETWEEN 18 AND 39 THEN '18-3...`
- **Q79** (Epidemiológica Complexa): Número de linhas: got=5 exp=3
  SQL: `SELECT 
  CASE
    WHEN idade_paciente < 18 THEN '0-17'
    WHEN idade_paciente BETWEEN 18 AND 34 THEN '18-34'
    WHEN ...`
- **Q84** (Epidemiológica Complexa): Linha não encontrada: {'faixa_permanencia': '1-3 dias', 'total': 27844}
  SQL: `SELECT 
  CASE
    WHEN dias_perm <= 3 THEN '1-3 dias'
    WHEN dias_perm BETWEEN 4 AND 7 THEN '4-7 dias'
    WHEN dias_...`
- **Q93** (Financeira): Número de linhas: got=5 exp=3
  SQL: `SELECT 
  CASE
    WHEN idade_paciente < 18 THEN '0-17'
    WHEN idade_paciente BETWEEN 18 AND 34 THEN '18-34'
    WHEN ...`
- **Q97** (Financeira): Linha 1: got={'cid_principal': 'S221', 'custo_medio': 14053.88} exp={'cid_principal': 'S221', 'internacoes': 41, 'custo_medio': 14053.88}
  SQL: `SELECT cid_principal,
  ROUND(SUM(COALESCE(custo_total, 0)) / COUNT(DISTINCT n_aih), 2) AS custo_medio
FROM processed
WH...`
- **Q99** (Financeira): Linha 1: got={'cod_procedimento': '0702100048', 'custo_total': 46893518.21} exp={'cod_procedimento': '0701020369', 'custo_total': 1201460.4}
  SQL: `SELECT cod_procedimento,
  ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
FROM processed
WHERE sistema = 'SIA'
 ...`
- **Q109** (Temporal/Comparativa): Linha 1: got={'mes_cmpt': 1, 'taxa_mortalidade': 3.44} exp={'mes_cmpt': 1, 'internacoes': 5209, 'obitos': 179.0, 'taxa_mortalidade': 3.44}
  SQL: `SELECT mes_cmpt,
  ROUND(100.0 * SUM(CASE WHEN morte = 1 THEN 1 ELSE 0 END) / COUNT(DISTINCT n_aih), 2) AS taxa_mortalid...`
- **Q111** (Temporal/Comparativa): Linha não encontrada: {'semestre': '1S', 'total': 31575}
  SQL: `SELECT
  CASE
    WHEN mes_cmpt BETWEEN 1 AND 6 THEN '1º Semestre'
    ELSE '2º Semestre'
  END AS semestre,
  COUNT(DIS...`
- **Q117** (Temporal/Comparativa): Linha não encontrada: {'semestre': '1S', 'custo_total': 82207816.93}
  SQL: `SELECT
  CASE
    WHEN mes_cmpt BETWEEN 1 AND 6 THEN 'Semestre 1'
    ELSE 'Semestre 2'
  END AS semestre,
  ROUND(SUM(C...`

---
_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_