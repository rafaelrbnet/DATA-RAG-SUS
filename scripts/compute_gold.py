"""
compute_gold.py — Pre-compute benchmark gold-standard for any state/year.

Runs each query's reference SQL against DuckDB and saves the results to
results/gold_{state}_{year}.json, which evaluate_benchmark.py then loads
via --gold-file.

Usage:
    uv run python scripts/compute_gold.py --state SP --year 2023
    uv run python scripts/compute_gold.py --state RJ --year 2022
    uv run python scripts/compute_gold.py --state RJ --year 2023
    uv run python scripts/compute_gold.py --state SP --year 2022 --verify

    # --verify: cross-checks SP/2022 output against hardcoded values in
    # evaluate_benchmark.py (use once to validate the gold SQL is correct).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.rag.executor import query as duckdb_query

# ── Gold SQL templates ────────────────────────────────────────────────────────
# Placeholders: {state} → state abbreviation (SP, RJ, MG...)
#               {year}  → integer year (2022, 2023...)
# Column names MUST match exactly what evaluate_benchmark.py BENCHMARK["gold"] expects.

GOLD_SQL: dict[str, str] = {
    "Q01": """
        SELECT COUNT(DISTINCT n_aih) AS total_internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q02": """
        SELECT COUNT(*) AS total_procedimentos
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q03": """
        SELECT COUNT(DISTINCT n_aih) AS total_internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'""",
    "Q04": """
        SELECT COUNT(DISTINCT n_aih) AS total_internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (cid_principal LIKE 'M16%' OR cid_principal LIKE 'M17%')""",
    "Q05": """
        SELECT COUNT(DISTINCT n_aih) AS total_obitos
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND morte = 1""",
    "Q06": """
        SELECT COUNT(DISTINCT n_aih) AS total_internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND icd_group = 'S00-T98'""",
    "Q07": """
        SELECT COUNT(DISTINCT n_aih) AS total_internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND icd_group = 'M00-M99'""",
    "Q08": """
        SELECT COUNT(DISTINCT n_aih) AS total_mulheres_internadas
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND sexo_paciente = 'F'
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q09": """
        SELECT COUNT(DISTINCT n_aih) AS total_homens_internados
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND sexo_paciente = 'M'
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q10": """
        SELECT COUNT(DISTINCT n_aih) AS total_internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND idade_paciente >= 60
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q11": """
        SELECT COUNT(DISTINCT cod_munic_estabelecimento) AS num_municipios
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q12": """
        SELECT COUNT(DISTINCT cnes_estabelecimento) AS num_estabelecimentos
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q13": """
        SELECT COUNT(DISTINCT n_aih) AS total_internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND dias_perm > 7""",
    "Q14": """
        SELECT COUNT(DISTINCT n_aih) AS total_internacoes_uti
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND uti_int_to > 0""",
    "Q15": """
        SELECT COUNT(DISTINCT n_aih) AS total_internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'
          AND idade_paciente >= 70""",
    "Q16": """
        SELECT cid_principal, COUNT(DISTINCT n_aih) AS total_internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY cid_principal
        ORDER BY total_internacoes DESC
        LIMIT 10""",
    "Q17": """
        SELECT cod_munic_estabelecimento, COUNT(DISTINCT n_aih) AS internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY cod_munic_estabelecimento
        ORDER BY internacoes DESC
        LIMIT 5""",
    "Q18": """
        SELECT
          CASE
            WHEN idade_paciente < 18 THEN '0-17'
            WHEN idade_paciente BETWEEN 18 AND 59 THEN '18-59'
            ELSE '60+'
          END AS faixa_etaria,
          COUNT(DISTINCT n_aih) AS total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY faixa_etaria
        ORDER BY faixa_etaria""",
    "Q19": """
        SELECT cid_principal,
          COUNT(DISTINCT n_aih) AS total_internacoes,
          SUM(CASE WHEN morte = 1 THEN 1 ELSE 0 END) AS total_obitos,
          ROUND(100.0 * SUM(CASE WHEN morte = 1 THEN 1 ELSE 0 END)
                / COUNT(DISTINCT n_aih), 2) AS taxa_mortalidade_pct
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY cid_principal
        HAVING COUNT(DISTINCT n_aih) >= 40
        ORDER BY taxa_mortalidade_pct DESC
        LIMIT 5""",
    "Q20": """
        SELECT cid_principal, COUNT(*) AS total_procedimentos
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY cid_principal
        ORDER BY total_procedimentos DESC
        LIMIT 10""",
    "Q21": """
        SELECT sexo_paciente,
          COUNT(DISTINCT n_aih) AS total_internacoes,
          ROUND(100.0 * COUNT(DISTINCT n_aih) / SUM(COUNT(DISTINCT n_aih)) OVER (), 1) AS pct
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY sexo_paciente
        ORDER BY total_internacoes DESC""",
    "Q22": """
        SELECT
          COUNT(DISTINCT n_aih) AS total_s72,
          SUM(CASE WHEN idade_paciente >= 60 THEN 1 ELSE 0 END) AS s72_idosos,
          ROUND(100.0 * SUM(CASE WHEN idade_paciente >= 60 THEN 1 ELSE 0 END)
                / COUNT(DISTINCT n_aih), 1) AS pct_idosos
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'""",
    "Q23": """
        SELECT cid_principal,
          COUNT(DISTINCT n_aih) AS total_internacoes,
          ROUND(AVG(dias_perm), 1) AS permanencia_media_dias
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY cid_principal
        HAVING COUNT(DISTINCT n_aih) >= 40
        ORDER BY permanencia_media_dias DESC
        LIMIT 5""",
    "Q24": """
        SELECT cnes_estabelecimento, COUNT(DISTINCT n_aih) AS internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY cnes_estabelecimento
        ORDER BY internacoes DESC
        LIMIT 5""",
    "Q25": """
        SELECT raca_cor_paciente,
          COUNT(DISTINCT n_aih) AS total,
          ROUND(100.0 * COUNT(DISTINCT n_aih) / SUM(COUNT(DISTINCT n_aih)) OVER (), 1) AS pct
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY raca_cor_paciente
        ORDER BY total DESC""",
    "Q26": """
        SELECT cid_principal,
          COUNT(DISTINCT n_aih) AS internacoes,
          SUM(dias_perm) AS total_dias_internacao
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY cid_principal
        ORDER BY total_dias_internacao DESC
        LIMIT 10""",
    "Q27": """
        SELECT cod_procedimento, COUNT(*) AS total_procedimentos
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY cod_procedimento
        ORDER BY total_procedimentos DESC
        LIMIT 5""",
    "Q28": """
        SELECT
          COUNT(DISTINCT n_aih) AS total_internacoes,
          SUM(CASE WHEN cod_munic_residencia != cod_munic_estabelecimento
                   THEN 1 ELSE 0 END) AS deslocamento_intermunicipal,
          ROUND(100.0 * SUM(CASE WHEN cod_munic_residencia != cod_munic_estabelecimento
                                 THEN 1 ELSE 0 END) / COUNT(DISTINCT n_aih), 1) AS pct_deslocamento
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q29": """
        SELECT mes_cmpt, sistema,
          CASE WHEN sistema = 'SIH' THEN COUNT(DISTINCT n_aih) ELSE COUNT(*) END AS total
        FROM processed
        WHERE uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY mes_cmpt, sistema
        ORDER BY mes_cmpt, sistema""",
    "Q30": """
        SELECT
          ROUND(AVG(idade_paciente), 1) AS idade_media,
          MIN(idade_paciente) AS idade_minima,
          MAX(idade_paciente) AS idade_maxima
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'""",
    "Q31": """
        SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q32": """
        SELECT ROUND(AVG(COALESCE(custo_total, 0)), 2) AS custo_medio
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q33": """
        SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'""",
    "Q34": """
        SELECT cid_principal,
          COUNT(DISTINCT n_aih) AS internacoes,
          ROUND(SUM(custo_total), 2) AS custo_total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY cid_principal
        ORDER BY custo_total DESC
        LIMIT 5""",
    "Q35": """
        SELECT
          COUNT(DISTINCT n_aih) AS internacoes_com_opme,
          SUM(val_ortp) AS custo_total_opme
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND val_ortp > 0""",
    "Q36": """
        SELECT ROUND(SUM(COALESCE(val_sp, 0)), 2) AS total_honorarios_profissionais
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q37": """
        SELECT ROUND(SUM(custo_total) / SUM(dias_perm), 2) AS custo_por_dia
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q38": """
        SELECT cnes_estabelecimento,
          COUNT(DISTINCT n_aih) AS internacoes,
          ROUND(SUM(custo_total), 2) AS custo_total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY cnes_estabelecimento
        ORDER BY custo_total DESC
        LIMIT 5""",
    "Q39": """
        SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q40": """
        SELECT
          ROUND(SUM(val_sh), 2) AS custo_servico_hospitalar,
          ROUND(SUM(val_sp), 2) AS honorarios_profissionais,
          ROUND(100.0 * SUM(val_sh) / (SUM(val_sh) + SUM(val_sp)), 1) AS pct_hospitalar
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q41": """
        SELECT mes_cmpt, COUNT(DISTINCT n_aih) AS total_internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY mes_cmpt
        ORDER BY mes_cmpt""",
    "Q42": """
        SELECT mes_cmpt, COUNT(*) AS procedimentos
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY mes_cmpt
        ORDER BY mes_cmpt""",
    "Q43": """
        SELECT mes_cmpt, COUNT(DISTINCT n_aih) AS total_internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'
        GROUP BY mes_cmpt
        ORDER BY total_internacoes DESC
        LIMIT 1""",
    "Q44": """
        SELECT mes_cmpt, ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY mes_cmpt
        ORDER BY mes_cmpt""",
    "Q45": """
        SELECT mes_cmpt, icd_group, COUNT(DISTINCT n_aih) AS internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY mes_cmpt, icd_group
        ORDER BY mes_cmpt, icd_group""",
    "Q46": """
        SELECT mes_cmpt, COUNT(*) AS total_obitos
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND morte = 1
        GROUP BY mes_cmpt
        ORDER BY mes_cmpt""",
    "Q47": """
        SELECT
          CASE
            WHEN mes_cmpt BETWEEN 1 AND 3 THEN 'Q1'
            WHEN mes_cmpt BETWEEN 4 AND 6 THEN 'Q2'
            WHEN mes_cmpt BETWEEN 7 AND 9 THEN 'Q3'
            ELSE 'Q4'
          END AS trimestre,
          ROUND(AVG(dias_perm), 1) AS permanencia_media_dias
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY trimestre
        ORDER BY trimestre""",
    "Q48": """
        SELECT mes_cmpt,
          ROUND(SUM(COALESCE(custo_total, 0)) / COUNT(DISTINCT n_aih), 2) AS custo_medio
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY mes_cmpt
        ORDER BY custo_medio DESC
        LIMIT 1""",
    "Q49": """
        SELECT mes_cmpt, COUNT(DISTINCT n_aih) AS total_fraturas
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'
        GROUP BY mes_cmpt
        ORDER BY mes_cmpt""",
    "Q50": """
        SELECT mes_cmpt, COUNT(DISTINCT n_aih) AS total_internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND idade_paciente >= 60
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY mes_cmpt
        ORDER BY mes_cmpt""",
    # ── Epidemiológica Simples (Q51–Q68) ─────────────────────────────────────
    "Q51": """
        SELECT COUNT(DISTINCT n_aih) AS total_mulheres_s72
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'
          AND sexo_paciente = 'F'""",
    "Q52": """
        SELECT COUNT(DISTINCT n_aih) AS total_homens_s72
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'
          AND sexo_paciente = 'M'""",
    "Q53": """
        SELECT COUNT(DISTINCT n_aih) AS internacoes_curtas
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND dias_perm <= 1""",
    "Q54": """
        SELECT COUNT(*) AS procedimentos_mulheres
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND sexo_paciente = 'F'""",
    "Q55": """
        SELECT COUNT(*) AS procedimentos_idosos
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND idade_paciente >= 60""",
    "Q56": """
        SELECT COUNT(DISTINCT n_aih) AS internacoes_fratura_tibia
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S82%'""",
    "Q57": """
        SELECT COUNT(DISTINCT n_aih) AS internacoes_fratura_punho
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S52%'""",
    "Q58": """
        SELECT COUNT(DISTINCT n_aih) AS internacoes_fratura_ombro
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S42%'""",
    "Q59": """
        SELECT COUNT(DISTINCT n_aih) AS internacoes_criancas
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND idade_paciente < 18""",
    "Q60": """
        SELECT COUNT(DISTINCT n_aih) AS internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND dias_perm BETWEEN 8 AND 30""",
    "Q61": """
        SELECT COUNT(DISTINCT n_aih) AS internacoes_artrose_idosos
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (cid_principal LIKE 'M16%' OR cid_principal LIKE 'M17%')
          AND idade_paciente >= 60""",
    "Q62": """
        SELECT COUNT(DISTINCT n_aih) AS internacoes_s72_mulheres_idosas
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'
          AND sexo_paciente = 'F'
          AND idade_paciente >= 70""",
    "Q63": """
        SELECT COUNT(DISTINCT n_aih) AS internacoes_longa_permanencia
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND dias_perm > 30""",
    "Q64": """
        SELECT COUNT(*) AS procedimentos_homens
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND sexo_paciente = 'M'""",
    "Q65": """
        SELECT COUNT(DISTINCT n_aih) AS internacoes_trauma_idosos
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND icd_group = 'S00-T98'
          AND idade_paciente >= 60""",
    "Q66": """
        SELECT COUNT(DISTINCT n_aih) AS internacoes_m_mulheres
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND icd_group = 'M00-M99'
          AND sexo_paciente = 'F'""",
    "Q67": """
        SELECT COUNT(DISTINCT n_aih) AS obitos_s72
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'
          AND morte = 1""",
    "Q68": """
        SELECT COUNT(DISTINCT cod_munic_estabelecimento) AS municipios_sia
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    # ── Epidemiológica Complexa (Q69–Q84) ────────────────────────────────────
    "Q69": """
        SELECT cod_munic_estabelecimento, COUNT(*) AS total
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY cod_munic_estabelecimento
        ORDER BY total DESC
        LIMIT 5""",
    "Q70": """
        SELECT
          CASE
            WHEN idade_paciente < 18 THEN '0-17'
            WHEN idade_paciente BETWEEN 18 AND 59 THEN '18-59'
            ELSE '60+'
          END AS faixa_etaria,
          COUNT(*) AS total
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY faixa_etaria
        ORDER BY faixa_etaria""",
    "Q71": """
        SELECT cid_principal,
          COUNT(DISTINCT n_aih) AS total_internacoes,
          SUM(CASE WHEN morte = 1 THEN 1 ELSE 0 END) AS total_obitos
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY cid_principal
        ORDER BY total_obitos DESC
        LIMIT 5""",
    "Q72": """
        SELECT sexo_paciente, ROUND(AVG(dias_perm), 1) AS permanencia_media
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY sexo_paciente
        ORDER BY permanencia_media DESC""",
    "Q73": """
        SELECT cnes_estabelecimento, COUNT(*) AS total
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY cnes_estabelecimento
        ORDER BY total DESC
        LIMIT 10""",
    "Q74": """
        SELECT sexo_paciente,
          SUM(CASE WHEN morte = 1 THEN 1 ELSE 0 END) AS total_obitos,
          ROUND(100.0 * SUM(CASE WHEN morte = 1 THEN 1 ELSE 0 END)
                / COUNT(DISTINCT n_aih), 2) AS taxa_mortalidade
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY sexo_paciente
        ORDER BY taxa_mortalidade DESC""",
    "Q75": """
        SELECT icd_group,
          CASE
            WHEN idade_paciente < 18 THEN '0-17'
            WHEN idade_paciente BETWEEN 18 AND 59 THEN '18-59'
            ELSE '60+'
          END AS faixa_etaria,
          COUNT(DISTINCT n_aih) AS total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY icd_group, faixa_etaria
        ORDER BY icd_group, faixa_etaria""",
    "Q76": """
        SELECT cid_principal, COUNT(DISTINCT n_aih) AS internacoes,
          ROUND(AVG(dias_perm), 1) AS permanencia_media
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND icd_group = 'S00-T98'
        GROUP BY cid_principal
        HAVING COUNT(DISTINCT n_aih) >= 40
        ORDER BY permanencia_media DESC
        LIMIT 5""",
    "Q77": """
        SELECT
          CASE
            WHEN idade_paciente < 18 THEN '0-17'
            WHEN idade_paciente BETWEEN 18 AND 39 THEN '18-39'
            WHEN idade_paciente BETWEEN 40 AND 59 THEN '40-59'
            WHEN idade_paciente BETWEEN 60 AND 79 THEN '60-79'
            ELSE '80+'
          END AS faixa_etaria,
          COUNT(DISTINCT n_aih) AS total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY faixa_etaria
        ORDER BY faixa_etaria""",
    "Q78": """
        SELECT cnes_estabelecimento, COUNT(DISTINCT n_aih) AS internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'
        GROUP BY cnes_estabelecimento
        ORDER BY internacoes DESC
        LIMIT 5""",
    "Q79": """
        SELECT
          CASE
            WHEN idade_paciente < 18 THEN '0-17'
            WHEN idade_paciente BETWEEN 18 AND 59 THEN '18-59'
            ELSE '60+'
          END AS faixa_etaria,
          ROUND(AVG(dias_perm), 1) AS permanencia_media
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY faixa_etaria
        ORDER BY faixa_etaria""",
    "Q80": """
        SELECT cid_principal, COUNT(DISTINCT n_aih) AS total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND icd_group = 'M00-M99'
        GROUP BY cid_principal
        ORDER BY total DESC
        LIMIT 5""",
    "Q81": """
        SELECT COUNT(DISTINCT n_aih) AS internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND dias_perm BETWEEN 3 AND 7""",
    "Q82": """
        SELECT cid_principal, COUNT(*) AS total
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND idade_paciente >= 60
        GROUP BY cid_principal
        ORDER BY total DESC
        LIMIT 5""",
    "Q83": """
        SELECT ROUND(AVG(cnt), 1) AS media_internacoes_por_estabelecimento
        FROM (
          SELECT cnes_estabelecimento, COUNT(DISTINCT n_aih) AS cnt
          FROM processed
          WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
            AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          GROUP BY cnes_estabelecimento
        ) t""",
    "Q84": """
        SELECT
          CASE
            WHEN dias_perm BETWEEN 1 AND 3 THEN '1-3 dias'
            WHEN dias_perm BETWEEN 4 AND 7 THEN '4-7 dias'
            WHEN dias_perm BETWEEN 8 AND 14 THEN '8-14 dias'
            ELSE '15+ dias'
          END AS faixa_permanencia,
          COUNT(DISTINCT n_aih) AS total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY faixa_permanencia
        ORDER BY faixa_permanencia""",
    # ── Financeira (Q85–Q100) ─────────────────────────────────────────────────
    "Q85": """
        SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total_idosos
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND idade_paciente >= 60""",
    "Q86": """
        SELECT ROUND(AVG(COALESCE(custo_total, 0)), 2) AS custo_medio_s72
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'""",
    "Q87": """
        SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total_uti
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND uti_int_to > 0""",
    "Q88": """
        SELECT sexo_paciente, ROUND(AVG(COALESCE(custo_total, 0)), 2) AS custo_medio
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY sexo_paciente
        ORDER BY custo_medio DESC""",
    "Q89": """
        SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total_criancas
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND idade_paciente < 18""",
    "Q90": """
        SELECT ROUND(AVG(COALESCE(custo_total, 0)), 2) AS custo_medio_longa_perm
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND dias_perm > 7""",
    "Q91": """
        SELECT cod_munic_estabelecimento, ROUND(SUM(custo_total), 2) AS custo_total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY cod_munic_estabelecimento
        ORDER BY custo_total DESC
        LIMIT 3""",
    "Q92": """
        SELECT ROUND(SUM(COALESCE(val_sh, 0)), 2) AS custo_hospitalar_total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q93": """
        SELECT
          CASE
            WHEN idade_paciente < 18 THEN '0-17'
            WHEN idade_paciente BETWEEN 18 AND 59 THEN '18-59'
            ELSE '60+'
          END AS faixa_etaria,
          ROUND(AVG(COALESCE(custo_total, 0)), 2) AS custo_medio
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY faixa_etaria
        ORDER BY faixa_etaria""",
    "Q94": """
        SELECT ROUND(SUM(COALESCE(val_uti, 0)), 2) AS custo_uti_total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q95": """
        SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total_m
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND icd_group = 'M00-M99'""",
    "Q96": """
        SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total_s
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND icd_group = 'S00-T98'""",
    "Q97": """
        SELECT cid_principal, COUNT(DISTINCT n_aih) AS internacoes,
          ROUND(AVG(custo_total), 2) AS custo_medio
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY cid_principal
        HAVING COUNT(DISTINCT n_aih) >= 40
        ORDER BY custo_medio DESC
        LIMIT 5""",
    "Q98": """
        SELECT ROUND(AVG(COALESCE(custo_total, 0)), 2) AS custo_medio_sia
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')""",
    "Q99": """
        SELECT cod_procedimento, ROUND(SUM(custo_total), 2) AS custo_total
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY cod_procedimento
        ORDER BY custo_total DESC
        LIMIT 5""",
    "Q100": """
        SELECT ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND idade_paciente >= 60
          AND dias_perm > 7""",
    # ── Temporal / Comparativa (Q101–Q118) ───────────────────────────────────
    "Q101": """
        SELECT mes_cmpt, COUNT(*) AS total_obitos
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND morte = 1
        GROUP BY mes_cmpt
        ORDER BY total_obitos DESC
        LIMIT 1""",
    "Q102": """
        SELECT mes_cmpt, COUNT(DISTINCT n_aih) AS internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'
          AND idade_paciente >= 60
        GROUP BY mes_cmpt
        ORDER BY mes_cmpt""",
    "Q103": """
        SELECT mes_cmpt, ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
          AND idade_paciente >= 60
        GROUP BY mes_cmpt
        ORDER BY mes_cmpt""",
    "Q104": """
        SELECT mes_cmpt, ROUND(SUM(custo_total), 2) AS custo_total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'
        GROUP BY mes_cmpt
        ORDER BY custo_total DESC
        LIMIT 1""",
    "Q105": """
        SELECT mes_cmpt, COUNT(DISTINCT n_aih) AS internacoes
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND icd_group = 'S00-T98'
        GROUP BY mes_cmpt
        ORDER BY mes_cmpt""",
    "Q106": """
        SELECT mes_cmpt, sexo_paciente, COUNT(*) AS total
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY mes_cmpt, sexo_paciente
        ORDER BY mes_cmpt, sexo_paciente""",
    "Q107": """
        SELECT
          CASE
            WHEN mes_cmpt BETWEEN 1 AND 3 THEN 'Q1'
            WHEN mes_cmpt BETWEEN 4 AND 6 THEN 'Q2'
            WHEN mes_cmpt BETWEEN 7 AND 9 THEN 'Q3'
            ELSE 'Q4'
          END AS trimestre,
          icd_group,
          COUNT(DISTINCT n_aih) AS total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY trimestre, icd_group
        ORDER BY trimestre, icd_group""",
    "Q108": """
        SELECT mes_cmpt, ROUND(AVG(dias_perm), 1) AS permanencia_media
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY mes_cmpt
        ORDER BY permanencia_media DESC
        LIMIT 1""",
    "Q109": """
        SELECT mes_cmpt,
          COUNT(DISTINCT n_aih) AS internacoes,
          SUM(CASE WHEN morte = 1 THEN 1 ELSE 0 END) AS obitos,
          ROUND(100.0 * SUM(CASE WHEN morte = 1 THEN 1 ELSE 0 END)
                / COUNT(DISTINCT n_aih), 2) AS taxa_mortalidade
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY mes_cmpt
        ORDER BY mes_cmpt""",
    "Q110": """
        SELECT
          CASE
            WHEN mes_cmpt BETWEEN 1 AND 3 THEN 'Q1'
            WHEN mes_cmpt BETWEEN 4 AND 6 THEN 'Q2'
            WHEN mes_cmpt BETWEEN 7 AND 9 THEN 'Q3'
            ELSE 'Q4'
          END AS trimestre,
          COUNT(DISTINCT n_aih) AS total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'
        GROUP BY trimestre
        ORDER BY total DESC
        LIMIT 1""",
    "Q111": """
        SELECT
          CASE WHEN mes_cmpt <= 6 THEN '1S' ELSE '2S' END AS semestre,
          COUNT(DISTINCT n_aih) AS total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY semestre
        ORDER BY semestre""",
    "Q112": """
        SELECT mes_cmpt, ROUND(AVG(COALESCE(custo_total, 0)), 2) AS custo_medio
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'
        GROUP BY mes_cmpt
        ORDER BY mes_cmpt""",
    "Q113": """
        SELECT mes_cmpt, sexo_paciente, COUNT(DISTINCT n_aih) AS total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY mes_cmpt, sexo_paciente
        ORDER BY mes_cmpt, sexo_paciente""",
    "Q114": """
        SELECT mes_cmpt, COUNT(*) AS total
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY mes_cmpt
        ORDER BY total ASC
        LIMIT 1""",
    "Q115": """
        SELECT
          CASE
            WHEN mes_cmpt BETWEEN 1 AND 3 THEN 'Q1'
            WHEN mes_cmpt BETWEEN 4 AND 6 THEN 'Q2'
            WHEN mes_cmpt BETWEEN 7 AND 9 THEN 'Q3'
            ELSE 'Q4'
          END AS trimestre,
          COUNT(*) AS total
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY trimestre
        ORDER BY trimestre""",
    "Q116": """
        SELECT mes_cmpt, ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
        FROM processed
        WHERE sistema = 'SIA' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY mes_cmpt
        ORDER BY mes_cmpt""",
    "Q117": """
        SELECT
          CASE WHEN mes_cmpt <= 6 THEN '1S' ELSE '2S' END AS semestre,
          ROUND(SUM(COALESCE(custo_total, 0)), 2) AS custo_total
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND (icd_group = 'M00-M99' OR icd_group = 'S00-T98')
        GROUP BY semestre
        ORDER BY semestre""",
    "Q118": """
        SELECT mes_cmpt, ROUND(AVG(dias_perm), 1) AS permanencia_media
        FROM processed
        WHERE sistema = 'SIH' AND uf_origem = '{state}' AND ano_cmpt = {year}
          AND cid_principal LIKE 'S72%'
        GROUP BY mes_cmpt
        ORDER BY mes_cmpt""",
}


def _to_serializable(obj):
    """Convert numpy/pandas types to native Python for JSON serialization."""
    import math
    if hasattr(obj, "item"):
        return obj.item()
    if isinstance(obj, float) and math.isnan(obj):
        return None
    return obj


def compute_gold(state: str, year: int) -> dict[str, list[dict]]:
    gold: dict[str, list[dict]] = {}
    total = len(GOLD_SQL)
    for i, (qid, sql_tmpl) in enumerate(GOLD_SQL.items(), 1):
        sql = sql_tmpl.format(state=state, year=year)
        try:
            df = duckdb_query(sql)
            rows = [
                {k: _to_serializable(v) for k, v in row.items()}
                for row in df.to_dict(orient="records")
            ]
            gold[qid] = rows
            print(f"  [{i:02d}/{total}] {qid} — {len(rows)} row(s)")
        except Exception as exc:
            print(f"  [{i:02d}/{total}] {qid} — ERROR: {exc}", file=sys.stderr)
            gold[qid] = []
    return gold


def verify_against_hardcoded(gold: dict[str, list[dict]]) -> None:
    """Spot-check SP/2022 scalar queries against the hardcoded BENCHMARK values."""
    KNOWN = {
        "Q01": ("total_internacoes", 65970),
        "Q02": ("total_procedimentos", 105016),
        "Q03": ("total_internacoes", 29356),
        "Q05": ("total_obitos", 2223),
        "Q10": ("total_internacoes", 26666),
    }
    ok = 0
    for qid, (col, expected) in KNOWN.items():
        rows = gold.get(qid, [])
        got = rows[0].get(col) if rows else None
        match = abs((got or 0) - expected) <= max(1, expected * 0.01)
        status = "OK" if match else f"MISMATCH (got {got}, expected {expected})"
        print(f"  {qid} {col}: {status}")
        if match:
            ok += 1
    print(f"\n  {ok}/{len(KNOWN)} spot-checks passed.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute benchmark gold-standard for any state/year")
    parser.add_argument("--state", default="SP",   help="State abbreviation (SP, RJ, MG...)")
    parser.add_argument("--year",  type=int, default=2022, help="Year (2022, 2023...)")
    parser.add_argument("--out",   default=None, help="Output path (default: results/gold_{state}_{year}.json)")
    parser.add_argument("--verify", action="store_true", help="Cross-check SP/2022 against hardcoded values")
    args = parser.parse_args()

    out_path = Path(args.out) if args.out else (
        Path(__file__).resolve().parent.parent / "results" / f"gold_{args.state}_{args.year}.json"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"\nComputing gold-standard: state={args.state}, year={args.year}")
    print(f"Output: {out_path}\n")

    gold = compute_gold(args.state, args.year)

    if args.verify and args.state == "SP" and args.year == 2022:
        print("\nVerifying SP/2022 against hardcoded values...")
        verify_against_hardcoded(gold)

    out_path.write_text(json.dumps(gold, ensure_ascii=False, indent=2))
    print(f"\nSaved {len(gold)} query gold-standards → {out_path}")


if __name__ == "__main__":
    main()
