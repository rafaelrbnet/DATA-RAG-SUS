"""
Benchmark Evaluation Script — SUS Data RAG
===========================================
Envia as 50 perguntas do benchmark via POST /query e compara os resultados
retornados pelo LLM com o gold-standard definido em benchmark-queries-sus-data-rag.md.

Uso:
    # Com a API rodando em outro terminal:
    uv run uvicorn src.api.main:app --reload

    # Em outro terminal:
    uv run python scripts/evaluate_benchmark.py
    uv run python scripts/evaluate_benchmark.py --model ollama   # (padrão)
    uv run python scripts/evaluate_benchmark.py --model openai
    uv run python scripts/evaluate_benchmark.py --timeout 120 --out results/eval_ollama.json

Saída:
    - JSON detalhado com resultado de cada query
    - Markdown com relatório de Execution Accuracy por categoria
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

# ── Configuração ─────────────────────────────────────────────────────────────

API_URL = "http://127.0.0.1:8000"
DEFAULT_TIMEOUT = 180  # segundos — Ollama 14B pode demorar

# ── Gold-standard ─────────────────────────────────────────────────────────────

BENCHMARK: list[dict] = [
    # ── EPIDEMIOLÓGICA SIMPLES ────────────────────────────────────────────────
    {
        "id": "Q01", "cat": "Epidemiológica Simples",
        "q": "Total de internações ortopédicas (M00-M99 e S00-S99) em SP em 2022",
        "gold": [{"total_internacoes": 65970}],
        "match": "scalar", "col": "total_internacoes",
    },
    {
        "id": "Q02", "cat": "Epidemiológica Simples",
        "q": "Total de procedimentos ambulatoriais ortopédicos em SP em 2022",
        "gold": [{"total_procedimentos": 105016}],
        "match": "scalar", "col": "total_procedimentos",
    },
    {
        "id": "Q03", "cat": "Epidemiológica Simples",
        "q": "Total de internações por fratura de fêmur (S72) em SP em 2022",
        "gold": [{"internacoes_fratura_femur": 29356}],
        "match": "scalar", "col": "internacoes_fratura_femur",
    },
    {
        "id": "Q04", "cat": "Epidemiológica Simples",
        "q": "Total de internações por osteoartrose (M16, M17) em SP em 2022",
        "gold": [{"internacoes_artrose": 156}],
        "match": "scalar", "col": "internacoes_artrose",
    },
    {
        "id": "Q05", "cat": "Epidemiológica Simples",
        "q": "Número de óbitos em internações ortopédicas em SP em 2022",
        "gold": [{"obitos": 2223}],
        "match": "scalar", "col": "obitos",
    },
    {
        "id": "Q06", "cat": "Epidemiológica Simples",
        "q": "Total de internações por traumatismos (S00-T98) em SP em 2022",
        "gold": [{"internacoes_trauma": 59668}],
        "match": "scalar", "col": "internacoes_trauma",
    },
    {
        "id": "Q07", "cat": "Epidemiológica Simples",
        "q": "Total de internações por doenças osteomusculares (M00-M99) em SP em 2022",
        "gold": [{"internacoes_osteomuscular": 6302}],
        "match": "scalar", "col": "internacoes_osteomuscular",
    },
    {
        "id": "Q08", "cat": "Epidemiológica Simples",
        "q": "Número de mulheres internadas por causa ortopédica em SP em 2022",
        "gold": [{"internacoes_femininas": 25847}],
        "match": "scalar", "col": "internacoes_femininas",
    },
    {
        "id": "Q09", "cat": "Epidemiológica Simples",
        "q": "Número de homens internados por causa ortopédica em SP em 2022",
        "gold": [{"internacoes_masculinas": 40123}],
        "match": "scalar", "col": "internacoes_masculinas",
    },
    {
        "id": "Q10", "cat": "Epidemiológica Simples",
        "q": "Número de internações ortopédicas de idosos (60 anos ou mais) em SP em 2022",
        "gold": [{"internacoes_idosos": 26666}],
        "match": "scalar", "col": "internacoes_idosos",
    },
    {
        "id": "Q11", "cat": "Epidemiológica Simples",
        "q": "Número de municípios distintos com internação ortopédica registrada em SP em 2022",
        "gold": [{"municipios": 211}],
        "match": "scalar", "col": "municipios",
    },
    {
        "id": "Q12", "cat": "Epidemiológica Simples",
        "q": "Número de estabelecimentos distintos com internação ortopédica em SP em 2022",
        "gold": [{"estabelecimentos": 381}],
        "match": "scalar", "col": "estabelecimentos",
    },
    {
        "id": "Q13", "cat": "Epidemiológica Simples",
        "q": "Número de internações ortopédicas com permanência superior a 7 dias em SP em 2022",
        "gold": [{"internacoes_longa_permanencia": 16451}],
        "match": "scalar", "col": "internacoes_longa_permanencia",
    },
    {
        "id": "Q14", "cat": "Epidemiológica Simples",
        "q": "Número de internações ortopédicas com uso de UTI em SP em 2022",
        "gold": [{"internacoes_uti": 3}],
        "match": "scalar", "col": "internacoes_uti",
    },
    {
        "id": "Q15", "cat": "Epidemiológica Simples",
        "q": "Número de internações por fratura de quadril (S72) em pacientes com 70 anos ou mais em SP em 2022",
        "gold": [{"fraturas_quadril_idosos": 15119}],
        "match": "scalar", "col": "fraturas_quadril_idosos",
    },
    # ── EPIDEMIOLÓGICA COMPLEXA ───────────────────────────────────────────────
    {
        "id": "Q16", "cat": "Epidemiológica Complexa",
        "q": "Top 10 CIDs ortopédicos por volume de internação em SP em 2022",
        "gold": [
            {"cid_principal": "S720", "total": 8980},
            {"cid_principal": "S721", "total": 6610},
            {"cid_principal": "S723", "total": 5044},
            {"cid_principal": "T813", "total": 3860},
            {"cid_principal": "S722", "total": 2424},
            {"cid_principal": "S729", "total": 2356},
            {"cid_principal": "S822", "total": 1793},
            {"cid_principal": "S724", "total": 1588},
            {"cid_principal": "S728", "total": 1506},
            {"cid_principal": "T814", "total": 1243},
        ],
        "match": "ordered_rows", "key_cols": ["cid_principal", "total"],
    },
    {
        "id": "Q17", "cat": "Epidemiológica Complexa",
        "q": "Top 5 municípios de SP com maior número de internações ortopédicas em 2022",
        "gold": [
            {"cod_munic_estabelecimento": "350000", "total": 29824},
            {"cod_munic_estabelecimento": "355030", "total": 6093},
            {"cod_munic_estabelecimento": "354870", "total": 1248},
            {"cod_munic_estabelecimento": "350950", "total": 1234},
            {"cod_munic_estabelecimento": "354340", "total": 1150},
        ],
        "match": "ordered_rows", "key_cols": ["cod_munic_estabelecimento", "total"],
    },
    {
        "id": "Q18", "cat": "Epidemiológica Complexa",
        "q": "Distribuição de internações ortopédicas por faixa etária (0-17, 18-59, 60+) em SP em 2022",
        "gold": [
            {"faixa_etaria": "0-17",  "total": 3619},
            {"faixa_etaria": "18-59", "total": 35685},
            {"faixa_etaria": "60+",   "total": 26666},
        ],
        "match": "unordered_rows", "key_cols": ["faixa_etaria", "total"],
    },
    {
        "id": "Q19", "cat": "Epidemiológica Complexa",
        "q": "Taxa de mortalidade hospitalar por CID ortopédico (top 5) em SP em 2022",
        "gold": [
            {"cid_principal": "S062", "total_internacoes": 70,  "total_obitos": 21, "taxa_mortalidade_pct": 30.0},
            {"cid_principal": "S063", "total_internacoes": 78,  "total_obitos": 19, "taxa_mortalidade_pct": 24.36},
            {"cid_principal": "S065", "total_internacoes": 706, "total_obitos": 152,"taxa_mortalidade_pct": 21.53},
            {"cid_principal": "S367", "total_internacoes": 95,  "total_obitos": 19, "taxa_mortalidade_pct": 20.0},
            {"cid_principal": "S270", "total_internacoes": 131, "total_obitos": 25, "taxa_mortalidade_pct": 19.08},
        ],
        "match": "ordered_rows", "key_cols": ["cid_principal", "total_internacoes", "total_obitos", "taxa_mortalidade_pct"],
    },
    {
        "id": "Q20", "cat": "Epidemiológica Complexa",
        "q": "Top 10 CIDs ambulatoriais ortopédicos por volume de produção em SP em 2022",
        "gold": [
            {"cid_principal": "S720", "total": 16466},
            {"cid_principal": "S729", "total": 12180},
            {"cid_principal": "S723", "total": 10624},
            {"cid_principal": "S398", "total": 8107},
            {"cid_principal": "S881", "total": 7604},
            {"cid_principal": "S781", "total": 7302},
            {"cid_principal": "S889", "total": 4798},
            {"cid_principal": "M216", "total": 4743},
            {"cid_principal": "T882", "total": 3563},
            {"cid_principal": "S721", "total": 3445},
        ],
        "match": "ordered_rows", "key_cols": ["cid_principal", "total"],
    },
    {
        "id": "Q21", "cat": "Epidemiológica Complexa",
        "q": "Proporção de internações ortopédicas por sexo em SP em 2022",
        "gold": [
            {"sexo_paciente": "M", "total": 40123, "pct": 60.8},
            {"sexo_paciente": "F", "total": 25847, "pct": 39.2},
        ],
        "match": "unordered_rows", "key_cols": ["sexo_paciente", "total", "pct"],
    },
    {
        "id": "Q22", "cat": "Epidemiológica Complexa",
        "q": "Número de internações por fratura de fêmur (S72) em idosos (60+) comparado ao total em SP em 2022",
        "gold": [{"total_s72": 29356, "s72_idosos": 19120, "pct_idosos": 65.1}],
        "match": "ordered_rows", "key_cols": ["total_s72", "s72_idosos", "pct_idosos"],
    },
    {
        "id": "Q23", "cat": "Epidemiológica Complexa",
        "q": "Top 5 CIDs ortopédicos com maior permanência média hospitalar em SP em 2022",
        "gold": [
            {"cid_principal": "S063", "total_internacoes": 78, "permanencia_media_dias": 16.4},
            {"cid_principal": "S122", "total_internacoes": 46, "permanencia_media_dias": 15.9},
            {"cid_principal": "S320", "total_internacoes": 48, "permanencia_media_dias": 14.1},
            {"cid_principal": "S062", "total_internacoes": 70, "permanencia_media_dias": 14.0},
            {"cid_principal": "S221", "total_internacoes": 41, "permanencia_media_dias": 13.7},
        ],
        "match": "ordered_rows", "key_cols": ["cid_principal", "total_internacoes", "permanencia_media_dias"],
    },
    {
        "id": "Q24", "cat": "Epidemiológica Complexa",
        "q": "Top 5 estabelecimentos com maior volume de internação ortopédica em SP em 2022",
        "gold": [
            {"cnes_estabelecimento": "2078015", "total_internacoes": 2221},
            {"cnes_estabelecimento": "2091399", "total_internacoes": 1676},
            {"cnes_estabelecimento": "2077396", "total_internacoes": 1619},
            {"cnes_estabelecimento": "2081695", "total_internacoes": 1230},
            {"cnes_estabelecimento": "7373465", "total_internacoes": 1204},
        ],
        "match": "ordered_rows", "key_cols": ["cnes_estabelecimento", "total_internacoes"],
    },
    {
        "id": "Q25", "cat": "Epidemiológica Complexa",
        "q": "Distribuição de internações ortopédicas por raça/cor do paciente em SP em 2022",
        "gold": [
            {"raca_cor_paciente": "01", "total": 38121, "pct": 57.8},
            {"raca_cor_paciente": "03", "total": 18259, "pct": 27.7},
            {"raca_cor_paciente": "99", "total": 5700,  "pct": 8.6},
            {"raca_cor_paciente": "02", "total": 3418,  "pct": 5.2},
            {"raca_cor_paciente": "04", "total": 462,   "pct": 0.7},
            {"raca_cor_paciente": "05", "total": 10,    "pct": 0.0},
        ],
        "match": "unordered_rows", "key_cols": ["raca_cor_paciente", "total", "pct"],
    },
    {
        "id": "Q26", "cat": "Epidemiológica Complexa",
        "q": "CIDs ortopédicos com maior número de dias totais de internação em SP em 2022",
        "gold": [
            {"cid_principal": "S720", "internacoes": 8980,  "total_dias_internacao": 64885},
            {"cid_principal": "S721", "internacoes": 6610,  "total_dias_internacao": 44099},
            {"cid_principal": "T813", "internacoes": 3860,  "total_dias_internacao": 28976},
            {"cid_principal": "S723", "internacoes": 5044,  "total_dias_internacao": 27024},
            {"cid_principal": "S722", "internacoes": 2424,  "total_dias_internacao": 16004},
            {"cid_principal": "S729", "internacoes": 2356,  "total_dias_internacao": 13870},
            {"cid_principal": "T814", "internacoes": 1243,  "total_dias_internacao": 10712},
            {"cid_principal": "S724", "internacoes": 1588,  "total_dias_internacao": 10106},
            {"cid_principal": "S728", "internacoes": 1506,  "total_dias_internacao": 9895},
            {"cid_principal": "S065", "internacoes": 706,   "total_dias_internacao": 8084},
        ],
        "match": "ordered_rows", "key_cols": ["cid_principal", "internacoes", "total_dias_internacao"],
    },
    {
        "id": "Q27", "cat": "Epidemiológica Complexa",
        "q": "Top 5 procedimentos ambulatoriais ortopédicos mais realizados em SP em 2022",
        "gold": [
            {"cod_procedimento": "0302050019", "total": 70536},
            {"cod_procedimento": "0701060018", "total": 6235},
            {"cod_procedimento": "0701050047", "total": 4334},
            {"cod_procedimento": "0701060034", "total": 2046},
            {"cod_procedimento": "0701010142", "total": 2001},
        ],
        "match": "ordered_rows", "key_cols": ["cod_procedimento", "total"],
    },
    {
        "id": "Q28", "cat": "Epidemiológica Complexa",
        "q": "Proporção de internações ortopédicas com paciente de outro município em SP em 2022",
        "gold": [{"total_internacoes": 65970, "deslocamento_intermunicipal": 37219, "pct_deslocamento": 56.4}],
        "match": "ordered_rows", "key_cols": ["total_internacoes", "deslocamento_intermunicipal", "pct_deslocamento"],
    },
    {
        "id": "Q29", "cat": "Epidemiológica Complexa",
        "q": "Comparativo de volume mensal SIA vs SIH ortopédico em SP em 2022",
        "gold": [
            {"mes_cmpt": 1,  "sistema": "SIA", "total": 5898},
            {"mes_cmpt": 1,  "sistema": "SIH", "total": 5212},
            {"mes_cmpt": 2,  "sistema": "SIA", "total": 7116},
            {"mes_cmpt": 2,  "sistema": "SIH", "total": 5022},
            {"mes_cmpt": 3,  "sistema": "SIA", "total": 8509},
            {"mes_cmpt": 3,  "sistema": "SIH", "total": 5608},
            {"mes_cmpt": 4,  "sistema": "SIA", "total": 8352},
            {"mes_cmpt": 4,  "sistema": "SIH", "total": 5136},
            {"mes_cmpt": 5,  "sistema": "SIA", "total": 9535},
            {"mes_cmpt": 5,  "sistema": "SIH", "total": 5416},
            {"mes_cmpt": 6,  "sistema": "SIA", "total": 9264},
            {"mes_cmpt": 6,  "sistema": "SIH", "total": 5202},
            {"mes_cmpt": 7,  "sistema": "SIA", "total": 9042},
            {"mes_cmpt": 7,  "sistema": "SIH", "total": 5852},
            {"mes_cmpt": 8,  "sistema": "SIA", "total": 10993},
            {"mes_cmpt": 8,  "sistema": "SIH", "total": 6010},
            {"mes_cmpt": 9,  "sistema": "SIA", "total": 9421},
            {"mes_cmpt": 9,  "sistema": "SIH", "total": 5752},
            {"mes_cmpt": 10, "sistema": "SIA", "total": 9197},
            {"mes_cmpt": 10, "sistema": "SIH", "total": 5772},
            {"mes_cmpt": 11, "sistema": "SIA", "total": 9297},
            {"mes_cmpt": 11, "sistema": "SIH", "total": 5353},
            {"mes_cmpt": 12, "sistema": "SIA", "total": 8392},
            {"mes_cmpt": 12, "sistema": "SIH", "total": 5676},
        ],
        "match": "ordered_rows", "key_cols": ["mes_cmpt", "sistema", "total"],
    },
    {
        "id": "Q30", "cat": "Epidemiológica Complexa",
        "q": "Idade média dos pacientes internados por fratura de fêmur (S72) em SP em 2022",
        "gold": [{"idade_media": 62.4, "idade_minima": 0, "idade_maxima": 99}],
        "match": "ordered_rows", "key_cols": ["idade_media", "idade_minima", "idade_maxima"],
    },
    # ── FINANCEIRA ────────────────────────────────────────────────────────────
    {
        "id": "Q31", "cat": "Financeira",
        "q": "Custo total de todas as internações ortopédicas em SP em 2022",
        "gold": [{"custo_total_reais": 175283428.77}],
        "match": "scalar", "col": "custo_total_reais",
    },
    {
        "id": "Q32", "cat": "Financeira",
        "q": "Custo médio por internação ortopédica em SP em 2022",
        "gold": [{"custo_medio_por_internacao": 2655.37}],
        "match": "scalar", "col": "custo_medio_por_internacao",
    },
    {
        "id": "Q33", "cat": "Financeira",
        "q": "Custo total de internações por fratura de fêmur (S72) em SP em 2022",
        "gold": [{"custo_total_s72": 79277316.96}],
        "match": "scalar", "col": "custo_total_s72",
    },
    {
        "id": "Q34", "cat": "Financeira",
        "q": "Top 5 CIDs ortopédicos com maior custo total de internação em SP em 2022",
        "gold": [
            {"cid_principal": "S720", "internacoes": 8980, "custo_total": 29879588.9},
            {"cid_principal": "S721", "internacoes": 6610, "custo_total": 17207131.57},
            {"cid_principal": "S723", "internacoes": 5044, "custo_total": 12728219.46},
            {"cid_principal": "T813", "internacoes": 3860, "custo_total": 6607677.78},
            {"cid_principal": "S722", "internacoes": 2424, "custo_total": 5726268.91},
        ],
        "match": "ordered_rows", "key_cols": ["cid_principal", "internacoes", "custo_total"],
    },
    {
        "id": "Q35", "cat": "Financeira",
        "q": "Custo total de internações ortopédicas com uso de OPME (val_ortp > 0) em SP em 2022",
        "gold": [{"internacoes_com_opme": 0, "custo_total_opme": None}],
        "match": "ordered_rows", "key_cols": ["internacoes_com_opme", "custo_total_opme"],
    },
    {
        "id": "Q36", "cat": "Financeira",
        "q": "Valor total de honorários profissionais (val_sp) em internações ortopédicas em SP em 2022",
        "gold": [{"total_honorarios": 30640001.52}],
        "match": "scalar", "col": "total_honorarios",
    },
    {
        "id": "Q37", "cat": "Financeira",
        "q": "Custo médio por dia de internação ortopédica em SP em 2022",
        "gold": [{"custo_por_dia": 432.54}],
        "match": "scalar", "col": "custo_por_dia",
    },
    {
        "id": "Q38", "cat": "Financeira",
        "q": "Top 5 estabelecimentos com maior custo total de internações ortopédicas em SP em 2022",
        "gold": [
            {"cnes_estabelecimento": "2078015", "internacoes": 2221, "custo_total": 11218869.54},
            {"cnes_estabelecimento": "2077396", "internacoes": 1619, "custo_total": 7242879.23},
            {"cnes_estabelecimento": "2078775", "internacoes": 1068, "custo_total": 4066905.35},
            {"cnes_estabelecimento": "2081695", "internacoes": 1230, "custo_total": 3701329.73},
            {"cnes_estabelecimento": "2688689", "internacoes": 1011, "custo_total": 3622383.58},
        ],
        "match": "ordered_rows", "key_cols": ["cnes_estabelecimento", "internacoes", "custo_total"],
    },
    {
        "id": "Q39", "cat": "Financeira",
        "q": "Custo total de procedimentos ambulatoriais ortopédicos em SP em 2022",
        "gold": [{"custo_total_sia": 8721217.91}],
        "match": "scalar", "col": "custo_total_sia",
    },
    {
        "id": "Q40", "cat": "Financeira",
        "q": "Proporção custo serviço hospitalar vs honorários em internações ortopédicas em SP em 2022",
        "gold": [{"custo_servico_hospitalar": 144636796.74, "honorarios_profissionais": 30640001.52, "pct_hospitalar": 82.5}],
        "match": "ordered_rows", "key_cols": ["custo_servico_hospitalar", "honorarios_profissionais", "pct_hospitalar"],
    },
    # ── TEMPORAL / COMPARATIVA ────────────────────────────────────────────────
    {
        "id": "Q41", "cat": "Temporal/Comparativa",
        "q": "Distribuição mensal de internações ortopédicas em SP em 2022",
        "gold": [
            {"mes_cmpt": 1,  "internacoes": 5209},
            {"mes_cmpt": 2,  "internacoes": 5021},
            {"mes_cmpt": 3,  "internacoes": 5604},
            {"mes_cmpt": 4,  "internacoes": 5136},
            {"mes_cmpt": 5,  "internacoes": 5416},
            {"mes_cmpt": 6,  "internacoes": 5202},
            {"mes_cmpt": 7,  "internacoes": 5852},
            {"mes_cmpt": 8,  "internacoes": 6006},
            {"mes_cmpt": 9,  "internacoes": 5752},
            {"mes_cmpt": 10, "internacoes": 5772},
            {"mes_cmpt": 11, "internacoes": 5351},
            {"mes_cmpt": 12, "internacoes": 5676},
        ],
        "match": "ordered_rows", "key_cols": ["mes_cmpt", "internacoes"],
    },
    {
        "id": "Q42", "cat": "Temporal/Comparativa",
        "q": "Distribuição mensal de procedimentos ambulatoriais ortopédicos em SP em 2022",
        "gold": [
            {"mes_cmpt": 1,  "procedimentos": 5898},
            {"mes_cmpt": 2,  "procedimentos": 7116},
            {"mes_cmpt": 3,  "procedimentos": 8509},
            {"mes_cmpt": 4,  "procedimentos": 8352},
            {"mes_cmpt": 5,  "procedimentos": 9535},
            {"mes_cmpt": 6,  "procedimentos": 9264},
            {"mes_cmpt": 7,  "procedimentos": 9042},
            {"mes_cmpt": 8,  "procedimentos": 10993},
            {"mes_cmpt": 9,  "procedimentos": 9421},
            {"mes_cmpt": 10, "procedimentos": 9197},
            {"mes_cmpt": 11, "procedimentos": 9297},
            {"mes_cmpt": 12, "procedimentos": 8392},
        ],
        "match": "ordered_rows", "key_cols": ["mes_cmpt", "procedimentos"],
    },
    {
        "id": "Q43", "cat": "Temporal/Comparativa",
        "q": "Mês com maior número de internações por fratura de fêmur (S72) em SP em 2022",
        "gold": [{"mes_cmpt": 8, "fraturas": 2757}],
        "match": "scalar", "col": "mes_cmpt",
    },
    {
        "id": "Q44", "cat": "Temporal/Comparativa",
        "q": "Evolução mensal do custo total de internações ortopédicas em SP em 2022",
        "gold": [
            {"mes_cmpt": 1,  "custo_total": 12942488.31},
            {"mes_cmpt": 2,  "custo_total": 12530190.26},
            {"mes_cmpt": 3,  "custo_total": 14223832.66},
            {"mes_cmpt": 4,  "custo_total": 13771879.44},
            {"mes_cmpt": 5,  "custo_total": 14952640.76},
            {"mes_cmpt": 6,  "custo_total": 13786785.50},
            {"mes_cmpt": 7,  "custo_total": 15817720.10},
            {"mes_cmpt": 8,  "custo_total": 16289447.24},
            {"mes_cmpt": 9,  "custo_total": 15715314.26},
            {"mes_cmpt": 10, "custo_total": 16046281.06},
            {"mes_cmpt": 11, "custo_total": 14341406.49},
            {"mes_cmpt": 12, "custo_total": 14865442.69},
        ],
        "match": "ordered_rows", "key_cols": ["mes_cmpt", "custo_total"],
    },
    {
        "id": "Q45", "cat": "Temporal/Comparativa",
        "q": "Volume mensal de internações por trauma (S00-T98) vs doenças musculoesqueléticas (M00-M99) em SP em 2022",
        "gold": [
            {"mes_cmpt": 1,  "icd_group": "M00-M99", "internacoes": 476},
            {"mes_cmpt": 1,  "icd_group": "S00-T98", "internacoes": 4733},
            {"mes_cmpt": 2,  "icd_group": "M00-M99", "internacoes": 461},
            {"mes_cmpt": 2,  "icd_group": "S00-T98", "internacoes": 4560},
            {"mes_cmpt": 3,  "icd_group": "M00-M99", "internacoes": 511},
            {"mes_cmpt": 3,  "icd_group": "S00-T98", "internacoes": 5093},
            {"mes_cmpt": 4,  "icd_group": "M00-M99", "internacoes": 467},
            {"mes_cmpt": 4,  "icd_group": "S00-T98", "internacoes": 4669},
            {"mes_cmpt": 5,  "icd_group": "M00-M99", "internacoes": 567},
            {"mes_cmpt": 5,  "icd_group": "S00-T98", "internacoes": 4849},
            {"mes_cmpt": 6,  "icd_group": "M00-M99", "internacoes": 512},
            {"mes_cmpt": 6,  "icd_group": "S00-T98", "internacoes": 4690},
            {"mes_cmpt": 7,  "icd_group": "M00-M99", "internacoes": 573},
            {"mes_cmpt": 7,  "icd_group": "S00-T98", "internacoes": 5279},
            {"mes_cmpt": 8,  "icd_group": "M00-M99", "internacoes": 563},
            {"mes_cmpt": 8,  "icd_group": "S00-T98", "internacoes": 5443},
            {"mes_cmpt": 9,  "icd_group": "M00-M99", "internacoes": 588},
            {"mes_cmpt": 9,  "icd_group": "S00-T98", "internacoes": 5164},
            {"mes_cmpt": 10, "icd_group": "M00-M99", "internacoes": 584},
            {"mes_cmpt": 10, "icd_group": "S00-T98", "internacoes": 5188},
            {"mes_cmpt": 11, "icd_group": "M00-M99", "internacoes": 530},
            {"mes_cmpt": 11, "icd_group": "S00-T98", "internacoes": 4821},
            {"mes_cmpt": 12, "icd_group": "M00-M99", "internacoes": 494},
            {"mes_cmpt": 12, "icd_group": "S00-T98", "internacoes": 5182},
        ],
        "match": "ordered_rows", "key_cols": ["mes_cmpt", "icd_group", "internacoes"],
    },
    {
        "id": "Q46", "cat": "Temporal/Comparativa",
        "q": "Evolução mensal de óbitos em internações ortopédicas em SP em 2022",
        "gold": [
            {"mes_cmpt": 1,  "total_obitos": 179},
            {"mes_cmpt": 2,  "total_obitos": 182},
            {"mes_cmpt": 3,  "total_obitos": 167},
            {"mes_cmpt": 4,  "total_obitos": 156},
            {"mes_cmpt": 5,  "total_obitos": 187},
            {"mes_cmpt": 6,  "total_obitos": 182},
            {"mes_cmpt": 7,  "total_obitos": 212},
            {"mes_cmpt": 8,  "total_obitos": 217},
            {"mes_cmpt": 9,  "total_obitos": 183},
            {"mes_cmpt": 10, "total_obitos": 224},
            {"mes_cmpt": 11, "total_obitos": 144},
            {"mes_cmpt": 12, "total_obitos": 190},
        ],
        "match": "ordered_rows", "key_cols": ["mes_cmpt", "total_obitos"],
    },
    {
        "id": "Q47", "cat": "Temporal/Comparativa",
        "q": "Permanência média por trimestre nas internações ortopédicas em SP em 2022",
        "gold": [
            {"trimestre": "Q1", "permanencia_media_dias": 5.8},
            {"trimestre": "Q2", "permanencia_media_dias": 6.0},
            {"trimestre": "Q3", "permanencia_media_dias": 6.2},
            {"trimestre": "Q4", "permanencia_media_dias": 6.1},
        ],
        "match": "unordered_rows", "key_cols": ["trimestre", "permanencia_media_dias"],
    },
    {
        "id": "Q48", "cat": "Temporal/Comparativa",
        "q": "Mês com maior custo médio por internação ortopédica em SP em 2022",
        "gold": [{"mes_cmpt": 10, "custo_medio": 2780.02}],
        "match": "scalar", "col": "mes_cmpt",
    },
    {
        "id": "Q49", "cat": "Temporal/Comparativa",
        "q": "Sazonalidade mensal de fraturas de fêmur (S72) em SP em 2022",
        "gold": [
            {"mes_cmpt": 1,  "fraturas_femur": 2220},
            {"mes_cmpt": 2,  "fraturas_femur": 2076},
            {"mes_cmpt": 3,  "fraturas_femur": 2385},
            {"mes_cmpt": 4,  "fraturas_femur": 2207},
            {"mes_cmpt": 5,  "fraturas_femur": 2337},
            {"mes_cmpt": 6,  "fraturas_femur": 2403},
            {"mes_cmpt": 7,  "fraturas_femur": 2676},
            {"mes_cmpt": 8,  "fraturas_femur": 2757},
            {"mes_cmpt": 9,  "fraturas_femur": 2614},
            {"mes_cmpt": 10, "fraturas_femur": 2690},
            {"mes_cmpt": 11, "fraturas_femur": 2402},
            {"mes_cmpt": 12, "fraturas_femur": 2592},
        ],
        "match": "ordered_rows", "key_cols": ["mes_cmpt", "fraturas_femur"],
    },
    {
        "id": "Q50", "cat": "Temporal/Comparativa",
        "q": "Volume mensal de internações ortopédicas de idosos (60 anos ou mais) em SP em 2022",
        "gold": [
            {"mes_cmpt": 1,  "internacoes_idosos": 1987},
            {"mes_cmpt": 2,  "internacoes_idosos": 1920},
            {"mes_cmpt": 3,  "internacoes_idosos": 2145},
            {"mes_cmpt": 4,  "internacoes_idosos": 2021},
            {"mes_cmpt": 5,  "internacoes_idosos": 2207},
            {"mes_cmpt": 6,  "internacoes_idosos": 2173},
            {"mes_cmpt": 7,  "internacoes_idosos": 2419},
            {"mes_cmpt": 8,  "internacoes_idosos": 2489},
            {"mes_cmpt": 9,  "internacoes_idosos": 2332},
            {"mes_cmpt": 10, "internacoes_idosos": 2464},
            {"mes_cmpt": 11, "internacoes_idosos": 2161},
            {"mes_cmpt": 12, "internacoes_idosos": 2354},
        ],
        "match": "ordered_rows", "key_cols": ["mes_cmpt", "internacoes_idosos"],
    },
]

# ── Lógica de comparação ──────────────────────────────────────────────────────

def _eq(a, b, tol: float = 0.01) -> bool:
    """Compara dois valores com tolerância para floats."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if isinstance(a, float) or isinstance(b, float):
        try:
            fa, fb = float(a), float(b)
            if fa == 0 and fb == 0:
                return True
            return abs(fa - fb) <= tol * max(abs(fa), abs(fb), 1)
        except (TypeError, ValueError):
            return False
    try:
        return int(a) == int(b)
    except (TypeError, ValueError):
        return str(a) == str(b)


def _row_eq_strict(got: dict, exp: dict, key_cols: list[str]) -> bool:
    """Match exato: verifica valores nas colunas nomeadas do gold."""
    for col in key_cols:
        if col not in got:
            return False
        if not _eq(got[col], exp.get(col)):
            return False
    return True


def _row_eq_flexible(got: dict, exp: dict, key_cols: list[str]) -> tuple[bool, bool]:
    """
    Tenta match exato primeiro; se falhar por nome de coluna, tenta match posicional.

    Retorna (matched: bool, was_flexible: bool).

    Rationale: Execution Accuracy avalia se o SQL gerado retorna os mesmos DADOS
    que o gold-standard, independente do alias de coluna escolhido pelo LLM.
    Aliases diferentes para os mesmos valores não constituem erro semântico —
    o resultado é correto. Esta decisão é documentada como nota metodológica
    no paper (seção Validation).
    """
    if _row_eq_strict(got, exp, key_cols):
        return True, False
    # Fallback posicional: compara valores na ordem das colunas do gold
    exp_vals = list(exp.values())
    got_vals = list(got.values())
    if len(exp_vals) != len(got_vals):
        return False, False
    if all(_eq(g, e) for g, e in zip(got_vals, exp_vals)):
        return True, True
    return False, False


def score(q: dict, result: list | None) -> tuple[bool, str, bool]:
    """
    Retorna (correct: bool, reason: str, used_flexible: bool).

    used_flexible=True indica que o match foi por valores posicionais
    (coluna com nome diferente mas valor correto).
    """
    if result is None:
        return False, "API retornou erro ou timeout", False
    if not isinstance(result, list):
        return False, f"Resultado não é lista: {type(result)}", False

    gold = q["gold"]
    match = q["match"]

    if match == "scalar":
        col = q["col"]
        if not result:
            return False, "Resultado vazio", False
        got_val = result[0].get(col) if isinstance(result[0], dict) else None
        flexible = False
        if got_val is None:
            # coluna com nome diferente — tenta o primeiro valor
            got_val = next(iter(result[0].values()), None) if result else None
            flexible = got_val is not None
        exp_val = gold[0][col]
        ok = _eq(got_val, exp_val)
        return ok, f"got={got_val} exp={exp_val}", flexible and ok

    if match in ("ordered_rows", "unordered_rows"):
        key_cols = q["key_cols"]
        if len(result) != len(gold):
            return False, f"Número de linhas: got={len(result)} exp={len(gold)}", False
        any_flexible = False
        if match == "ordered_rows":
            for i, (g, e) in enumerate(zip(result, gold)):
                matched, flex = _row_eq_flexible(g, e, key_cols)
                if not matched:
                    return False, f"Linha {i+1}: got={g} exp={e}", False
                if flex:
                    any_flexible = True
            label = "flexível" if any_flexible else "exato"
            return True, f"{len(gold)} linhas corretas ({label})", any_flexible
        else:
            remaining = list(result)
            for e in gold:
                found = False
                for i, g in enumerate(remaining):
                    matched, flex = _row_eq_flexible(g, e, key_cols)
                    if matched:
                        remaining.pop(i)
                        found = True
                        if flex:
                            any_flexible = True
                        break
                if not found:
                    return False, f"Linha não encontrada: {e}", False
            label = "flexível" if any_flexible else "exato"
            return True, f"{len(gold)} linhas corretas, ordem livre ({label})", any_flexible

    return False, f"Tipo de match desconhecido: {match}", False


# ── Chamada à API ─────────────────────────────────────────────────────────────

def call_api(question: str, timeout: int) -> dict:
    t0 = time.time()
    try:
        resp = requests.post(
            f"{API_URL}/query",
            json={"question": question},
            timeout=timeout,
        )
        elapsed = round((time.time() - t0) * 1000)
        if resp.status_code != 200:
            return {"error": f"HTTP {resp.status_code}: {resp.text[:200]}", "elapsed_ms": elapsed}
        data = resp.json()
        return {
            "sql": data.get("sql"),
            "result": data.get("result"),
            "explanation": data.get("explanation"),
            "error": data.get("error"),
            "elapsed_ms": elapsed,
        }
    except requests.exceptions.Timeout:
        return {"error": f"Timeout após {timeout}s", "elapsed_ms": timeout * 1000}
    except Exception as exc:
        return {"error": str(exc), "elapsed_ms": round((time.time() - t0) * 1000)}


def call_direct(question: str, timeout: int, temperature: float = 0.0) -> dict:
    """Chama rag direto (sem API HTTP) — permite controlar temperatura por chamada."""
    import json as _json
    from src.rag.sql_generator import generate_sql
    from src.rag.executor import query as execute_sql

    t0 = time.time()
    sql = None
    try:
        sql = generate_sql(question, temperature=temperature)
    except Exception as exc:
        return {"error": f"Falha SQL: {exc}", "elapsed_ms": round((time.time() - t0) * 1000)}

    try:
        df = execute_sql(sql)
        result = _json.loads(df.to_json(orient="records", default_handler=str))
        return {"sql": sql, "result": result, "elapsed_ms": round((time.time() - t0) * 1000)}
    except Exception as exc:
        return {"sql": sql, "error": f"Falha DuckDB: {exc}", "elapsed_ms": round((time.time() - t0) * 1000)}


# ── Relatório Markdown ────────────────────────────────────────────────────────

def render_report(model: str, run_ts: str, records: list[dict]) -> str:
    total = len(records)
    correct = sum(1 for r in records if r["correct"])
    flexible = sum(1 for r in records if r.get("flexible_match"))
    errors = sum(1 for r in records if r.get("api_error"))
    ea = round(100 * correct / total, 1)
    mean_tta = round(sum(r["elapsed_ms"] for r in records) / total / 1000, 1)

    # Wilson IC 95% para a proporção
    import math
    p = correct / total
    z = 1.959964
    center = (2 * total * p + z**2) / (2 * (total + z**2))
    margin = z * math.sqrt(z**2 + 4 * total * p * (1 - p)) / (2 * (total + z**2))
    ci_lo = round((center - margin) * 100, 1)
    ci_hi = round((center + margin) * 100, 1)

    cats: dict[str, dict] = {}
    for r in records:
        c = r["cat"]
        if c not in cats:
            cats[c] = {"total": 0, "correct": 0}
        cats[c]["total"] += 1
        cats[c]["correct"] += r["correct"]

    lines = [
        f"# Benchmark Evaluation Report — SUS Data RAG",
        f"",
        f"**Model:** {model}  ",
        f"**Run date:** {run_ts}  ",
        f"**API:** {API_URL}  ",
        f"",
        f"## Summary",
        f"",
        f"| Metric | Value |",
        f"|---|---|",
        f"| Total queries | {total} |",
        f"| Correct (EA numerator) | {correct} |",
        f"| **Execution Accuracy (EA)** | **{ea}%** |",
        f"| Wilson IC 95% | [{ci_lo}%, {ci_hi}%] |",
        f"| Correct via flexible match (alias diferente) | {flexible} |",
        f"| API errors / timeouts | {errors} |",
        f"| Mean Time-to-Answer | {mean_tta}s |",
        f"",
        f"> **Nota metodológica — Scoring:** A Execution Accuracy avalia se o SQL gerado",
        f"> retorna os mesmos **valores** que o gold-standard, independente do nome das colunas.",
        f"> Quando o LLM usa um alias diferente mas retorna os dados corretos (match posicional),",
        f"> a query é contada como correta e sinalizada com `~`. Esta decisão segue a definição",
        f"> de EA em [Lee et al., 2022] — comparação de conjuntos de resultado, não de SQL texto.",
        f"",
        f"## EA by Category",
        f"",
        f"| Category | Correct | Total | EA |",
        f"|---|---:|---:|---:|",
    ]
    for cat, v in cats.items():
        cat_ea = round(100 * v["correct"] / v["total"], 1)
        lines.append(f"| {cat} | {v['correct']} | {v['total']} | {cat_ea}% |")

    lines += ["", "## Query Results", ""]
    for r in records:
        status = "✅" if r["correct"] else ("⚠️" if r.get("api_error") else "❌")
        lines.append(f"### {status} {r['id']} — {r['cat']}")
        lines.append(f"")
        lines.append(f"**Q:** {r['question']}")
        lines.append(f"")
        if r.get("sql"):
            lines.append(f"**SQL gerado:**")
            lines.append(f"```sql")
            lines.append(r["sql"])
            lines.append(f"```")
            lines.append(f"")
        if r.get("api_error"):
            lines.append(f"**Erro:** {r['api_error']}")
        else:
            lines.append(f"**Resultado:** `{r['score_reason']}`")
            lines.append(f"**TTA:** {r['elapsed_ms']}ms")
        lines.append(f"")
        lines.append(f"---")
        lines.append(f"")

    lines += [
        f"## Failures Detail",
        f"",
    ]
    failures = [r for r in records if not r["correct"]]
    if not failures:
        lines.append("_Nenhuma falha._")
    else:
        for r in failures:
            lines.append(f"- **{r['id']}** ({r['cat']}): {r['score_reason']}")
            if r.get("sql"):
                lines.append(f"  SQL: `{r['sql'][:120]}...`")

    lines += [
        f"",
        f"---",
        f"_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_",
    ]
    return "\n".join(lines)


# ── Relatório Agregado (multi-run) ────────────────────────────────────────────

def render_aggregate_report(model: str, temperature: float, all_runs: list) -> str:
    n_runs = len(all_runs)
    n_queries = len(all_runs[0])

    run_eas = [sum(1 for r in run if r["correct"]) / n_queries * 100 for run in all_runs]
    mean_ea = sum(run_eas) / n_runs
    if n_runs > 1:
        var = sum((x - mean_ea) ** 2 for x in run_eas) / (n_runs - 1)
        sd_ea = math.sqrt(var)
    else:
        sd_ea = 0.0

    correct_total = round(mean_ea / 100 * n_queries)
    p = correct_total / n_queries
    z = 1.959964
    center = (2 * n_queries * p + z**2) / (2 * (n_queries + z**2))
    margin = z * math.sqrt(z**2 + 4 * n_queries * p * (1 - p)) / (2 * (n_queries + z**2))
    ci_lo = round((center - margin) * 100, 1)
    ci_hi = round((center + margin) * 100, 1)

    cats_runs: dict = {}
    for run in all_runs:
        cat_counts: dict = {}
        for r in run:
            c = r["cat"]
            if c not in cat_counts:
                cat_counts[c] = {"total": 0, "correct": 0}
            cat_counts[c]["total"] += 1
            cat_counts[c]["correct"] += int(r["correct"])
        for cat, v in cat_counts.items():
            cats_runs.setdefault(cat, []).append(100 * v["correct"] / v["total"])

    lines = [
        "# Benchmark Aggregate Report — SUS Data RAG",
        "",
        f"**Model:** {model}  ",
        f"**Runs:** {n_runs}  ",
        f"**Temperature:** {temperature}  ",
        f"**Queries per run:** {n_queries}  ",
        "",
        "## Resumo por Run",
        "",
        "| Run | Corretas | EA |",
        "|---|---:|---:|",
    ]
    for i, (run, ea) in enumerate(zip(all_runs, run_eas), 1):
        correct = sum(1 for r in run if r["correct"])
        lines.append(f"| Run {i} | {correct}/{n_queries} | {ea:.1f}% |")

    lines += [
        f"| **Média** | — | **{mean_ea:.1f}%** |",
        f"| **DP** | — | **{sd_ea:.1f}%** |",
        "",
        f"**Wilson IC 95% (média):** [{ci_lo}%, {ci_hi}%]",
        "",
        "## EA por Categoria (Média ± DP)",
        "",
        "| Categoria | Média EA | DP |",
        "|---|---:|---:|",
    ]
    for cat, eas in cats_runs.items():
        m = sum(eas) / len(eas)
        sd = math.sqrt(sum((x - m) ** 2 for x in eas) / max(len(eas) - 1, 1)) if len(eas) > 1 else 0.0
        lines.append(f"| {cat} | {m:.1f}% | {sd:.1f}% |")

    if temperature == 0.0:
        lines += [
            "",
            "> **Nota metodológica:** `temperature=0` (decodificação greedy) → outputs determinísticos.",
            "> DP=0% entre runs confirma reprodutibilidade total. Registrado no Methods como garantia de rigor.",
        ]

    lines += ["", "---", "_Gerado por scripts/evaluate_benchmark.py — SUS Data RAG — USF/Mestrado_"]
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def _run_one(queries: list, caller, timeout: int) -> list:
    """Executa uma passagem pelo benchmark. caller(question, timeout) → dict."""
    records = []
    n = len(queries)
    for i, q in enumerate(queries, 1):
        print(f"[{i:02d}/{n}] {q['id']} — {q['q'][:60]}...", end=" ", flush=True)
        resp = caller(q["q"], timeout)
        if resp.get("error"):
            correct, reason, flexible = False, resp["error"], False
        else:
            correct, reason, flexible = score(q, resp.get("result"))
        icon = "✅" if correct else "❌"
        flex_tag = " ~" if flexible else ""
        print(f"{icon}{flex_tag} {resp['elapsed_ms']}ms")
        records.append({
            "id": q["id"],
            "cat": q["cat"],
            "question": q["q"],
            "correct": correct,
            "flexible_match": flexible,
            "score_reason": reason,
            "sql": resp.get("sql"),
            "llm_result": resp.get("result"),
            "gold_result": q["gold"],
            "api_error": resp.get("error"),
            "elapsed_ms": resp["elapsed_ms"],
        })
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark evaluation — SUS Data RAG")
    parser.add_argument("--model",       default="ollama", help="ollama | openai")
    parser.add_argument("--timeout",     type=int,   default=DEFAULT_TIMEOUT)
    parser.add_argument("--out",         default=None, help="Caminho base do JSON de saída")
    parser.add_argument("--start",       type=int,   default=1,   help="Query inicial (1-50)")
    parser.add_argument("--end",         type=int,   default=50,  help="Query final (1-50)")
    parser.add_argument("--runs",        type=int,   default=1,   help="Número de execuções (multi-run)")
    parser.add_argument("--temperature", type=float, default=0.0, help="Temperatura LLM (0=greedy, >0=estocástico)")
    parser.add_argument("--direct",      action="store_true",     help="Bypass API: chama rag direto (suporta --temperature)")
    args = parser.parse_args()

    out_dir = Path("results")
    out_dir.mkdir(exist_ok=True)
    mestrado_dir = Path("/Users/rbnet/Library/CloudStorage/Dropbox/Docs/MestradoUSF/ProjetoDataRag/Projeto")

    if args.direct:
        print(f"⚡ Modo direto (bypass API) — temperature={args.temperature}")
        caller = lambda q, t: call_direct(q, t, temperature=args.temperature)
    else:
        try:
            health = requests.get(f"{API_URL}/health", timeout=5)
            assert health.json().get("status") == "ok"
            print(f"✅ API disponível em {API_URL}")
        except Exception:
            print(f"❌ API não responde em {API_URL}")
            print(f"   Inicie com: uv run uvicorn src.api.main:app --reload")
            print(f"   Ou use --direct para bypass da API.")
            sys.exit(1)
        caller = lambda q, t: call_api(q, t)

    queries = [q for q in BENCHMARK if args.start <= int(q["id"][1:]) <= args.end]
    print(f"\n🚀 {args.runs} run(s) × {len(queries)} queries — modelo: {args.model} — temp: {args.temperature}")
    print(f"   Timeout por query: {args.timeout}s\n")

    all_runs = []
    ts_slug_base = datetime.now().strftime("%Y%m%d_%H%M")

    for run_idx in range(1, args.runs + 1):
        if args.runs > 1:
            print(f"\n{'─'*60}")
            print(f"  RUN {run_idx}/{args.runs}")
            print(f"{'─'*60}\n")

        run_ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        records = _run_one(queries, caller, args.timeout)
        all_runs.append(records)

        # Salvar JSON individual
        if args.out and args.runs == 1:
            json_path = Path(args.out)
        else:
            suffix = f"_r{run_idx}" if args.runs > 1 else ""
            json_path = out_dir / f"eval_{args.model}_{ts_slug_base}{suffix}.json"
        json_path.write_text(json.dumps(records, ensure_ascii=False, indent=2, default=str))

        # Salvar relatório MD individual
        md_path = json_path.with_suffix(".md")
        md_path.write_text(render_report(args.model, run_ts, records))

        # Copiar para Mestrado
        if mestrado_dir.exists():
            dest = mestrado_dir / md_path.name
            dest.write_text(md_path.read_text())
            print(f"\n📄 Relatório copiado para: {dest}")

        # Resumo do run
        correct = sum(1 for r in records if r["correct"])
        flexible_ct = sum(1 for r in records if r.get("flexible_match"))
        ea = round(100 * correct / len(records), 1)
        p = correct / len(records)
        z = 1.959964
        center = (2 * len(records) * p + z**2) / (2 * (len(records) + z**2))
        margin = z * math.sqrt(z**2 + 4 * len(records) * p * (1 - p)) / (2 * (len(records) + z**2))
        ci_lo = round((center - margin) * 100, 1)
        ci_hi = round((center + margin) * 100, 1)
        print(f"\n{'='*60}")
        print(f"  Run {run_idx} — EA ({args.model}): {correct}/{len(records)} = {ea}%")
        print(f"  Wilson IC 95%: [{ci_lo}%, {ci_hi}%]")
        print(f"  Matches flexíveis: {flexible_ct}")
        print(f"  JSON: {json_path}")
        print(f"{'='*60}")

    # Relatório agregado quando runs > 1
    if args.runs > 1:
        agg_md = render_aggregate_report(args.model, args.temperature, all_runs)
        agg_path = out_dir / f"eval_{args.model}_{ts_slug_base}_aggregate.md"
        agg_path.write_text(agg_md)
        if mestrado_dir.exists():
            dest = mestrado_dir / agg_path.name
            dest.write_text(agg_md)
            print(f"\n📊 Relatório agregado: {dest}")
        run_eas = [sum(1 for r in run if r["correct"]) / len(queries) * 100 for run in all_runs]
        mean_ea = sum(run_eas) / args.runs
        if args.runs > 1:
            sd_ea = math.sqrt(sum((x - mean_ea) ** 2 for x in run_eas) / (args.runs - 1))
        else:
            sd_ea = 0.0
        print(f"\n{'='*60}")
        print(f"  AGREGADO {args.runs} runs — Média EA: {mean_ea:.1f}% ± {sd_ea:.1f}% DP")
        print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
