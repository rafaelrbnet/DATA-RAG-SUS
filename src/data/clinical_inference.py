"""
Enriquecimento clínico determinístico: lê data/processed/ e grava data/enriched/.

Objetivo:
  - Gerar narrativa de Evento Assistencial (AHEN) por registro.
  - Preservar rastreabilidade por row_id e versionar regras clínicas.
"""

from __future__ import annotations

from pathlib import Path
import re

import pandas as pd

from .log_util import log


def _root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


PROCESSED_BASE = _root() / "data" / "processed"
ENRICHED_BASE = _root() / "data" / "enriched"

QUEM = "Python"
ONDE_BASE = "clinical_inference"

CLINICAL_INFERENCE_VERSION = "ahen_v1.0.0"
CLINICAL_TEMPLATE_ID = "AHEN_V1"
CLINICAL_TEMPLATE_LABEL = "Assistive Health Event Narrative"

OUTPUT_COLUMNS = [
    "row_id",
    "clinical_template_id",
    "clinical_template_label",
    "clinical_inference_version",
    "clinical_inference_rule_id",
    "clinical_inference_confidence",
    "clinical_inference_reason",
    "clinical_interpretacao_clinica",
    "clinical_tipo_atendimento",
    "clinical_deslocamento_territorial",
    "clinical_event_narrative",
]


def _as_clean_string(series: pd.Series | None, index: pd.Index) -> pd.Series:
    if series is None:
        return pd.Series(pd.NA, index=index, dtype="string")
    s = series.astype("string").str.strip()
    return s.replace(
        {
            "": pd.NA,
            "nan": pd.NA,
            "NaN": pd.NA,
            "none": pd.NA,
            "None": pd.NA,
            "<NA>": pd.NA,
        }
    )


def _extract_partitions(path: Path) -> tuple[str | None, str | None, int | None]:
    ano = uf = None
    for p in path.parts:
        if p.startswith("ano="):
            ano = p.split("=", 1)[1]
        elif p.startswith("uf="):
            uf = p.split("=", 1)[1]
    mes = None
    m = re.search(r"_(\d{2})\.parquet$", path.name)
    if m:
        mes = int(m.group(1))
    return ano, uf, mes


def _enriched_path_for_processed(processed_path: Path) -> Path | None:
    ano, uf, mes = _extract_partitions(processed_path)
    if not (ano and uf and mes is not None):
        return None
    dest_dir = ENRICHED_BASE / f"ano={ano}" / f"uf={uf}"
    return dest_dir / f"sus_{uf}_{ano}_{mes:02d}.parquet"


def _enriched_file_has_valid_row_id(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        sample = pd.read_parquet(path, columns=["row_id"]).head(10)
    except Exception:
        return False
    if "row_id" not in sample.columns:
        return False
    row_id = sample["row_id"].astype("string")
    if row_id.isna().any():
        return False
    return not row_id.str.startswith("row_missing_", na=False).any()


def _infer_interpretacao_clinica(icd_group: pd.Series) -> pd.Series:
    g = icd_group.astype("string").str.upper()
    out = pd.Series("condicoes clinicas diversas", index=icd_group.index, dtype="string")
    out[g.str.startswith("I", na=False)] = "doencas do aparelho circulatorio"
    out[g.str.startswith("J", na=False)] = "doencas do aparelho respiratorio"
    out[g.str.startswith("M", na=False)] = "doencas osteomusculares e do tecido conjuntivo"
    out[g.str.startswith("S", na=False) | g.str.startswith("T", na=False)] = "lesoes e causas traumaticas"
    out[g.str.startswith("C", na=False)] = "neoplasias"
    out[g.str.startswith("E", na=False)] = "doencas endocrinas, nutricionais e metabolicas"
    return out.astype("string")


def _infer_tipo_atendimento(sistema: pd.Series) -> pd.Series:
    s = sistema.astype("string").str.upper()
    out = pd.Series("atendimento nao especificado", index=sistema.index, dtype="string")
    out[s.eq("SIA")] = "producao ambulatorial"
    out[s.eq("SIH")] = "episodio de internacao"
    return out.astype("string")


def _infer_deslocamento_territorial(
    pa_mndif: pd.Series,
    pa_ufdif: pd.Series,
    cod_munic_residencia: pd.Series,
    cod_munic_estabelecimento: pd.Series,
) -> pd.Series:
    mnd = pa_mndif.astype("string")
    ufd = pa_ufdif.astype("string")
    res = cod_munic_residencia.astype("string")
    est = cod_munic_estabelecimento.astype("string")

    out = pd.Series("sem evidencia de deslocamento territorial", index=pa_mndif.index, dtype="string")
    out[mnd.eq("1")] = "deslocamento intermunicipal"
    out[ufd.eq("1")] = "deslocamento interestadual"

    both = res.str.len().eq(6).fillna(False) & est.str.len().eq(6).fillna(False)
    uf_diff = res.str[:2].ne(est.str[:2]).fillna(False)
    mun_diff = res.ne(est).fillna(False)

    out[(out == "sem evidencia de deslocamento territorial") & both & uf_diff] = "deslocamento interestadual"
    out[(out == "sem evidencia de deslocamento territorial") & both & mun_diff] = "deslocamento intermunicipal"
    return out.astype("string")


def _format_custo(custo_total: pd.Series) -> pd.Series:
    n = pd.to_numeric(custo_total, errors="coerce")
    text = n.map(lambda v: f"{v:,.2f}" if pd.notna(v) else "nao informado")
    text = text.str.replace(",", "X", regex=False).str.replace(".", ",", regex=False).str.replace("X", ".", regex=False)
    return text.astype("string")


def _safe_text(series: pd.Series | None, index: pd.Index, fallback: str = "nao informado") -> pd.Series:
    s = _as_clean_string(series, index=index)
    return s.fillna(fallback).astype("string")


def _build_narrative(df: pd.DataFrame) -> pd.DataFrame:
    idx = df.index

    sistema = _safe_text(df.get("sistema"), idx, "desconhecido")
    competencia = _safe_text(df.get("competencia_ano_mes"), idx, "nao informada")
    cnes = _safe_text(df.get("cnes_estabelecimento"), idx)
    cod_munic_est = _safe_text(df.get("cod_munic_estabelecimento"), idx)
    uf = _safe_text(df.get("uf_origem"), idx)
    cid = _safe_text(df.get("cid_principal"), idx)
    icd_group = _safe_text(df.get("icd_group"), idx, "grupo nao informado")
    nome_proced = _safe_text(df.get("nome_proced"), idx, "procedimento nao informado")
    cod_proced = _safe_text(df.get("cod_procedimento"), idx)
    custo_fmt = _format_custo(df.get("custo_total"))
    n_aih = _safe_text(df.get("n_aih"), idx)
    dias_perm = _safe_text(df.get("dias_perm"), idx)
    morte = _safe_text(df.get("morte"), idx)
    pa_docorig = _safe_text(df.get("pa_docorig"), idx)
    pa_autoriz = _safe_text(df.get("pa_autoriz"), idx)

    interpretacao = _infer_interpretacao_clinica(icd_group)
    tipo_atendimento = _infer_tipo_atendimento(sistema)
    deslocamento = _infer_deslocamento_territorial(
        _safe_text(df.get("pa_mndif"), idx),
        _safe_text(df.get("pa_ufdif"), idx),
        _safe_text(df.get("cod_munic_residencia"), idx),
        _safe_text(df.get("cod_munic_estabelecimento"), idx),
    )

    is_sia = sistema.eq("SIA")
    is_sih = sistema.eq("SIH")

    rule_id = pd.Series("RULE_AHEN_BASE_V11", index=idx, dtype="string")
    rule_id.loc[is_sia] = "RULE_AHEN_SIA_V11"
    rule_id.loc[is_sih] = "RULE_AHEN_SIH_V11"

    conf = pd.Series(0.96, index=idx, dtype="Float64")
    reason = pd.Series(
        "Narrativa AHEN v1.1 gerada por regras deterministicas com variacao por sistema.",
        index=idx,
        dtype="string",
    )

    detalhe_sistema = pd.Series(
        "O registro foi tratado com regra generica por ausencia de classificacao de sistema.",
        index=idx,
        dtype="string",
    )
    detalhe_sistema.loc[is_sia] = (
        "Por se tratar de producao ambulatorial (SIA), o registro descreve um procedimento faturado, "
        + "com documento de origem "
        + pa_docorig
        + " e autorizacao "
        + pa_autoriz
        + "."
    )
    detalhe_sistema.loc[is_sih] = (
        "Por se tratar de episodio de internacao (SIH), o registro refere-se a uma AIH "
        + n_aih
        + ", com permanencia informada de "
        + dias_perm
        + " dia(s) e indicador de obito "
        + morte
        + "."
    )

    text = (
        "Registro de evento assistencial identificado na base "
        + sistema
        + ", referente a competencia "
        + competencia
        + ".\n\n"
        + "O evento foi realizado no estabelecimento CNES "
        + cnes
        + ", localizado em "
        + cod_munic_est
        + "/"
        + uf
        + ".\n\n"
        + "O diagnostico principal registrado foi "
        + cid
        + " ("
        + icd_group
        + "), associado a condicoes relacionadas a "
        + interpretacao
        + ".\n\n"
        + "Foi realizado o procedimento "
        + nome_proced
        + " ("
        + cod_proced
        + ").\n\n"
        + "O evento ocorreu no contexto "
        + tipo_atendimento
        + ".\n\n"
        + "O custo total associado ao evento foi de aproximadamente R$ "
        + custo_fmt
        + ".\n\n"
        + "Quando aplicavel, indicadores administrativos sugerem "
        + deslocamento
        + ".\n\n"
        + detalhe_sistema
        + "\n\n"
        + "Este registro representa um evento assistencial individual na base administrativa "
        + "e nao implica correspondencia direta com registros de outros sistemas ou episodios clinicos."
    ).astype("string")

    out = pd.DataFrame(index=idx)
    if "row_id" in df.columns:
        out["row_id"] = _safe_text(df.get("row_id"), idx, "row_id_ausente")
    else:
        out["row_id"] = pd.Series([f"row_missing_{i}" for i in range(len(df))], index=idx, dtype="string")
    out["clinical_template_id"] = CLINICAL_TEMPLATE_ID
    out["clinical_template_label"] = CLINICAL_TEMPLATE_LABEL
    out["clinical_inference_version"] = CLINICAL_INFERENCE_VERSION
    out["clinical_inference_rule_id"] = rule_id
    out["clinical_inference_confidence"] = conf
    out["clinical_inference_reason"] = reason
    out["clinical_interpretacao_clinica"] = interpretacao
    out["clinical_tipo_atendimento"] = tipo_atendimento
    out["clinical_deslocamento_territorial"] = deslocamento
    out["clinical_event_narrative"] = text
    return out.loc[:, OUTPUT_COLUMNS]


def enrich_single_file(processed_path: Path) -> Path | None:
    dest = _enriched_path_for_processed(processed_path)
    if dest is None:
        log(QUEM, ONDE_BASE, f"ERRO: path sem particoes esperadas: {processed_path}")
        return None
    try:
        df = pd.read_parquet(processed_path)
    except Exception as e:
        log(QUEM, str(processed_path), f"ERRO ao ler processed parquet: {e}")
        return None

    enriched = _build_narrative(df)
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        enriched.to_parquet(dest, index=False)
        return dest
    except Exception as e:
        log(QUEM, str(dest), f"ERRO ao gravar enriched parquet: {e}")
        return None


def run_clinical_inference(skip_existing: bool = True) -> None:
    if not PROCESSED_BASE.is_dir():
        log(QUEM, ONDE_BASE, f"Diretorio inexistente: {PROCESSED_BASE}")
        print("ERRO: Diretorio data/processed/ inexistente.", flush=True)
        return

    all_processed = sorted(PROCESSED_BASE.rglob("*.parquet"))
    if not all_processed:
        print("Nenhum parquet em data/processed/. Execute o transform antes.", flush=True)
        return

    if skip_existing:
        pending: list[Path] = []
        for p in all_processed:
            dest = _enriched_path_for_processed(p)
            if dest is None:
                continue
            if not _enriched_file_has_valid_row_id(dest):
                pending.append(p)
    else:
        pending = all_processed

    if not pending:
        print("Nenhum arquivo pendente: data/enriched/ ja espelha data/processed/.", flush=True)
        return

    total = len(pending)
    if skip_existing and len(all_processed) > total:
        print(
            f"Pulando {len(all_processed) - total} arquivo(s) ja enriquecidos. Processando {total} pendente(s).",
            flush=True,
        )
    print(f"Iniciando enriquecimento clinico: {total} arquivo(s).", flush=True)

    ok = 0
    fail = 0
    for i, path in enumerate(pending, start=1):
        ano, uf, mes = _extract_partitions(path)
        label = f"{uf or '??'} {ano or '????'}-{(mes or 0):02d}"
        print(f"  [{i}/{total}] Enriquecendo: {label}", flush=True)
        out = enrich_single_file(path)
        if out is not None:
            ok += 1
        else:
            fail += 1
            print(f"       Falha ao enriquecer {label}", flush=True)

    n_enriched = len(list(ENRICHED_BASE.rglob("*.parquet"))) if ENRICHED_BASE.is_dir() else 0
    log(QUEM, ONDE_BASE, f"Concluido: {ok} enriquecidos, {fail} falhas. data/enriched/: {n_enriched} arquivos.")
    print(f"Concluido: {ok} enriquecidos, {fail} falhas. data/enriched/: {n_enriched} arquivos.", flush=True)


if __name__ == "__main__":
    run_clinical_inference()
