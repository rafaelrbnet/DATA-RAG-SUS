"""
Transformação canônica: lê Parquets de data/raw/ e grava domínio amplo/unificado
em data/processed/.

Estratégia:
  - data/raw/ é fonte de verdade.
  - Processar apenas arquivos em raw sem correspondente em processed (diff).
  - Um arquivo por vez para controle de memória.
"""

from __future__ import annotations

from pathlib import Path
import re

import pandas as pd

from .log_util import log


def _root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


RAW_BASE = _root() / "data" / "raw"
PROCESSED_BASE = _root() / "data" / "processed"

QUEM = "Python"
ONDE_BASE = "transform"

COMMON_COLUMNS = [
    "ano_cmpt",
    "mes_cmpt",
    "sistema",
    "uf_origem",
    "main_icd",
    "icd_group",
    "opm_flag",
    "fisio_flag",
    "mun_res_status",
    "mun_res_tipo",
    "mun_res_nome",
    "mun_res_uf",
    "mun_res_lat",
    "mun_res_lon",
    "mun_res_alt",
    "mun_res_area",
]

SIA_SPECIFIC_COLUMNS = [
    "pa_cmp",
    "pa_mvm",
    "pa_idade",
    "idademin",
    "idademax",
    "pa_sexo",
    "pa_racacor",
    "pa_etnia",
    "pa_ufmun",
    "pa_munpcn",
    "pa_ufdif",
    "pa_mndif",
    "pa_coduni",
    "pa_cnpjcpf",
    "pa_cnpjmnt",
    "pa_nat_jur",
    "pa_cnsmed",
    "pa_cbocod",
    "pa_proc_id",
    "nome_proced",
    "pa_grupo",
    "pa_subgru",
    "pa_cidpri",
    "pa_cidsec",
    "pa_cidcas",
    "pa_qtdpro",
    "pa_qtdapr",
    "pa_valpro",
    "pa_valapr",
    "pa_vl_cf",
    "pa_vl_cl",
    "pa_vl_inc",
    "nu_pa_tot",
    "nu_vpa_tot",
    "pa_docorig",
    "pa_autoriz",
    "pa_catend",
    "pa_motsai",
    "pa_indica",
    "pa_tpfin",
    "pa_subfin",
    "pa_gestao",
]

SIH_SPECIFIC_COLUMNS = [
    "n_aih",
    "cnes",
    "cgc_hosp",
    "cnpj_mant",
    "munic_res",
    "munic_mov",
    "uf_zi",
    "idade",
    "cod_idade",
    "sexo",
    "nasc",
    "raca_cor",
    "etnia",
    "cep",
    "nacional",
    "diag_princ",
    "diag_secun",
    "diagsec1",
    "diagsec2",
    "diagsec3",
    "diagsec4",
    "diagsec5",
    "diagsec6",
    "diagsec7",
    "diagsec8",
    "diagsec9",
    "cid_morte",
    "cid_notif",
    "cid_asso",
    "cid_princ",
    "proc_rea",
    "proc_solic",
    "uti_mes_to",
    "uti_int_to",
    "qt_diarias",
    "dias_perm",
    "val_tot",
    "val_sh",
    "val_sp",
    "val_sadt",
    "val_ortp",
    "val_uti",
    "val_uci",
    "val_sangue",
    "val_acomp",
    "dt_inter",
    "dt_saida",
    "morte",
    "cobranca",
    "gestao",
    "financ",
    "faec_tp",
    "aud_just",
    "sis_just",
    "sequencia",
]

DERIVED_COLUMNS = [
    "idade_paciente",
    "sexo_paciente",
    "raca_cor_paciente",
    "cod_munic_residencia",
    "cod_munic_estabelecimento",
    "cnes_estabelecimento",
    "cod_procedimento",
    "cid_principal",
    "custo_total",
    "competencia_ano_mes",
]

CANONICAL_OUTPUT_COLUMNS = COMMON_COLUMNS + SIA_SPECIFIC_COLUMNS + SIH_SPECIFIC_COLUMNS + DERIVED_COLUMNS

# Aliases históricos/heterogêneos após normalização.
ALIAS_TO_CANONICAL = {
    "municip_res": "munic_res",
    "municip_mov": "munic_mov",
    "pa_uf_mun": "pa_ufmun",
    "pa_mun_pcn": "pa_munpcn",
}


def _is_temporary_parquet_artifact(path: Path) -> bool:
    name = path.name
    return (
        name.startswith(".downloading_")
        or name.startswith(".download_")
        or name.startswith(".tmp_")
        or name.endswith(".parquet.tmp")
    )


def _processed_path_for_raw(raw_path: Path) -> Path:
    ano, uf, _sistema, mes = _extract_partitions(raw_path)
    if not (ano and uf and mes is not None):
        # fallback defensivo para não quebrar chamadas antigas
        rel = raw_path.relative_to(RAW_BASE)
        return PROCESSED_BASE / rel
    return _processed_path_for_group(ano, uf, mes)


def _normalize_col_name(name: str) -> str:
    s = str(name).strip()
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return ALIAS_TO_CANONICAL.get(s, s)


def _coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    canonical = [_normalize_col_name(c) for c in df.columns]
    groups: dict[str, list[int]] = {}
    for idx, col in enumerate(canonical):
        groups.setdefault(col, []).append(idx)

    out: dict[str, pd.Series] = {}
    for col, idxs in groups.items():
        if len(idxs) == 1:
            out[col] = df.iloc[:, idxs[0]]
        else:
            block = df.iloc[:, idxs]
            out[col] = block.bfill(axis=1).iloc[:, 0]
    return pd.DataFrame(out)


def _pick_first_present(df: pd.DataFrame, candidates: list[str]) -> pd.Series | None:
    for col in candidates:
        if col in df.columns:
            return df[col]
    return None


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


def _as_clean_numeric(series: pd.Series | None, index: pd.Index) -> pd.Series:
    if series is None:
        return pd.Series(pd.NA, index=index, dtype="Float64")
    s = _as_clean_string(series, index=index)
    return pd.to_numeric(s, errors="coerce").astype("Float64")


def _derive_competencia(df: pd.DataFrame, ano_part: str, mes_part: str) -> pd.Series:
    index = df.index

    # SIA: pa_cmp
    pa_cmp = _pick_first_present(df, ["pa_cmp"])
    if pa_cmp is not None:
        s = _as_clean_string(pa_cmp, index=index)
        digits = s.str.replace(r"\D", "", regex=True).str[:6]
        out = pd.to_numeric(digits, errors="coerce").astype("Int64")
        if out.notna().any():
            return out

    # SIH: ano_cmpt + mes_cmpt
    ano = _as_clean_numeric(_pick_first_present(df, ["ano_cmpt"]), index=index).astype("Int64")
    mes = _as_clean_numeric(_pick_first_present(df, ["mes_cmpt"]), index=index).astype("Int64")

    ano_fallback = pd.to_numeric(pd.Series([ano_part] * len(df), index=index), errors="coerce").astype("Int64")
    mes_fallback = pd.to_numeric(pd.Series([mes_part] * len(df), index=index), errors="coerce").astype("Int64")

    ano_i = ano.fillna(ano_fallback)
    mes_i = mes.fillna(mes_fallback)
    return (ano_i * 100 + mes_i).astype("Int64")


def _build_unified_table(df: pd.DataFrame, ano_part: str, mes_part: str) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)

    # Domínio amplo: comuns + específicas (SIA/SIH), preservando valores da origem.
    for col in COMMON_COLUMNS + SIA_SPECIFIC_COLUMNS + SIH_SPECIFIC_COLUMNS:
        if col in df.columns:
            out[col] = df[col]
        else:
            out[col] = pd.NA

    # Derivadas unificadas para consumo analítico/RAG.
    out["idade_paciente"] = _as_clean_numeric(
        _pick_first_present(df, ["pa_idade", "idade"]), index=df.index
    )
    out["sexo_paciente"] = _as_clean_string(
        _pick_first_present(df, ["pa_sexo", "sexo"]), index=df.index
    )
    out["raca_cor_paciente"] = _as_clean_string(
        _pick_first_present(df, ["pa_racacor", "raca_cor"]), index=df.index
    )
    out["cod_munic_residencia"] = _as_clean_string(
        _pick_first_present(df, ["pa_munpcn", "munic_res"]), index=df.index
    )
    out["cod_munic_estabelecimento"] = _as_clean_string(
        _pick_first_present(df, ["pa_ufmun", "uf_zi"]), index=df.index
    )
    out["cnes_estabelecimento"] = _as_clean_string(
        _pick_first_present(df, ["pa_coduni", "cnes"]), index=df.index
    )
    out["cod_procedimento"] = _as_clean_string(
        _pick_first_present(df, ["pa_proc_id", "proc_rea"]), index=df.index
    )
    out["cid_principal"] = _as_clean_string(
        _pick_first_present(df, ["pa_cidpri", "diag_princ"]), index=df.index
    )
    out["custo_total"] = _as_clean_numeric(
        _pick_first_present(df, ["pa_valapr", "val_tot"]), index=df.index
    )
    out["competencia_ano_mes"] = _derive_competencia(df, ano_part=ano_part, mes_part=mes_part)

    return out.loc[:, CANONICAL_OUTPUT_COLUMNS]


def _extract_partitions(raw_path: Path) -> tuple[str | None, str | None, str | None]:
    ano = uf = sistema = None
    for p in raw_path.parts:
        if p.startswith("ano="):
            ano = p.split("=", 1)[1]
        elif p.startswith("uf="):
            uf = p.split("=", 1)[1]
        elif p.startswith("sistema="):
            sistema = p.split("=", 1)[1]
    mes = None
    m = re.search(r"_(\d{2})\.parquet$", raw_path.name)
    if m:
        mes = int(m.group(1))
    return ano, uf, sistema, mes


def _processed_path_for_group(ano: str, uf: str, mes: int) -> Path:
    dest_dir = PROCESSED_BASE / f"ano={ano}" / f"uf={uf}"
    return dest_dir / f"sus_{uf}_{ano}_{mes:02d}.parquet"


def _load_and_transform_raw(raw_path: Path, ano: str, mes: int) -> pd.DataFrame | None:
    mes_part = str(mes)

    try:
        try:
            df = pd.read_parquet(raw_path, dtype_backend="pyarrow")
        except (TypeError, ValueError):
            df = pd.read_parquet(raw_path)
    except Exception as e:
        log(QUEM, str(raw_path), f"ERRO ao ler Parquet: {e}")
        return None

    df = _coalesce_duplicate_columns(df)
    return _build_unified_table(df, ano_part=ano, mes_part=mes_part)


def transform_single_file(raw_path: Path) -> Path | None:
    # Mantido para compatibilidade, processa um arquivo isolado e escreve sem partição por sistema.
    ano, uf, _sistema, mes = _extract_partitions(raw_path)
    if not (ano and uf and mes is not None):
        log(QUEM, ONDE_BASE, f"ERRO: path sem partições esperadas: {raw_path}")
        return None
    df = _load_and_transform_raw(raw_path, ano=ano, mes=mes)
    if df is None:
        return None
    dest_path = _processed_path_for_group(ano, uf, mes)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(dest_path, index=False)
        return dest_path
    except Exception as e:
        log(QUEM, str(dest_path), f"ERRO ao gravar: {e}")
        return None
    finally:
        del df


def _group_raw_files(raw_files: list[Path]) -> dict[tuple[str, str, int], list[Path]]:
    groups: dict[tuple[str, str, int], list[Path]] = {}
    for p in raw_files:
        ano, uf, _sistema, mes = _extract_partitions(p)
        if not (ano and uf and mes is not None):
            continue
        key = (ano, uf, mes)
        groups.setdefault(key, []).append(p)
    for files in groups.values():
        files.sort()
    return groups


def _transform_group(ano: str, uf: str, mes: int, paths: list[Path]) -> Path | None:
    frames: list[pd.DataFrame] = []
    for p in paths:
        df = _load_and_transform_raw(p, ano=ano, mes=mes)
        if df is None:
            return None
        frames.append(df)
    if not frames:
        return None
    if len(frames) == 1:
        out = frames[0]
    else:
        out = pd.concat(frames, axis=0, ignore_index=True)

    dest_path = _processed_path_for_group(ano, uf, mes)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        out.to_parquet(dest_path, index=False)
        return dest_path
    except Exception as e:
        log(QUEM, str(dest_path), f"ERRO ao gravar: {e}")
        return None
    finally:
        del out
        for f in frames:
            del f


def run_transform(skip_existing: bool = True) -> None:
    if not RAW_BASE.is_dir():
        log(QUEM, ONDE_BASE, f"Diretório inexistente: {RAW_BASE}")
        print("ERRO: Diretório data/raw/ inexistente.", flush=True)
        return

    all_raw = sorted(
        p for p in RAW_BASE.rglob("*.parquet")
        if not _is_temporary_parquet_artifact(p)
    )
    grouped = _group_raw_files(all_raw)
    if skip_existing:
        groups = {
            k: v for k, v in grouped.items()
            if not _processed_path_for_group(k[0], k[1], k[2]).exists()
        }
    else:
        groups = grouped

    if not groups:
        if not all_raw:
            log(QUEM, ONDE_BASE, f"Nenhum .parquet em {RAW_BASE}")
            print("Nenhum .parquet em data/raw/. Execute a ingestão antes.", flush=True)
        else:
            log(QUEM, ONDE_BASE, "Nenhum arquivo pendente para transform (raw já espelhado em processed).")
            print(
                "Nenhum arquivo pendente: todos os grupos (ano/uf/mês) de data/raw/ já têm correspondente em data/processed/.",
                flush=True,
            )
        return

    total = len(groups)
    if skip_existing and len(grouped) > total:
        print(
            f"Pulando {len(grouped) - total} grupo(s) já em data/processed/. Processando {total} pendente(s).",
            flush=True,
        )
    print(f"Iniciando transform: {total} grupo(s) (ano/uf/mês) em data/raw/", flush=True)
    ok = 0
    fail = 0
    keys = sorted(groups.keys(), key=lambda x: (x[0], x[1], x[2]))
    for i, key in enumerate(keys, start=1):
        ano, uf, mes = key
        print(f"  [{i}/{total}] Processando: {uf} {ano}-{mes:02d}", flush=True)
        result = _transform_group(ano, uf, mes, groups[key])
        if result is not None:
            ok += 1
        else:
            fail += 1
            print(f"       ⚠ Falha ao processar {uf} {ano}-{mes:02d}", flush=True)

    n_processed = len(list(PROCESSED_BASE.rglob("*.parquet"))) if PROCESSED_BASE.is_dir() else 0
    log(QUEM, ONDE_BASE, f"Concluído: {ok} processados, {fail} falhas. data/processed/: {n_processed} arquivos .parquet.")
    print(f"Concluído: {ok} processados, {fail} falhas. data/processed/: {n_processed} arquivos.", flush=True)


if __name__ == "__main__":
    run_transform()
