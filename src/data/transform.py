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
    "idademin",
    "idademax",
    "pa_etnia",
    "pa_ufdif",
    "pa_mndif",
    "pa_cnpjcpf",
    "pa_cnpjmnt",
    "pa_nat_jur",
    "pa_cnsmed",
    "pa_cbocod",
    "nome_proced",
    "pa_grupo",
    "pa_subgru",
    "pa_cidsec",
    "pa_cidcas",
    "pa_qtdpro",
    "pa_qtdapr",
    "pa_valpro",
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
    "cgc_hosp",
    "cnpj_mant",
    "munic_mov",
    "cod_idade",
    "nasc",
    "etnia",
    "cep",
    "nacional",
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
    "proc_solic",
    "uti_mes_to",
    "uti_int_to",
    "qt_diarias",
    "dias_perm",
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
    "year_comp": "ano_cmpt",
    "month_comp": "mes_cmpt",
    "system": "sistema",
    "uf": "uf_origem",
    "municip_res": "munic_res",
    "municip_mov": "munic_mov",
    "munresstatus": "mun_res_status",
    "munrestipo": "mun_res_tipo",
    "munresnome": "mun_res_nome",
    "munresuf": "mun_res_uf",
    "munreslat": "mun_res_lat",
    "munreslon": "mun_res_lon",
    "munresalt": "mun_res_alt",
    "munresarea": "mun_res_area",
    "pa_uf_mun": "pa_ufmun",
    "pa_mun_pcn": "pa_munpcn",
}

BOOL_COLUMNS = {"opm_flag", "fisio_flag"}
INT_COLUMNS = {"ano_cmpt", "mes_cmpt", "competencia_ano_mes"}
FLOAT_COLUMNS = {
    "mun_res_lat",
    "mun_res_lon",
    "mun_res_alt",
    "mun_res_area",
    "idademin",
    "idademax",
    "pa_qtdpro",
    "pa_qtdapr",
    "pa_valpro",
    "pa_vl_cf",
    "pa_vl_cl",
    "pa_vl_inc",
    "nu_pa_tot",
    "nu_vpa_tot",
    "uti_mes_to",
    "uti_int_to",
    "qt_diarias",
    "dias_perm",
    "val_sh",
    "val_sp",
    "val_sadt",
    "val_ortp",
    "val_uti",
    "val_uci",
    "val_sangue",
    "val_acomp",
    "idade_paciente",
    "custo_total",
}

UF_CODE_TO_SIGLA = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL",
    "28": "SE", "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
    "41": "PR", "42": "SC", "43": "RS", "50": "MS", "51": "MT", "52": "GO", "53": "DF",
}
UF_SIGLAS_VALIDAS = set(UF_CODE_TO_SIGLA.values())

CID_CHAPTER_BY_LETTER = {
    "A": "A00-B99",
    "B": "A00-B99",
    "C": "C00-D48",
    "D": "C00-D48",
    "E": "E00-E90",
    "F": "F00-F99",
    "G": "G00-G99",
    "H": "H00-H59",
    "I": "I00-I99",
    "J": "J00-J99",
    "K": "K00-K93",
    "L": "L00-L99",
    "M": "M00-M99",
    "N": "N00-N99",
    "O": "O00-O99",
    "P": "P00-P96",
    "Q": "Q00-Q99",
    "R": "R00-R99",
    "S": "S00-T98",
    "T": "S00-T98",
    "V": "V01-Y98",
    "W": "V01-Y98",
    "X": "V01-Y98",
    "Y": "V01-Y98",
    "Z": "Z00-Z99",
    "U": "U00-U99",
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


def _as_clean_int(series: pd.Series | None, index: pd.Index) -> pd.Series:
    s = _as_clean_numeric(series, index=index)
    return s.round(0).astype("Int64")


def _as_clean_bool(series: pd.Series | None, index: pd.Index) -> pd.Series:
    if series is None:
        return pd.Series(pd.NA, index=index, dtype="boolean")
    s = _as_clean_string(series, index=index).str.lower()
    true_set = {"1", "true", "t", "sim", "s", "y", "yes"}
    false_set = {"0", "false", "f", "nao", "não", "n", "no"}
    out = pd.Series(pd.NA, index=index, dtype="boolean")
    out[s.isin(true_set)] = True
    out[s.isin(false_set)] = False
    return out


def _normalize_sexo(series: pd.Series) -> pd.Series:
    s = _as_clean_string(series, index=series.index).str.upper()
    map_dict = {
        "M": "M",
        "1": "M",
        "MASCULINO": "M",
        "F": "F",
        "3": "F",
        "FEMININO": "F",
        "IGNORADO": pd.NA,
        "9": pd.NA,
        "0": pd.NA,
    }
    # Regras canônicas: M(1), F(3), demais códigos/campos inválidos -> nulo.
    return s.map(lambda v: map_dict.get(v, pd.NA)).astype("string")


def _normalize_raca_cor(series: pd.Series) -> pd.Series:
    """
    Padroniza raça/cor para códigos oficiais:
    01 Branca, 02 Preta, 03 Parda, 04 Amarela, 05 Indígena, 99 Sem Informação.
    """
    s = _as_clean_string(series, index=series.index).str.upper()
    map_dict = {
        "1": "01",
        "01": "01",
        "BRANCA": "01",
        "2": "02",
        "02": "02",
        "PRETA": "02",
        "3": "03",
        "03": "03",
        "PARDA": "03",
        "4": "04",
        "04": "04",
        "AMARELA": "04",
        "5": "05",
        "05": "05",
        "INDIGENA": "05",
        "INDÍGENA": "05",
        "9": "99",
        "99": "99",
        "0": "99",
    }
    # Vazios/nulos e valores fora do dicionário também vão para 99.
    out = s.map(lambda v: map_dict.get(v, "99"))
    out[s.isna()] = "99"
    return out.astype("string")


def _normalize_municipio_ibge6(series: pd.Series) -> pd.Series:
    """
    Padroniza código de município para 6 dígitos IBGE (sem DV):
    - mantém apenas dígitos;
    - truncagem para 6 quando vier com 7+;
    - valores técnicos inválidos -> null.
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    code6 = digits.str[:6]
    invalid = {"", "000000", "999999"}
    out = code6.mask(code6.isin(invalid), pd.NA)
    return out.astype("string")


def _normalize_municipio_estabelecimento(series: pd.Series) -> pd.Series:
    """
    Padroniza código do município/gestão do estabelecimento para 6 dígitos.

    Observação (SIH/uf_zi):
    - Valores no formato XX0000 podem representar gestão estadual (UF gestora),
      não necessariamente município físico do estabelecimento.
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    code6 = digits.str[:6]
    invalid = {"", "000000", "999999"}
    out = code6.mask(code6.isin(invalid), pd.NA)
    return out.astype("string")


def _normalize_cnes(series: pd.Series) -> pd.Series:
    """
    Padroniza CNES para 7 dígitos:
    - remove espaços e caracteres não numéricos;
    - mantém os 7 últimos dígitos e aplica zero-fill à esquerda;
    - valores técnicos inválidos -> null.
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    code7 = digits.str[-7:].str.zfill(7)
    invalid = {"0000000", "9999999"}
    out = code7.mask(digits.isna() | digits.eq("") | code7.isin(invalid), pd.NA)
    return out.astype("string")


def _normalize_cod_procedimento(series: pd.Series) -> pd.Series:
    """
    Padroniza código de procedimento (SIGTAP) para 10 dígitos.
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    code10 = digits.str[-10:].str.zfill(10)
    invalid = {"0000000000"}
    out = code10.mask(digits.isna() | digits.eq("") | code10.isin(invalid), pd.NA)
    return out.astype("string")


def _normalize_custo_total(series: pd.Series) -> pd.Series:
    """
    Padroniza custo_total para Float64 com suporte a separador decimal vírgula.
    """
    s = _as_clean_string(series, index=series.index)
    compact = s.str.replace(r"\s+", "", regex=True)

    # Quando houver '.' e ',', assume padrão brasileiro (milhar='.', decimal=',').
    has_dot = compact.str.contains(r"\.", na=False)
    has_comma = compact.str.contains(",", na=False)
    mixed = has_dot & has_comma
    normalized = compact.where(~mixed, compact.str.replace(".", "", regex=False))
    normalized = normalized.str.replace(",", ".", regex=False)

    return pd.to_numeric(normalized, errors="coerce").astype("Float64")


def _normalize_ano_cmpt(series: pd.Series) -> pd.Series:
    """
    Padroniza ano de competência para Int64 (AAAA):
    - aceita string/número;
    - corrige ano com 2 dígitos para século 21 (23 -> 2023);
    - invalida valores fora da faixa [2000, 2099].
    """
    idx = series.index
    s = _as_clean_string(series, index=idx)

    # Tentativa principal: parse numérico direto (cobre 2024, 2024.0, "2024").
    num = pd.to_numeric(s.str.replace(",", ".", regex=False), errors="coerce")
    year = num.round(0).astype("Int64")

    # Fallback: extrair apenas dígitos (cobre formatos heterogêneos).
    missing = year.isna()
    if missing.any():
        digits = s.str.replace(r"\D", "", regex=True)
        fallback = pd.to_numeric(
            digits.where(digits.str.len().isin([2, 4])),
            errors="coerce",
        ).astype("Int64")
        year = year.where(~missing, fallback)

    # Ajuste para ano com 2 dígitos (01..99). Zero é inválido.
    year = year.where(~year.eq(0), pd.NA)
    two_digits = year.between(1, 99, inclusive="both")
    year = year.where(~two_digits, year + 2000)

    # Faixa válida.
    year = year.where(year.between(2000, 2099, inclusive="both"), pd.NA)
    return year.astype("Int64")


def _normalize_mes_cmpt(series: pd.Series, competencia_ano_mes: pd.Series | None = None) -> pd.Series:
    """
    Padroniza mês de competência para Int64:
    - aceita string/número;
    - valores válidos no intervalo 1..12;
    - fallback opcional pelo campo competencia_ano_mes (AAAAMM).
    """
    idx = series.index
    m = _as_clean_int(series, index=idx)
    m = m.where(m.between(1, 12, inclusive="both"), pd.NA)

    if competencia_ano_mes is not None:
        comp = _as_clean_int(competencia_ano_mes, index=idx)
        comp_mes = (comp % 100).astype("Int64")
        comp_mes = comp_mes.where(comp_mes.between(1, 12, inclusive="both"), pd.NA)
        m = m.fillna(comp_mes)

    return m.astype("Int64")


def _normalize_cid_principal(series: pd.Series) -> pd.Series:
    """
    Padroniza CID principal no padrão CID-10 textual:
    - trim + uppercase;
    - remove ponto decimal;
    - mantém apenas alfanuméricos;
    - aceita formato com 3 ou 4 caracteres (ex.: A09, J450);
    - valores técnicos inválidos -> null.
    """
    s = _as_clean_string(series, index=series.index).str.upper()
    compact = s.str.replace(".", "", regex=False).str.replace(r"[^A-Z0-9]", "", regex=True)
    invalid = {"", "000", "0000", "999", "9999", "-"}
    valid = compact.str.match(r"^[A-Z][0-9]{2}[0-9A-Z]?$", na=False)
    out = compact.where(valid & ~compact.isin(invalid), pd.NA)
    return out.astype("string")


def _normalize_sistema(series: pd.Series) -> pd.Series:
    """
    Padroniza rótulo de sistema para SIA/SIH.
    """
    s = _as_clean_string(series, index=series.index).str.upper()
    map_dict = {
        "SIA": "SIA",
        "PA": "SIA",
        "BI": "SIA",
        "AD": "SIA",
        "SIH": "SIH",
        "RD": "SIH",
        "RJ": "SIH",
        "ER": "SIH",
    }
    return s.map(lambda v: map_dict.get(v, pd.NA)).astype("string")


def _normalize_uf_origem(series: pd.Series, cod_munic_residencia: pd.Series | None = None) -> pd.Series:
    """
    Padroniza UF de origem em sigla de 2 caracteres (27 UFs IBGE).
    Aceita sigla direta e código numérico (ex.: 35 -> SP).
    """
    s = _as_clean_string(series, index=series.index).str.upper()

    # Se já vier com sigla, valida.
    from_sigla = s.where(s.isin(UF_SIGLAS_VALIDAS), pd.NA)

    # Se vier código/município numérico, usa os 2 primeiros dígitos.
    digits = s.str.replace(r"\D", "", regex=True)
    code2 = digits.str[:2]
    from_code = code2.map(lambda c: UF_CODE_TO_SIGLA.get(c, pd.NA))

    out = from_sigla.fillna(from_code)

    # Fallback via município de residência (já padronizado em 6 dígitos).
    if cod_munic_residencia is not None:
        cmr = _as_clean_string(cod_munic_residencia, index=series.index)
        cmr_code2 = cmr.str.replace(r"\D", "", regex=True).str[:2]
        from_cmr = cmr_code2.map(lambda c: UF_CODE_TO_SIGLA.get(c, pd.NA))
        out = out.fillna(from_cmr)

    return out.astype("string")


def _normalize_icd_group(series: pd.Series, main_icd: pd.Series) -> pd.Series:
    """
    Normaliza icd_group como string de agrupamento CID-10.
    Prioriza valores válidos já preenchidos; senão deriva pelo capítulo de main_icd.
    """
    s = _as_clean_string(series, index=series.index).str.upper()

    valid_range = s.str.match(r"^[A-Z][0-9]{2}-[A-Z][0-9]{2}$", na=False)
    valid_cap = s.str.match(r"^CAP[IÍ]TULO\s+[IVXLC]+$", na=False)
    keep = s.where(valid_range | valid_cap, pd.NA)

    m = _normalize_cid_principal(main_icd)
    chapter = m.str[:1].map(lambda ch: CID_CHAPTER_BY_LETTER.get(ch, pd.NA))
    out = keep.fillna(chapter)
    return out.astype("string")


def _derive_competencia(df: pd.DataFrame, ano_part: str, mes_part: str) -> pd.Series:
    index = df.index

    def _valid_competencia(serie: pd.Series) -> pd.Series:
        s = serie.astype("Int64")
        ano = (s // 100).astype("Int64")
        mes = (s % 100).astype("Int64")
        ok = (
            s.notna()
            & ano.between(1900, 2099, inclusive="both")
            & mes.between(1, 12, inclusive="both")
        )
        return s.where(ok, pd.NA)

    # SIA: pa_cmp
    pa_cmp = _pick_first_present(df, ["pa_cmp"])
    if pa_cmp is not None:
        s = _as_clean_string(pa_cmp, index=index)
        digits = s.str.replace(r"\D", "", regex=True).str[:6]
        out = _valid_competencia(pd.to_numeric(digits, errors="coerce"))
        if out.notna().any():
            return out.astype("Int64")

    # SIH: ano_cmpt + mes_cmpt (ano pode vir com 2 dígitos)
    ano = _as_clean_int(_pick_first_present(df, ["ano_cmpt"]), index=index)
    mes = _as_clean_int(_pick_first_present(df, ["mes_cmpt"]), index=index)
    ano = ano.where(ano.isna() | (ano >= 100), ano + 2000)
    comp = (ano * 100 + mes).astype("Int64")
    comp = _valid_competencia(comp)
    if comp.notna().any():
        return comp.astype("Int64")

    # Fallback: partição do arquivo
    ano_fallback = pd.to_numeric(pd.Series([ano_part] * len(df), index=index), errors="coerce").astype("Int64")
    mes_fallback = pd.to_numeric(pd.Series([mes_part] * len(df), index=index), errors="coerce").astype("Int64")
    fallback = (ano_fallback * 100 + mes_fallback).astype("Int64")
    return _valid_competencia(fallback).astype("Int64")


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

    out = out.loc[:, CANONICAL_OUTPUT_COLUMNS]
    return _normalize_output_types(out)


def _normalize_output_types(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    for col in CANONICAL_OUTPUT_COLUMNS:
        s = df[col] if col in df.columns else pd.Series(pd.NA, index=df.index)
        if col in BOOL_COLUMNS:
            out[col] = _as_clean_bool(s, index=df.index)
        elif col in INT_COLUMNS:
            out[col] = _as_clean_int(s, index=df.index)
        elif col in FLOAT_COLUMNS:
            out[col] = _as_clean_numeric(s, index=df.index)
        else:
            out[col] = _as_clean_string(s, index=df.index)

    out["sistema"] = _normalize_sistema(out["sistema"])
    # Fallback por evidência de origem quando rótulo não vier preenchido.
    is_sia = out["pa_cmp"].notna() if "pa_cmp" in out.columns else pd.Series(False, index=out.index)
    is_sih = out["n_aih"].notna() if "n_aih" in out.columns else pd.Series(False, index=out.index)
    out.loc[out["sistema"].isna() & is_sia & ~is_sih, "sistema"] = "SIA"
    out.loc[out["sistema"].isna() & is_sih & ~is_sia, "sistema"] = "SIH"
    out["ano_cmpt"] = _normalize_ano_cmpt(out["ano_cmpt"])
    out["mes_cmpt"] = _normalize_mes_cmpt(out["mes_cmpt"], competencia_ano_mes=out["competencia_ano_mes"])
    out["main_icd"] = _normalize_cid_principal(out["main_icd"])
    out["icd_group"] = _normalize_icd_group(out["icd_group"], out["main_icd"])
    out["cid_principal"] = _normalize_cid_principal(out["cid_principal"])
    out["sexo_paciente"] = _normalize_sexo(out["sexo_paciente"])
    out["raca_cor_paciente"] = _normalize_raca_cor(out["raca_cor_paciente"])
    out["cod_munic_residencia"] = _normalize_municipio_ibge6(out["cod_munic_residencia"])
    out["uf_origem"] = _normalize_uf_origem(
        out["uf_origem"], cod_munic_residencia=out["cod_munic_residencia"]
    )
    out["cod_munic_estabelecimento"] = _normalize_municipio_estabelecimento(
        out["cod_munic_estabelecimento"]
    )
    out["cnes_estabelecimento"] = _normalize_cnes(out["cnes_estabelecimento"])
    out["cod_procedimento"] = _normalize_cod_procedimento(out["cod_procedimento"])
    out["custo_total"] = _normalize_custo_total(out["custo_total"])
    return out


def _extract_partitions(raw_path: Path) -> tuple[str | None, str | None, str | None, int | None]:
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
