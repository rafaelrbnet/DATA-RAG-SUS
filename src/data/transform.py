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
    "mun_res_zona",
]

SIA_SPECIFIC_COLUMNS = [
    "pa_cmp",
    "pa_mvm",
    "idademin",
    "idademax",
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
    "pa_pmdf",
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
    "etnia_paciente",
    "cnpj_mantenedora",
    "gestao_responsavel",
    "tipo_financiamento",
    "cid_secundario",
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
    "munreszona": "mun_res_zona",
    "pa_uf_mun": "pa_ufmun",
    "pa_mun_pcn": "pa_munpcn",
    "pmdf": "pa_pmdf",
    "pa_uti_int_to": "uti_int_to",
    "cgc_mant": "cnpj_mant",
    "dt_nasc": "nasc",
    "diag_sec1": "diagsec1",
    "diag_sec2": "diagsec2",
    "diag_sec3": "diagsec3",
    "diag_sec4": "diagsec4",
    "diag_sec5": "diagsec5",
    "diag_sec6": "diagsec6",
    "diag_sec7": "diagsec7",
    "diag_sec8": "diagsec8",
    "diag_sec9": "diagsec9",
    "cid_obito": "cid_morte",
    "cid_not": "cid_notif",
    "cid_causa_asso": "cid_asso",
    "diag_princ": "cid_princ",
    "tp_financ": "financ",
    "faec_tp_fin": "faec_tp",
    "just_aud": "aud_just",
    "just_sis": "sis_just",
    "seq_aih": "sequencia",
}

BOOL_COLUMNS = {"opm_flag", "fisio_flag"}
INT_COLUMNS = {
    "ano_cmpt",
    "mes_cmpt",
    "competencia_ano_mes",
    "pa_pmdf",
    "pa_qtdpro",
    "pa_qtdapr",
    "uti_mes_to",
    "uti_int_to",
    "qt_diarias",
    "dias_perm",
    "morte",
}
FLOAT_COLUMNS = {
    "mun_res_lat",
    "mun_res_lon",
    "mun_res_alt",
    "mun_res_area",
    "idademin",
    "idademax",
    "pa_valpro",
    "pa_vl_cf",
    "pa_vl_cl",
    "pa_vl_inc",
    "nu_pa_tot",
    "nu_vpa_tot",
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
    # Regra canônica estrita: somente 1 -> True e 0 -> False.
    s = _as_clean_numeric(series, index=index)
    out = pd.Series(pd.NA, index=index, dtype="boolean")
    out[s.eq(1)] = True
    out[s.eq(0)] = False
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


def _normalize_cid_asso(series: pd.Series) -> pd.Series:
    """
    Normaliza CID associado (causa externa) no SIH.
    Mantém somente CID-10 válido do Capítulo XX (V01-Y98), sem ponto.
    """
    cid = _normalize_cid_principal(series)
    out = cid.where(cid.str.match(r"^[VWXY][0-9]{2}[0-9A-Z]?$", na=False), pd.NA)
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


def _normalize_mun_res_zona(series: pd.Series) -> pd.Series:
    """
    Normaliza zona de residência do município:
    - 1/URBANA -> "1"
    - 2/RURAL -> "2"
    - 0/9/ignorado/ausente -> null
    """
    idx = series.index
    s = _as_clean_string(series, index=idx).str.upper()
    n = _as_clean_numeric(series, index=idx)

    out = pd.Series(pd.NA, index=idx, dtype="string")
    out[n.eq(1)] = "1"
    out[n.eq(2)] = "2"

    map_dict = {
        "1": "1",
        "2": "2",
        "URBANA": "1",
        "URBANO": "1",
        "U": "1",
        "RURAL": "2",
        "R": "2",
        "0": pd.NA,
        "9": pd.NA,
        "IGNORADO": pd.NA,
        "NAO INFORMADO": pd.NA,
        "NÃO INFORMADO": pd.NA,
    }
    mapped = s.map(lambda v: map_dict.get(v, pd.NA)).astype("string")
    out = out.fillna(mapped)
    return out.astype("string")


def _normalize_pa_ufdif(
    series: pd.Series,
    cod_munic_residencia: pd.Series | None = None,
    cod_munic_estabelecimento: pd.Series | None = None,
) -> pd.Series:
    """
    Normaliza indicador de divergência de UF (invasão estadual):
    - valores explícitos true/false -> "1"/"0";
    - 9/99/ignorado -> null;
    - fallback: compara UF (2 primeiros dígitos) entre municípios
      de residência e estabelecimento.
    """
    idx = series.index
    s = _as_clean_string(series, index=idx).str.upper()
    n = _as_clean_numeric(series, index=idx)

    out = pd.Series(pd.NA, index=idx, dtype="string")
    out[n.eq(1)] = "1"
    out[n.eq(0)] = "0"

    map_dict = {
        "1": "1",
        "S": "1",
        "SIM": "1",
        "TRUE": "1",
        "T": "1",
        "Y": "1",
        "YES": "1",
        "0": "0",
        "N": "0",
        "NAO": "0",
        "NÃO": "0",
        "NO": "0",
        "FALSE": "0",
        "F": "0",
        "9": pd.NA,
        "99": pd.NA,
        "IGNORADO": pd.NA,
    }
    mapped = s.map(lambda v: map_dict.get(v, pd.NA)).astype("string")
    out = out.fillna(mapped)

    if cod_munic_residencia is not None and cod_munic_estabelecimento is not None:
        res = _as_clean_string(cod_munic_residencia, index=idx).str.replace(r"\D", "", regex=True).str[:6]
        est = _as_clean_string(cod_munic_estabelecimento, index=idx).str.replace(r"\D", "", regex=True).str[:6]
        both = res.str.len().eq(6).fillna(False) & est.str.len().eq(6).fillna(False)
        diff = res.str[:2].ne(est.str[:2]).fillna(False)
        inferred = pd.Series(pd.NA, index=idx, dtype="string")
        inferred[both & diff] = "1"
        inferred[both & ~diff] = "0"
        out = out.fillna(inferred)

    return out.astype("string")


def _normalize_pa_mndif(
    series: pd.Series,
    cod_munic_residencia: pd.Series | None = None,
    cod_munic_estabelecimento: pd.Series | None = None,
) -> pd.Series:
    """
    Normaliza indicador de divergência municipal (invasão municipal):
    - valores explícitos true/false -> "1"/"0";
    - 9/99/ignorado -> null;
    - fallback: compara código do município de residência vs estabelecimento.
    """
    idx = series.index
    s = _as_clean_string(series, index=idx).str.upper()
    n = _as_clean_numeric(series, index=idx)

    out = pd.Series(pd.NA, index=idx, dtype="string")
    out[n.eq(1)] = "1"
    out[n.eq(0)] = "0"

    map_dict = {
        "1": "1",
        "S": "1",
        "SIM": "1",
        "TRUE": "1",
        "T": "1",
        "Y": "1",
        "YES": "1",
        "0": "0",
        "N": "0",
        "NAO": "0",
        "NÃO": "0",
        "NO": "0",
        "FALSE": "0",
        "F": "0",
        "9": pd.NA,
        "99": pd.NA,
        "IGNORADO": pd.NA,
    }
    mapped = s.map(lambda v: map_dict.get(v, pd.NA)).astype("string")
    out = out.fillna(mapped)

    if cod_munic_residencia is not None and cod_munic_estabelecimento is not None:
        res = _as_clean_string(cod_munic_residencia, index=idx).str.replace(r"\D", "", regex=True).str[:6]
        est = _as_clean_string(cod_munic_estabelecimento, index=idx).str.replace(r"\D", "", regex=True).str[:6]
        both = res.str.len().eq(6).fillna(False) & est.str.len().eq(6).fillna(False)
        diff = res.ne(est).fillna(False)
        inferred = pd.Series(pd.NA, index=idx, dtype="string")
        inferred[both & diff] = "1"
        inferred[both & ~diff] = "0"
        out = out.fillna(inferred)

    return out.astype("string")


def _normalize_pa_pmdf(series: pd.Series) -> pd.Series:
    """
    Normaliza PMDF (procedimento máximo diário por faturamento):
    - inteiro positivo;
    - faixa válida 1..9999;
    - 999/9999 mantidos (interpretação de sem limite depende da regra de negócio).
    """
    s = _as_clean_int(series, index=series.index)
    s = s.where(s.between(1, 9999, inclusive="both"), pd.NA)
    return s.astype("Int64")


def _normalize_cnpj14(series: pd.Series, null_zero: bool = True) -> pd.Series:
    """
    Normaliza CNPJ para 14 dígitos numéricos (sem máscara).
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    cnpj = digits.str[-14:].str.zfill(14)
    invalid = {""}
    mask = digits.isna() | digits.isin(invalid)
    if null_zero:
        mask = mask | cnpj.isin({"00000000000000"})
    out = cnpj.mask(mask, pd.NA)
    return out.astype("string")


def _normalize_pa_nat_jur(series: pd.Series) -> pd.Series:
    """
    Normaliza Natureza Jurídica (CONCLA/IBGE) para código canônico de 4 dígitos.
    Exemplo: 101-5 -> 1015.
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    code4 = digits.str[:4]
    out = code4.mask(digits.isna() | digits.eq("") | code4.str.len().lt(4) | code4.isin({"0000", "9999"}), pd.NA)
    return out.astype("string")


def _normalize_pa_cnsmed(series: pd.Series) -> pd.Series:
    """
    Normaliza CNS do profissional para 15 dígitos numéricos.
    - remove caracteres não numéricos;
    - mantém 15 dígitos (zfill quando necessário);
    - preserva 000000000000000 como sentinel permitido (ex.: BPA-C).
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    cns = digits.str[-15:].str.zfill(15)

    # CNS válidos costumam iniciar com 1,2,7,8,9; permite zeros integrais como sentinel.
    valid_prefix = cns.str[0].isin(["1", "2", "7", "8", "9"])
    sentinel_zero = cns.eq("000000000000000")
    valid = cns.str.len().eq(15).fillna(False) & (
        valid_prefix.fillna(False) | sentinel_zero.fillna(False)
    )

    out = cns.mask(digits.isna() | digits.eq("") | ~valid, pd.NA)
    return out.astype("string")


def _normalize_pa_cbocod(series: pd.Series) -> pd.Series:
    """
    Normaliza CBO do profissional para 6 dígitos (CBO 2002).
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    cbo = digits.str[-6:].str.zfill(6)
    out = cbo.mask(digits.isna() | digits.eq("") | cbo.eq("000000"), pd.NA)
    return out.astype("string")


def _normalize_nome_proced(series: pd.Series) -> pd.Series:
    """
    Normaliza nome do procedimento (descrição SIGTAP) como texto canônico.
    - trim e nulos técnicos via _as_clean_string;
    - limite defensivo de 250 caracteres.
    """
    s = _as_clean_string(series, index=series.index)
    return s.str.slice(0, 250).astype("string")


def _normalize_pa_quantidade(series: pd.Series) -> pd.Series:
    """
    Normaliza quantidade produzida/aprovada como inteiro não-negativo.
    """
    q = _as_clean_int(series, index=series.index)
    q = q.where(q >= 0, pd.NA)
    return q.astype("Int64")


def _normalize_cod_idade(series: pd.Series) -> pd.Series:
    """
    Normaliza COD_IDADE do SIH (unidade da idade).
    Domínio canônico: 2=dias, 3=meses, 4=anos.
    Aceita 1 (horas) como legado histórico.
    """
    s = _as_clean_string(series, index=series.index)
    code = s.str.replace(r"\D", "", regex=True).str[:1]
    valid = {"1", "2", "3", "4"}
    out = code.where(code.isin(valid), pd.NA)
    return out.astype("string")


def _normalize_etnia_indigena(series: pd.Series, raca_cor_paciente: pd.Series | None = None) -> pd.Series:
    """
    Normaliza código de etnia indígena (IBGE/FUNAI) para 4 dígitos.
    Regra de negócio: só é válido quando raca_cor_paciente == "05" (Indígena).
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    code = digits.str[-4:].str.zfill(4)
    out = code.mask(digits.isna() | digits.eq("") | code.eq("0000"), pd.NA)

    if raca_cor_paciente is not None:
        rc = _as_clean_string(raca_cor_paciente, index=series.index)
        out = out.where(rc.eq("05"), pd.NA)

    return out.astype("string")


def _normalize_nasc(series: pd.Series) -> pd.Series:
    """
    Normaliza data de nascimento (SIH):
    - prefere formato completo AAAAMMDD válido;
    - aceita ano isolado AAAA (quando base vier mascarada);
    - demais formatos inválidos -> null.
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    out = pd.Series(pd.NA, index=series.index, dtype="string")

    current_year = pd.Timestamp.now().year

    is_year = digits.str.fullmatch(r"\d{4}", na=False)
    year_num = pd.to_numeric(digits.where(is_year), errors="coerce")
    valid_year = is_year & year_num.between(1900, current_year, inclusive="both")
    out.loc[valid_year] = digits.loc[valid_year]

    is_date = digits.str.fullmatch(r"\d{8}", na=False)
    dt = pd.to_datetime(digits.where(is_date), format="%Y%m%d", errors="coerce")
    valid_date = dt.notna() & dt.dt.year.between(1900, current_year, inclusive="both")
    out.loc[valid_date] = digits.loc[valid_date]

    return out.astype("string")


def _normalize_cep(series: pd.Series) -> pd.Series:
    """
    Normaliza CEP de residência (SIH):
    - mantém apenas dígitos;
    - aceita CEP completo com 8 dígitos;
    - aceita CEP mascarado com 5 dígitos (bases públicas/LGPD);
    - demais formatos inválidos -> null.
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    valid = digits.str.fullmatch(r"\d{8}|\d{5}", na=False)
    out = digits.where(valid, pd.NA)
    out = out.mask(out.eq("00000000") | out.eq("00000"), pd.NA)
    return out.astype("string")


def _normalize_nacional(series: pd.Series) -> pd.Series:
    """
    Normaliza nacionalidade no SIH.
    Domínio canônico:
      010 = brasileiro
      020 = naturalizado
      030 = estrangeiro
      999 = ignorado (normalizado para null)
    """
    s = _as_clean_string(series, index=series.index).str.upper()
    digits = s.str.replace(r"\D", "", regex=True)
    code3 = digits.str[:3].str.zfill(3)

    # Suporte a layouts/extrações curtas (01, 02, 03).
    short_map = {"01": "010", "02": "020", "03": "030"}
    mapped_short = digits.map(lambda v: short_map.get(v, pd.NA)).astype("string")

    valid = {"010", "020", "030", "999"}
    out = code3.where(code3.isin(valid), pd.NA).fillna(mapped_short)
    out = out.mask(out.eq("999"), pd.NA)
    return out.astype("string")


def _normalize_pa_valpro(series: pd.Series) -> pd.Series:
    """
    Normaliza valor de produção (monetário) para Float64.
    Suporta vírgula/ponto decimal e padrão misto.
    """
    s = _as_clean_string(series, index=series.index)
    compact = s.str.replace(r"\s+", "", regex=True)

    has_dot = compact.str.contains(r"\.", na=False)
    has_comma = compact.str.contains(",", na=False)
    mixed = has_dot & has_comma
    normalized = compact.where(~mixed, compact.str.replace(".", "", regex=False))
    normalized = normalized.str.replace(",", ".", regex=False)

    v = pd.to_numeric(normalized, errors="coerce").astype("Float64")
    v = v.where(v >= 0, pd.NA)
    return v


def _normalize_pa_vl_cf(series: pd.Series) -> pd.Series:
    """
    Normaliza valor de complemento federal (monetário) para Float64.
    """
    return _normalize_pa_valpro(series)


def _normalize_pa_vl_cl(series: pd.Series) -> pd.Series:
    """
    Normaliza valor de complemento local (monetário) para Float64.
    """
    return _normalize_pa_valpro(series)


def _normalize_pa_vl_inc(series: pd.Series) -> pd.Series:
    """
    Normaliza valor de incremento (monetário) para Float64.
    """
    return _normalize_pa_valpro(series)


def _normalize_nu_pa_tot(series: pd.Series) -> pd.Series:
    """
    Normaliza valor total aprovado (NU_PA_TOT) para Float64.
    """
    return _normalize_pa_valpro(series)


def _normalize_val_sh(series: pd.Series) -> pd.Series:
    """
    Normaliza valor do serviço hospitalar (SIH) como monetário Float64.
    """
    return _normalize_pa_valpro(series)


def _normalize_val_sp(series: pd.Series) -> pd.Series:
    """
    Normaliza valor do serviço profissional (SIH) como monetário Float64.
    """
    return _normalize_pa_valpro(series)


def _normalize_val_sadt(series: pd.Series) -> pd.Series:
    """
    Normaliza valor de SADT (SIH) como monetário Float64.
    """
    return _normalize_pa_valpro(series)


def _normalize_val_ortp(series: pd.Series) -> pd.Series:
    """
    Normaliza valor de OPM/OPME (SIH) como monetário Float64.
    """
    return _normalize_pa_valpro(series)


def _normalize_val_uti(series: pd.Series) -> pd.Series:
    """
    Normaliza valor de diárias de UTI (SIH) como monetário Float64.
    """
    return _normalize_pa_valpro(series)


def _normalize_val_uci(series: pd.Series) -> pd.Series:
    """
    Normaliza valor de diárias de UCI (SIH) como monetário Float64.
    """
    return _normalize_pa_valpro(series)


def _normalize_val_sangue(series: pd.Series) -> pd.Series:
    """
    Normaliza valor de hemocomponentes/hemoderivados (SIH) como monetário Float64.
    """
    return _normalize_pa_valpro(series)


def _normalize_val_acomp(series: pd.Series) -> pd.Series:
    """
    Normaliza valor de diárias de acompanhante (SIH) como monetário Float64.
    """
    return _normalize_pa_valpro(series)


def _normalize_pa_docorig(series: pd.Series) -> pd.Series:
    """
    Normaliza documento de origem da produção ambulatorial.
    Domínio canônico principal: C, I, A, R.
    """
    s = _as_clean_string(series, index=series.index).str.upper()
    valid = {"C", "I", "A", "R"}
    out = s.where(s.isin(valid), pd.NA)
    return out.astype("string")


def _normalize_uti_int_to(series: pd.Series) -> pd.Series:
    """
    Normaliza total de diárias em UTI como inteiro não-negativo.
    """
    v = _as_clean_int(series, index=series.index)
    v = v.where(v.between(0, 999, inclusive="both"), pd.NA)
    return v.astype("Int64")


def _normalize_uti_mes_to(series: pd.Series) -> pd.Series:
    """
    Normaliza total de diárias de UTI na competência/mês.
    """
    v = _as_clean_int(series, index=series.index)
    v = v.where(v.between(0, 999, inclusive="both"), pd.NA)
    return v.astype("Int64")


def _normalize_qt_diarias(series: pd.Series) -> pd.Series:
    """
    Normaliza quantidade de diárias de internação no SIH.
    """
    v = _as_clean_int(series, index=series.index)
    v = v.where(v.between(0, 999, inclusive="both"), pd.NA)
    return v.astype("Int64")


def _parse_sih_date(series: pd.Series) -> pd.Series:
    """
    Faz parse defensivo de datas SIH (preferência YYYYMMDD, fallback DDMMYYYY).
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True).str[:8]
    dt = pd.to_datetime(digits, format="%Y%m%d", errors="coerce")
    alt = pd.to_datetime(digits, format="%d%m%Y", errors="coerce")
    return dt.fillna(alt)


def _normalize_sih_date(series: pd.Series) -> pd.Series:
    """
    Normaliza data SIH para string canônica AAAAMMDD.
    """
    dt = _parse_sih_date(series)
    out = pd.Series(pd.NA, index=series.index, dtype="string")
    valid = dt.notna()
    out.loc[valid] = dt.loc[valid].dt.strftime("%Y%m%d")
    return out.astype("string")


def _normalize_dias_perm(
    series: pd.Series,
    dt_inter: pd.Series | None = None,
    dt_saida: pd.Series | None = None,
) -> pd.Series:
    """
    Normaliza dias de permanência no SIH.
    """
    v = _as_clean_int(series, index=series.index)
    v = v.where(v.between(0, 999, inclusive="both"), pd.NA)

    # Fallback de derivação quando o campo vier ausente/inválido:
    # dias_perm = dt_saida - dt_inter (diferença em dias, sem negativos).
    if dt_inter is not None and dt_saida is not None:
        dti = _parse_sih_date(dt_inter)
        dts = _parse_sih_date(dt_saida)
        delta = (dts - dti).dt.days
        delta = pd.to_numeric(delta, errors="coerce").astype("Int64")
        delta = delta.where(delta.between(0, 999, inclusive="both"), pd.NA)
        v = v.fillna(delta)

    return v.astype("Int64")


def _normalize_pa_autoriz(series: pd.Series, pa_docorig: pd.Series | None = None) -> pd.Series:
    """
    Normaliza número de autorização/APAC para 13 dígitos numéricos.
    Regra de negócio:
    - quando PA_DOCORIG = 'A', exige autorização válida (13 dígitos != zeros);
    - quando origem != 'A', valor pode ficar nulo ou zeros sentinela.
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    auth = digits.str[-13:].str.zfill(13)
    out = auth.mask(digits.isna() | digits.eq(""), pd.NA)

    if pa_docorig is not None:
        doc = _as_clean_string(pa_docorig, index=series.index).str.upper()
        is_apac = doc.eq("A")
        # Em APAC, zeros não representam autorização válida.
        out = out.mask(is_apac & out.eq("0000000000000"), pd.NA)

    return out.astype("string")


def _normalize_pa_catend(series: pd.Series) -> pd.Series:
    """
    Normaliza caráter de atendimento para domínio canônico SIGTAP (01..06).
    """
    s = _as_clean_string(series, index=series.index).str.upper()
    digits = s.str.replace(r"\D", "", regex=True)
    code = digits.str[:2].str.zfill(2)

    map_txt = {
        "ELETIVO": "01",
        "URGÊNCIA": "02",
        "URGENCIA": "02",
    }
    txt = s.map(lambda v: map_txt.get(v, pd.NA)).astype("string")

    valid = {"01", "02", "03", "04", "05", "06"}
    out = code.where(code.isin(valid), pd.NA).fillna(txt)
    return out.astype("string")


def _normalize_pa_motsai(series: pd.Series) -> pd.Series:
    """
    Normaliza motivo de saída/desfecho do atendimento (2 dígitos).
    Domínio principal usado em APAC/RAAS/SIA.
    """
    s = _as_clean_string(series, index=series.index).str.upper()
    digits = s.str.replace(r"\D", "", regex=True)
    code = digits.str[:2].str.zfill(2)

    map_txt = {
        "ALTA CURA": "11",
        "ALTA MELHORA": "12",
        "ALTA A PEDIDO": "14",
        "ALTA EXAME": "15",
        "ALTA OUTROS": "16",
        "PERMANENCIA": "21",
        "PERMANÊNCIA": "21",
        "TRANSFERENCIA": "31",
        "TRANSFERÊNCIA": "31",
        "OBITO": "41",
        "ÓBITO": "41",
        "ABANDONO": "51",
    }
    txt = s.map(lambda v: map_txt.get(v, pd.NA)).astype("string")

    valid = {"11", "12", "14", "15", "16", "21", "31", "41", "51"}
    out = code.where(code.isin(valid), pd.NA).fillna(txt)
    return out.astype("string")


def _normalize_pa_indica(series: pd.Series) -> pd.Series:
    """
    Normaliza indicador de processamento do registro.
    Domínio canônico: 0 (aprovado), 1 (advertência), 2 (rejeitado/glosa).
    """
    s = _as_clean_string(series, index=series.index).str.upper()
    digits = s.str.replace(r"\D", "", regex=True)
    code = digits.str[:1]

    map_txt = {
        "APROVADO": "0",
        "ADVERTENCIA": "1",
        "ADVERTÊNCIA": "1",
        "REJEITADO": "2",
        "GLOSADO": "2",
    }
    txt = s.map(lambda v: map_txt.get(v, pd.NA)).astype("string")

    valid = {"0", "1", "2"}
    out = code.where(code.isin(valid), pd.NA).fillna(txt)
    return out.astype("string")


def _normalize_morte(series: pd.Series, cobranca: pd.Series | None = None) -> pd.Series:
    """
    Normaliza indicador de óbito na internação (SIH) para binário 0/1.
    Fallback opcional por motivo de saída/cobrança (41, 42, 43 -> óbito).
    """
    idx = series.index
    s = _as_clean_string(series, index=idx).str.upper()
    digits = s.str.replace(r"\D", "", regex=True).str[:1]

    out = pd.Series(pd.NA, index=idx, dtype="Int64")
    out.loc[digits.eq("1")] = 1
    out.loc[digits.eq("0")] = 0

    txt_map = {
        "SIM": 1,
        "NAO": 0,
        "NÃO": 0,
        "OBITO": 1,
        "ÓBITO": 1,
    }
    mapped = s.map(lambda v: txt_map.get(v, pd.NA)).astype("Int64")
    out = out.fillna(mapped)

    if cobranca is not None:
        cb = _as_clean_string(cobranca, index=idx).str.replace(r"\D", "", regex=True).str[:2]
        is_obito = cb.isin({"41", "42", "43"})
        out = out.mask(out.isna() & is_obito, 1)
        out = out.mask(out.isna() & cb.notna() & ~is_obito, 0)

    return out.astype("Int64")


def _normalize_cobranca(series: pd.Series) -> pd.Series:
    """
    Normaliza tipo de cobrança/status da AIH no SIH.
    Campo canônico em 2 dígitos.
    """
    s = _as_clean_string(series, index=series.index).str.upper()
    digits = s.str.replace(r"\D", "", regex=True)
    code = digits.str[:2].str.zfill(2)

    map_txt = {
        "ALTA": "11",
        "PERMANENCIA": "21",
        "PERMANÊNCIA": "21",
        "TRANSFERENCIA": "31",
        "TRANSFERÊNCIA": "31",
        "OBITO": "41",
        "ÓBITO": "41",
        "ENCERRAMENTO": "51",
    }
    txt = s.map(lambda v: map_txt.get(v, pd.NA)).astype("string")

    valid = {"11", "12", "13", "14", "15", "16", "21", "31", "41", "42", "43", "51"}
    out = code.where(code.isin(valid), pd.NA).fillna(txt)
    return out.astype("string")


def _normalize_financ(series: pd.Series) -> pd.Series:
    """
    Normaliza bloco de financiamento no SIH.
    Campo canônico em 2 dígitos.
    """
    s = _as_clean_string(series, index=series.index).str.upper()
    digits = s.str.replace(r"\D", "", regex=True)
    code = digits.str[:2].str.zfill(2)

    map_txt = {
        "ATENCAO BASICA": "01",
        "ATENÇÃO BÁSICA": "01",
        "MAC": "04",
        "MEDIA E ALTA COMPLEXIDADE": "04",
        "MÉDIA E ALTA COMPLEXIDADE": "04",
        "FAEC": "05",
        "INCENTIVOS": "07",
    }
    txt = s.map(lambda v: map_txt.get(v, pd.NA)).astype("string")

    valid = {"01", "04", "05", "07"}
    out = code.where(code.isin(valid), pd.NA).fillna(txt)
    return out.astype("string")


def _normalize_faec_tp(series: pd.Series, financ: pd.Series | None = None) -> pd.Series:
    """
    Normaliza subtipo FAEC no SIH.
    Aceita layouts com 6 dígitos (preferencial) e 4 dígitos (reduzido).
    Regra de negócio: só mantém valor quando FINANC = "05" (FAEC).
    """
    s = _as_clean_string(series, index=series.index).str.upper()
    digits = s.str.replace(r"\D", "", regex=True)

    code6 = digits.where(digits.str.len().eq(6))
    code4 = digits.where(digits.str.len().eq(4))

    map_txt = {
        "TRANSPLANTES": "050001",
        "NEFROLOGIA": "050002",
        "TERAPIA RENAL SUBSTITUTIVA": "050002",
        "CIRURGIAS ELETIVAS": "050003",
        "TRIAGEM NEONATAL": "050005",
    }
    txt = s.map(lambda v: map_txt.get(v, pd.NA)).astype("string")

    out = code6.fillna(code4).fillna(txt)
    out = out.mask(out.isin({"0000", "000000"}), pd.NA)

    # Domínio esperado de subtipos FAEC inicia com "05".
    out = out.where(out.str.match(r"^05\d{2}$|^05\d{4}$", na=False), pd.NA)

    if financ is not None:
        fin = _as_clean_string(financ, index=series.index)
        out = out.where(fin.eq("05"), pd.NA)

    return out.astype("string")


def _normalize_aud_just(series: pd.Series) -> pd.Series:
    """
    Normaliza justificativa de auditoria do SIH (AUD_JUST/JUST_AUD).
    Suporta código curto (1-2) e texto de observação (até 40 chars).
    """
    s = _as_clean_string(series, index=series.index)
    upper = s.str.upper()

    # Sentinelas comuns de "não se aplica"/sem intervenção.
    sentinel = {
        "0",
        "00",
        "000",
        "NA",
        "N/A",
        "NAO SE APLICA",
        "NÃO SE APLICA",
        "SEM JUSTIFICATIVA",
        "SEM INTERVENCAO",
        "SEM INTERVENÇÃO",
    }
    out = s.mask(upper.isin(sentinel), pd.NA)

    # Limite defensivo para layouts textuais expandidos.
    return out.str.slice(0, 40).astype("string")


def _normalize_sequencia(series: pd.Series) -> pd.Series:
    """
    Normaliza sequência técnica da AIH (SEQUENCIA/SEQ_AIH).
    Saída canônica em 3 dígitos (001..999).
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    seq = digits.str[:3].str.zfill(3)

    is_three_digits = seq.str.match(r"^[0-9]{3}$", na=False)
    not_zero = seq.fillna("").ne("000")
    valid = is_three_digits & not_zero
    out = seq.where(valid, pd.NA)
    return out.astype("string")


def _normalize_sis_just(series: pd.Series) -> pd.Series:
    """
    Normaliza justificativa automática do sistema (SIS_JUST/JUST_SIS) no SIH.
    Preferência por código de crítica em 3 dígitos; aceita texto curto expandido.
    """
    s = _as_clean_string(series, index=series.index)
    upper = s.str.upper()

    digits = s.str.replace(r"\D", "", regex=True)
    code3 = digits.str[:3].str.zfill(3)
    is_code3 = digits.str.len().between(1, 3, inclusive="both")

    out = pd.Series(pd.NA, index=series.index, dtype="string")
    out = out.mask(~out.notna(), pd.NA)
    out.loc[is_code3.fillna(False)] = code3.loc[is_code3.fillna(False)]

    # Mantém texto quando não for código numérico curto.
    out = out.fillna(s)

    sentinel = {
        "0",
        "00",
        "000",
        "NA",
        "N/A",
        "NAO SE APLICA",
        "NÃO SE APLICA",
        "SEM CRITICA",
        "SEM CRÍTICA",
        "SEM JUSTIFICATIVA",
    }
    out = out.mask(upper.isin(sentinel), pd.NA)

    # Limite defensivo para layouts textuais expandidos.
    return out.str.slice(0, 40).astype("string")


def _normalize_pa_tpfin(series: pd.Series) -> pd.Series:
    """
    Normaliza tipo de financiamento (SIGTAP/SIA).
    Domínio canônico principal: 01, 02, 04, 05, 06, 07.
    """
    s = _as_clean_string(series, index=series.index).str.upper()
    digits = s.str.replace(r"\D", "", regex=True)
    code = digits.str[:2].str.zfill(2)

    map_txt = {
        "PAB": "01",
        "ATENCAO BASICA": "01",
        "ATENÇÃO BÁSICA": "01",
        "ASSISTENCIA FARMACEUTICA": "02",
        "ASSISTÊNCIA FARMACÊUTICA": "02",
        "MAC": "04",
        "FRACAO MAC": "04",
        "FRAÇÃO MAC": "04",
        "FAEC": "05",
        "VIGILANCIA EM SAUDE": "06",
        "VIGILÂNCIA EM SAÚDE": "06",
        "INCENTIVOS": "07",
    }
    txt = s.map(lambda v: map_txt.get(v, pd.NA)).astype("string")

    valid = {"01", "02", "04", "05", "06", "07"}
    out = code.where(code.isin(valid), pd.NA).fillna(txt)
    return out.astype("string")


def _normalize_pa_subfin(series: pd.Series, pa_tpfin: pd.Series | None = None) -> pd.Series:
    """
    Normaliza subtipo de financiamento para 4 dígitos.
    Regras:
    - mantém código numérico com zfill(4);
    - se PA_TPFIN estiver disponível, valida compatibilidade por prefixo (2 primeiros dígitos);
    - 0000 é permitido como "sem detalhamento".
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    code = digits.str[:4].str.zfill(4)
    out = code.mask(digits.isna() | digits.eq("") | code.str.len().lt(4), pd.NA)

    if pa_tpfin is not None:
        tp = _as_clean_string(pa_tpfin, index=series.index).str.replace(r"\D", "", regex=True).str[:2].str.zfill(2)
        # 0000 permanece válido; demais devem casar com prefixo do tipo de financiamento.
        mismatch = (
            out.notna()
            & out.ne("0000")
            & tp.notna()
            & out.str[:2].ne(tp).fillna(False)
        )
        out = out.mask(mismatch, pd.NA)

    return out.astype("string")


def _normalize_pa_gestao(series: pd.Series) -> pd.Series:
    """
    Normaliza esfera de gestão do estabelecimento/processamento.
    Domínio canônico: M (municipal), E (estadual), D (dupla).
    """
    s = _as_clean_string(series, index=series.index).str.upper()

    map_txt = {
        "M": "M",
        "MUNICIPAL": "M",
        "E": "E",
        "ESTADUAL": "E",
        "D": "D",
        "DUPLA": "D",
    }
    out = s.map(lambda v: map_txt.get(v, pd.NA)).astype("string")
    return out


def _normalize_n_aih(series: pd.Series) -> pd.Series:
    """
    Normaliza número da AIH para 13 dígitos numéricos (texto).
    Estrutura mínima validada:
    - UF: 2 dígitos (prefixo IBGE conhecido)
    - Ano: 2 dígitos
    - Tipo AIH: 1 dígito
    - Sequencial + DV: 8 dígitos
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    aih = digits.str[-13:].str.zfill(13)

    uf = aih.str[:2]
    ano = aih.str[2:4]
    tipo = aih.str[4:5]
    seqdv = aih.str[5:13]

    valid_uf = uf.isin(UF_CODE_TO_SIGLA.keys())
    valid_ano = ano.str.match(r"^\d{2}$", na=False)
    valid_tipo = tipo.str.match(r"^\d$", na=False)
    valid_seqdv = seqdv.str.match(r"^\d{8}$", na=False)

    not_zero = aih.fillna("").ne("0000000000000")
    valid = (
        valid_uf.fillna(False)
        & valid_ano.fillna(False)
        & valid_tipo.fillna(False)
        & valid_seqdv.fillna(False)
        & not_zero
    )
    out = aih.mask(digits.isna() | digits.eq("") | ~valid, pd.NA)
    return out.astype("string")


def _normalize_pa_grupo(series: pd.Series, cod_procedimento: pd.Series | None = None) -> pd.Series:
    """
    Normaliza grupo SIGTAP para 2 dígitos.
    Fallback: primeiros 2 dígitos de cod_procedimento.
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    grp = digits.str[:2]
    out = grp.mask(digits.isna() | digits.eq("") | grp.str.len().lt(2) | grp.isin({"00", "99"}), pd.NA)

    if cod_procedimento is not None:
        cp = _normalize_cod_procedimento(cod_procedimento)
        inferred = cp.str[:2]
        inferred = inferred.mask(inferred.isin({"00", "99"}), pd.NA)
        out = out.fillna(inferred)

    return out.astype("string")


def _normalize_pa_subgru(series: pd.Series, cod_procedimento: pd.Series | None = None) -> pd.Series:
    """
    Normaliza subgrupo SIGTAP para 2 dígitos (3o e 4o dígitos do código).
    Fallback: dígitos [2:4] de cod_procedimento.
    """
    s = _as_clean_string(series, index=series.index)
    digits = s.str.replace(r"\D", "", regex=True)
    sub = digits.str[:2]
    out = sub.mask(digits.isna() | digits.eq("") | sub.str.len().lt(2) | sub.isin({"00", "99"}), pd.NA)

    if cod_procedimento is not None:
        cp = _normalize_cod_procedimento(cod_procedimento)
        inferred = cp.str[2:4]
        inferred = inferred.mask(inferred.isin({"00", "99"}), pd.NA)
        out = out.fillna(inferred)

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
    out["etnia_paciente"] = _as_clean_string(
        _pick_first_present(df, ["pa_etnia", "etnia"]), index=df.index
    )
    out["cnpj_mantenedora"] = _as_clean_string(
        _pick_first_present(df, ["pa_cnpjmnt", "cnpj_mant"]), index=df.index
    )
    out["gestao_responsavel"] = _as_clean_string(
        _pick_first_present(df, ["pa_gestao", "gestao"]), index=df.index
    )
    out["tipo_financiamento"] = _as_clean_string(
        _pick_first_present(df, ["pa_tpfin", "financ"]), index=df.index
    )
    out["cid_secundario"] = _as_clean_string(
        _pick_first_present(df, ["pa_cidsec", "diag_secun"]), index=df.index
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
        _pick_first_present(df, ["pa_cidpri", "diag_princ", "cid_princ"]), index=df.index
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
    if "pa_cidsec" in out.columns:
        out["pa_cidsec"] = _normalize_cid_principal(out["pa_cidsec"])
    if "pa_cidcas" in out.columns:
        out["pa_cidcas"] = _normalize_cid_principal(out["pa_cidcas"])
    if "diag_secun" in out.columns:
        out["diag_secun"] = _normalize_cid_principal(out["diag_secun"])
    for col in ["diagsec1", "diagsec2", "diagsec3", "diagsec4", "diagsec5", "diagsec6", "diagsec7", "diagsec8", "diagsec9"]:
        if col in out.columns:
            out[col] = _normalize_cid_principal(out[col])
    if "cid_morte" in out.columns:
        out["cid_morte"] = _normalize_cid_principal(out["cid_morte"])
    if "cid_notif" in out.columns:
        out["cid_notif"] = _normalize_cid_principal(out["cid_notif"])
    if "cid_asso" in out.columns:
        out["cid_asso"] = _normalize_cid_asso(out["cid_asso"])
    if "cid_princ" in out.columns:
        out["cid_princ"] = _normalize_cid_principal(out["cid_princ"])
    if "proc_solic" in out.columns:
        out["proc_solic"] = _normalize_cod_procedimento(out["proc_solic"])
    if "val_sh" in out.columns:
        out["val_sh"] = _normalize_val_sh(out["val_sh"])
    if "val_sp" in out.columns:
        out["val_sp"] = _normalize_val_sp(out["val_sp"])
    if "val_sadt" in out.columns:
        out["val_sadt"] = _normalize_val_sadt(out["val_sadt"])
    if "val_ortp" in out.columns:
        out["val_ortp"] = _normalize_val_ortp(out["val_ortp"])
    if "val_uti" in out.columns:
        out["val_uti"] = _normalize_val_uti(out["val_uti"])
    if "val_uci" in out.columns:
        out["val_uci"] = _normalize_val_uci(out["val_uci"])
    if "val_sangue" in out.columns:
        out["val_sangue"] = _normalize_val_sangue(out["val_sangue"])
    if "val_acomp" in out.columns:
        out["val_acomp"] = _normalize_val_acomp(out["val_acomp"])
    if "uti_mes_to" in out.columns:
        out["uti_mes_to"] = _normalize_uti_mes_to(out["uti_mes_to"])
    out["sexo_paciente"] = _normalize_sexo(out["sexo_paciente"])
    out["raca_cor_paciente"] = _normalize_raca_cor(out["raca_cor_paciente"])
    # Regra canônica: etnia do paciente unificada (SIA/SIH) só é aplicável para raça/cor = 05.
    if "etnia_paciente" in out.columns:
        out["etnia_paciente"] = _normalize_etnia_indigena(
            out["etnia_paciente"], raca_cor_paciente=out["raca_cor_paciente"]
        )
    if "cnpj_mantenedora" in out.columns:
        out["cnpj_mantenedora"] = _normalize_cnpj14(out["cnpj_mantenedora"])
    if "gestao_responsavel" in out.columns:
        out["gestao_responsavel"] = _normalize_pa_gestao(out["gestao_responsavel"])
    if "tipo_financiamento" in out.columns:
        out["tipo_financiamento"] = _normalize_pa_tpfin(out["tipo_financiamento"])
    if "cid_secundario" in out.columns:
        out["cid_secundario"] = _normalize_cid_principal(out["cid_secundario"])
    out["cod_munic_residencia"] = _normalize_municipio_ibge6(out["cod_munic_residencia"])
    if "munic_mov" in out.columns:
        out["munic_mov"] = _normalize_municipio_ibge6(out["munic_mov"])
    if "cod_idade" in out.columns:
        out["cod_idade"] = _normalize_cod_idade(out["cod_idade"])
    if "nasc" in out.columns:
        out["nasc"] = _normalize_nasc(out["nasc"])
    if "dt_inter" in out.columns:
        out["dt_inter"] = _normalize_sih_date(out["dt_inter"])
    if "dt_saida" in out.columns:
        out["dt_saida"] = _normalize_sih_date(out["dt_saida"])
    if "cep" in out.columns:
        out["cep"] = _normalize_cep(out["cep"])
    if "nacional" in out.columns:
        out["nacional"] = _normalize_nacional(out["nacional"])
    out["uf_origem"] = _normalize_uf_origem(
        out["uf_origem"], cod_munic_residencia=out["cod_munic_residencia"]
    )
    out["cod_munic_estabelecimento"] = _normalize_municipio_estabelecimento(
        out["cod_munic_estabelecimento"]
    )
    if "pa_ufdif" in out.columns:
        out["pa_ufdif"] = _normalize_pa_ufdif(
            out["pa_ufdif"],
            cod_munic_residencia=out["cod_munic_residencia"],
            cod_munic_estabelecimento=out["cod_munic_estabelecimento"],
        )
    if "pa_mndif" in out.columns:
        out["pa_mndif"] = _normalize_pa_mndif(
            out["pa_mndif"],
            cod_munic_residencia=out["cod_munic_residencia"],
            cod_munic_estabelecimento=out["cod_munic_estabelecimento"],
        )
    if "pa_pmdf" in out.columns:
        out["pa_pmdf"] = _normalize_pa_pmdf(out["pa_pmdf"])
    if "pa_qtdpro" in out.columns:
        out["pa_qtdpro"] = _normalize_pa_quantidade(out["pa_qtdpro"])
    if "pa_qtdapr" in out.columns:
        out["pa_qtdapr"] = _normalize_pa_quantidade(out["pa_qtdapr"])
    if "pa_valpro" in out.columns:
        out["pa_valpro"] = _normalize_pa_valpro(out["pa_valpro"])
    if "pa_vl_cf" in out.columns:
        out["pa_vl_cf"] = _normalize_pa_vl_cf(out["pa_vl_cf"])
    if "pa_vl_cl" in out.columns:
        out["pa_vl_cl"] = _normalize_pa_vl_cl(out["pa_vl_cl"])
    if "pa_vl_inc" in out.columns:
        out["pa_vl_inc"] = _normalize_pa_vl_inc(out["pa_vl_inc"])
    if "nu_pa_tot" in out.columns:
        out["nu_pa_tot"] = _normalize_nu_pa_tot(out["nu_pa_tot"])
    if "pa_docorig" in out.columns:
        out["pa_docorig"] = _normalize_pa_docorig(out["pa_docorig"])
    if "pa_autoriz" in out.columns:
        out["pa_autoriz"] = _normalize_pa_autoriz(out["pa_autoriz"], pa_docorig=out.get("pa_docorig"))
    if "pa_catend" in out.columns:
        out["pa_catend"] = _normalize_pa_catend(out["pa_catend"])
    if "pa_motsai" in out.columns:
        out["pa_motsai"] = _normalize_pa_motsai(out["pa_motsai"])
    if "cobranca" in out.columns:
        out["cobranca"] = _normalize_cobranca(out["cobranca"])
    if "morte" in out.columns:
        out["morte"] = _normalize_morte(out["morte"], cobranca=out.get("cobranca"))
    if "pa_indica" in out.columns:
        out["pa_indica"] = _normalize_pa_indica(out["pa_indica"])
    if "pa_tpfin" in out.columns:
        out["pa_tpfin"] = _normalize_pa_tpfin(out["pa_tpfin"])
    if "pa_subfin" in out.columns:
        out["pa_subfin"] = _normalize_pa_subfin(out["pa_subfin"], pa_tpfin=out.get("pa_tpfin"))
    if "pa_gestao" in out.columns:
        out["pa_gestao"] = _normalize_pa_gestao(out["pa_gestao"])
    if "gestao" in out.columns:
        out["gestao"] = _normalize_pa_gestao(out["gestao"])
    if "financ" in out.columns:
        out["financ"] = _normalize_financ(out["financ"])
    if "faec_tp" in out.columns:
        out["faec_tp"] = _normalize_faec_tp(out["faec_tp"], financ=out.get("financ"))
    if "aud_just" in out.columns:
        out["aud_just"] = _normalize_aud_just(out["aud_just"])
    if "sis_just" in out.columns:
        out["sis_just"] = _normalize_sis_just(out["sis_just"])
    if "sequencia" in out.columns:
        out["sequencia"] = _normalize_sequencia(out["sequencia"])
    if "n_aih" in out.columns:
        out["n_aih"] = _normalize_n_aih(out["n_aih"])
    if "cgc_hosp" in out.columns:
        out["cgc_hosp"] = _normalize_cnpj14(out["cgc_hosp"])
    if "cnpj_mant" in out.columns:
        out["cnpj_mant"] = _normalize_cnpj14(out["cnpj_mant"])
    if "uti_int_to" in out.columns:
        out["uti_int_to"] = _normalize_uti_int_to(out["uti_int_to"])
    if "qt_diarias" in out.columns:
        out["qt_diarias"] = _normalize_qt_diarias(out["qt_diarias"])
    if "dias_perm" in out.columns:
        out["dias_perm"] = _normalize_dias_perm(
            out["dias_perm"],
            dt_inter=out.get("dt_inter"),
            dt_saida=out.get("dt_saida"),
        )
    if "pa_cnpjcpf" in out.columns:
        out["pa_cnpjcpf"] = _normalize_cnpj14(out["pa_cnpjcpf"])
    if "pa_cnpjmnt" in out.columns:
        # No layout SIA, zeros podem significar ausência de mantenedora vinculada.
        out["pa_cnpjmnt"] = _normalize_cnpj14(out["pa_cnpjmnt"], null_zero=False)
    if "pa_nat_jur" in out.columns:
        out["pa_nat_jur"] = _normalize_pa_nat_jur(out["pa_nat_jur"])
    if "pa_cnsmed" in out.columns:
        out["pa_cnsmed"] = _normalize_pa_cnsmed(out["pa_cnsmed"])
    if "pa_cbocod" in out.columns:
        out["pa_cbocod"] = _normalize_pa_cbocod(out["pa_cbocod"])
    if "nome_proced" in out.columns:
        out["nome_proced"] = _normalize_nome_proced(out["nome_proced"])
    if "pa_grupo" in out.columns:
        out["pa_grupo"] = _normalize_pa_grupo(out["pa_grupo"], cod_procedimento=out["cod_procedimento"])
    if "pa_subgru" in out.columns:
        out["pa_subgru"] = _normalize_pa_subgru(out["pa_subgru"], cod_procedimento=out["cod_procedimento"])
    out["cnes_estabelecimento"] = _normalize_cnes(out["cnes_estabelecimento"])
    out["cod_procedimento"] = _normalize_cod_procedimento(out["cod_procedimento"])
    out["custo_total"] = _normalize_custo_total(out["custo_total"])
    out["mun_res_zona"] = _normalize_mun_res_zona(out["mun_res_zona"])
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
