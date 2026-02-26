"""
Transformação: lê Parquets de data/raw/ (particionado por ano, UF, sistema),
aplica o pipeline de transformações e grava em data/processed/.
Um arquivo por vez para controle de memória (ex.: 16 GB RAM).

data/raw/ é a fonte da verdade; data/processed/ é camada derivada.
Os arquivos permanecem em raw — não são removidos. Qualquer reprocessamento
(novas métricas, correções) deve ser feito a partir de raw.

Estratégia "o que processar": diff entre data/raw e data/processed (não o log).
Só processamos arquivos que existem em raw mas não têm correspondente em processed.
Vantagens: fonte única de verdade (filesystem), idempotente, resistente a rotação de log.

Para adicionar nova transformação ou métrica:
  1. Crie uma função (df: pd.DataFrame) -> pd.DataFrame que receba e retorne o DataFrame.
  2. Adicione-a à lista TRANSFORM_STEPS abaixo (ordem importa).
"""

from pathlib import Path
from typing import Callable

import pandas as pd

from .log_util import log

def _root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


RAW_BASE = _root() / "data" / "raw"
PROCESSED_BASE = _root() / "data" / "processed"

QUEM = "Python"
ONDE_BASE = "transform"


def _processed_path_for_raw(raw_path: Path) -> Path:
    """Caminho em data/processed/ correspondente ao arquivo em data/raw/ (estrutura espelhada)."""
    rel = raw_path.relative_to(RAW_BASE)
    return PROCESSED_BASE / rel

# Faixas de idade (provisório). TODO: basear em artigo de referência para categorização.
IDADE_GRUPOS = [
    (0, 17, "0-17"),
    (18, 59, "18-59"),
    (60, 120, "60+"),
]


def _ensure_numeric(series: pd.Series) -> pd.Series:
    """Converte para numérico, coercendo erros para NaN. Passa por string para evitar edge cases (Arrow/object)."""
    return pd.to_numeric(series.astype(str), errors="coerce")


def _add_custo_total(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalização: uma única coluna de valor por sistema → custo_total (não é soma).
    SIA e SIH usam nomes diferentes; escolhemos a coluna apropriada e expomos como custo_total.
    SIA: pa_valpro, pa_valapr, nu_vpa_tot, nu_pa_tot. SIH: valor, val_tot, valor_total.
    """
    cand = [
        "pa_valpro", "pa_valapr", "nu_vpa_tot", "nu_pa_tot",  # SIA
        "valor", "val_tot", "valor_total", "pa_valtot", "pa_val_ap",  # SIH / alternativos
    ]
    for c in cand:
        if c in df.columns:
            df = df.copy()
            df["custo_total"] = _ensure_numeric(df[c])
            return df
    for c in df.columns:
        if "val" in c.lower() and str(df[c].dtype) in ("object", "string", "float64", "int64", "Int64"):
            df = df.copy()
            df["custo_total"] = _ensure_numeric(df[c])
            return df
    df = df.copy()
    df["custo_total"] = pd.NA
    return df


def _add_idade_grupo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coluna derivada idade_grupo a partir de coluna de idade (em anos).
    Procura: idade, nu_idade, idade_anos, etc.
    """
    df = df.copy()
    idade_col = None
    for c in ["idade", "nu_idade", "idade_anos", "pa_idade"]:
        if c in df.columns:
            idade_col = c
            break
    if idade_col is None:
        df["idade_grupo"] = None
        return df
    s = _ensure_numeric(df[idade_col])
    def classificar(v):
        if pd.isna(v):
            return None
        v = int(v)
        for lo, hi, label in IDADE_GRUPOS:
            if lo <= v <= hi:
                return label
        return "outro"
    df["idade_grupo"] = s.map(classificar)
    return df


def _add_cid_capitulo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coluna derivada cid_capitulo: primeiro caractere do CID (capítulo CID-10).
    Grupos clínicos (icd_group) vêm do R (regex em analise_ortopedia.R); referenciar
    dicionário de dados ou documentação/artigo na documentação do projeto.
    """
    df = df.copy()
    for c in ["main_icd", "diag_princ", "pa_cidpri", "cid"]:
        if c in df.columns:
            s = df[c].astype(str).str.strip()
            first = s.str[:1]
            # Só mantém se for letra (capítulo CID-10) ou dígito; senão None
            df["cid_capitulo"] = first.where(
                first.str.match(r"[A-ZU0-9]", na=False),
                None
            )
            return df
    df["cid_capitulo"] = None
    return df


# Colunas que devem ser numéricas (SIA/SIH após clean_names no R)
NUMERIC_COLUMNS = [
    "pa_idade", "idademin", "idademax",
    "pa_qtdpro", "pa_qtdapr", "pa_valpro", "pa_valapr",
    "nu_vpa_tot", "nu_pa_tot", "pa_vl_cf", "pa_vl_cl", "pa_vl_inc", "pa_dif_val",
    "valor", "val_tot", "valor_total",
    "mun_res_lat", "mun_res_lon", "mun_res_alt", "mun_res_area",
]

# Colunas de UF: padronizar para 2 letras maiúsculas
UF_COLUMNS = ["uf_origem", "pa_ufmun", "pa_ufdif", "mun_res_uf"]


def _standardize_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Converte colunas conhecidas de valor/quantidade para numérico."""
    df = df.copy()
    for col in NUMERIC_COLUMNS:
        if col in df.columns and df[col].dtype == "object":
            df[col] = _ensure_numeric(df[col])
    return df


def _standardize_uf_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza colunas de UF para 2 letras maiúsculas."""
    df = df.copy()
    for col in UF_COLUMNS:
        if col not in df.columns:
            continue
        s = df[col].astype(str).str.strip().str.upper()
        # Mantém só os 2 primeiros caracteres se for código (ex.: "12" ou "AC")
        df[col] = s.str[:2].replace("NA", "").replace("NAN", "")
    return df


def _add_data_competencia(df: pd.DataFrame) -> pd.DataFrame:
    """Coluna derivada ano_mes (YYYYMM) para ordenação/filtro por competência."""
    df = df.copy()
    ano = df.get("ano_cmpt")
    mes = df.get("mes_cmpt")
    if ano is not None and mes is not None:
        df["ano_mes"] = _ensure_numeric(ano).astype("Int64") * 100 + _ensure_numeric(mes).astype("Int64")
    else:
        df["ano_mes"] = pd.NA
    return df


def _standardize_types(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza tipos: strings trim; numéricos e UFs nas funções dedicadas."""
    df = df.copy()
    for col in df.columns:
        if col in ("custo_total", "idade_grupo", "cid_capitulo", "ano_mes"):
            continue
        if df[col].dtype == "object" or str(df[col].dtype) == "string":
            df[col] = df[col].astype(str).str.strip().replace("nan", "").replace("None", "")
    return df


# Pipeline de transformações: ordem importa. Para nova métrica/coluna, crie uma função
# (df: pd.DataFrame) -> pd.DataFrame e adicione aqui.
TRANSFORM_STEPS: list[Callable[[pd.DataFrame], pd.DataFrame]] = [
    _add_custo_total,
    _add_idade_grupo,
    _add_cid_capitulo,
    _add_data_competencia,
    _standardize_numeric_columns,
    _standardize_uf_columns,
    _standardize_types,
]


def transform_single_file(raw_path: Path) -> Path | None:
    """
    Lê o Parquet em raw_path, aplica o pipeline de transformações e grava em
    data/processed/ (mesma estrutura de partições). O arquivo em raw permanece (fonte da verdade).
    Retorna o path de destino ou None em caso de erro.
    """
    # Inferir partições a partir do caminho: .../ano=X/uf=Y/sistema=Z/arquivo.parquet
    parts = raw_path.parts
    ano = uf = sistema = None
    for p in parts:
        if p.startswith("ano="):
            ano = p.split("=", 1)[1]
        elif p.startswith("uf="):
            uf = p.split("=", 1)[1]
        elif p.startswith("sistema="):
            sistema = p.split("=", 1)[1]
    if not (ano and uf and sistema):
        log(QUEM, ONDE_BASE, f"ERRO: path sem partições esperadas: {raw_path}")
        return None

    try:
        # dtype_backend='pyarrow' mantém tipos Arrow e tende a ser mais rápido que a conversão para StringDtype
        try:
            df = pd.read_parquet(raw_path, dtype_backend="pyarrow")
        except (TypeError, ValueError):
            df = pd.read_parquet(raw_path)
    except Exception as e:
        log(QUEM, str(raw_path), f"ERRO ao ler Parquet: {e}")
        return None

    if df.empty:
        pass  # grava vazio; sem log para não poluir
    else:
        for step in TRANSFORM_STEPS:
            df = step(df)

    dest_dir = PROCESSED_BASE / f"ano={ano}" / f"uf={uf}" / f"sistema={sistema}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / raw_path.name  # mesmo nome que em raw (estrutura espelhada)

    try:
        df.to_parquet(dest_path, index=False)
        return dest_path
    except Exception as e:
        log(QUEM, str(dest_path), f"ERRO ao gravar: {e}")
        return None
    finally:
        del df


def run_transform(skip_existing: bool = True) -> None:
    """
    Processa Parquets de data/raw/ que ainda não têm correspondente em data/processed/
    (mesma estrutura de partições). Arquivos em raw permanecem (fonte da verdade).

    Estratégia: diff raw vs processed — só processa se o arquivo em processed não existir.
    Use skip_existing=False para forçar reprocessamento de todos os arquivos de raw.
    """
    if not RAW_BASE.is_dir():
        log(QUEM, ONDE_BASE, f"Diretório inexistente: {RAW_BASE}")
        print("ERRO: Diretório data/raw/ inexistente.", flush=True)
        return

    all_raw = sorted(RAW_BASE.rglob("*.parquet"))
    if skip_existing:
        raw_files = [p for p in all_raw if not _processed_path_for_raw(p).exists()]
    else:
        raw_files = all_raw

    if not raw_files:
        if not all_raw:
            log(QUEM, ONDE_BASE, f"Nenhum .parquet em {RAW_BASE}")
            print("Nenhum .parquet em data/raw/. Execute a ingestão antes.", flush=True)
        else:
            log(QUEM, ONDE_BASE, "Nenhum arquivo pendente para transform (raw já espelhado em processed).")
            print("Nenhum arquivo pendente: todos os Parquets de data/raw/ já têm correspondente em data/processed/.", flush=True)
        return

    total = len(raw_files)
    if skip_existing and len(all_raw) > total:
        print(f"Pulando {len(all_raw) - total} já em data/processed/. Processando {total} pendente(s).", flush=True)
    print(f"Iniciando transform: {total} arquivo(s) em data/raw/", flush=True)
    ok = 0
    fail = 0
    for i, path in enumerate(raw_files, start=1):
        print(f"  [{i}/{total}] Processando: {path.name}", flush=True)
        result = transform_single_file(path)
        if result is not None:
            ok += 1
        else:
            fail += 1
            print(f"       ⚠ Falha ao processar {path.name}", flush=True)

    n_processed = len(list(PROCESSED_BASE.rglob("*.parquet"))) if PROCESSED_BASE.is_dir() else 0
    log(QUEM, ONDE_BASE, f"Concluído: {ok} processados, {fail} falhas. data/processed/: {n_processed} arquivos .parquet.")
    print(f"Concluído: {ok} processados, {fail} falhas. data/processed/: {n_processed} arquivos.", flush=True)


if __name__ == "__main__":
    run_transform()
