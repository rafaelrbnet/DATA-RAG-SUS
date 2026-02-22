"""
Transformação: lê Parquets de data/raw/ (particionado por ano, UF, sistema),
padroniza tipos, adiciona colunas derivadas (custo_total, idade_grupo, cid_capitulo)
e grava em data/processed/ com a mesma estrutura de partições.

Processa um arquivo por vez para controle de memória (16 GB RAM).
"""

from pathlib import Path

import pandas as pd

from .log_util import log

def _root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


RAW_BASE = _root() / "data" / "raw"
PROCESSED_BASE = _root() / "data" / "processed"

QUEM = "Python"
ONDE_BASE = "transform"

# Faixas de idade (padrão; pode ser configurável depois)
IDADE_GRUPOS = [
    (0, 17, "0-17"),
    (18, 59, "18-59"),
    (60, 120, "60+"),
]


def _ensure_numeric(series: pd.Series) -> pd.Series:
    """Converte para numérico, coercendo erros para NaN."""
    return pd.to_numeric(series, errors="coerce")


def _add_custo_total(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coluna derivada custo_total: soma de colunas de valor quando existirem,
    ou coluna única. Nomes comuns após clean_names (R): valor, val_tot, pa_valtot, etc.
    """
    cand = ["valor", "val_tot", "valor_total", "pa_valtot", "pa_val_ap"]
    for c in cand:
        if c in df.columns:
            df = df.copy()
            df["custo_total"] = _ensure_numeric(df[c])
            return df
    # Se não achar, tenta qualquer coluna com 'val' no nome
    for c in df.columns:
        if "val" in c.lower() and df[c].dtype in ("object", "float64", "int64", "Int64"):
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
    Coluna derivada cid_capitulo: primeiro caractere do CID (capítulo CID-10)
    a partir de main_icd / diag_princ / pa_cidpri. Ignora valores vazios ou 'nan'.
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


def _standardize_types(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza tipos: strings onde for texto, numérico onde for número."""
    df = df.copy()
    for col in df.columns:
        if col in ("custo_total", "idade_grupo", "cid_capitulo"):
            continue
        if df[col].dtype == "object":
            # mantém string; opcional: trim
            df[col] = df[col].astype(str).replace("nan", "").replace("None", "")
    return df


def transform_single_file(raw_path: Path) -> Path | None:
    """
    Lê um Parquet em raw_path, aplica transformações e grava em
    data/processed/ com a mesma estrutura de partições (ano=, uf=, sistema=).
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
        log(QUEM, ONDE_BASE, f"Path sem partições esperadas: {raw_path}")
        return None

    try:
        df = pd.read_parquet(raw_path)
    except Exception as e:
        log(QUEM, str(raw_path), f"Erro ao ler Parquet: {e}")
        return None

    if df.empty:
        log(QUEM, str(raw_path), "Arquivo vazio; gravando mesmo assim.")
    else:
        df = _add_custo_total(df)
        df = _add_idade_grupo(df)
        df = _add_cid_capitulo(df)
        df = _standardize_types(df)

    dest_dir = PROCESSED_BASE / f"ano={ano}" / f"uf={uf}" / f"sistema={sistema}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / raw_path.name

    try:
        df.to_parquet(dest_path, index=False)
        log(QUEM, str(dest_path), f"Transformado: {len(df)} linhas")
        return dest_path
    except Exception as e:
        log(QUEM, str(dest_path), f"Erro ao gravar: {e}")
        return None
    finally:
        del df


def run_transform() -> None:
    """
    Percorre data/raw/ (ano=, uf=, sistema=), processa cada .parquet
    um a um e grava em data/processed/ com as mesmas partições.
    """
    if not RAW_BASE.is_dir():
        log(QUEM, ONDE_BASE, f"Diretório inexistente: {RAW_BASE}")
        return

    raw_files = sorted(RAW_BASE.rglob("*.parquet"))
    if not raw_files:
        log(QUEM, ONDE_BASE, f"Nenhum .parquet em {RAW_BASE}")
        return

    ok = 0
    fail = 0
    for path in raw_files:
        result = transform_single_file(path)
        if result is not None:
            ok += 1
        else:
            fail += 1

    log(QUEM, ONDE_BASE, f"Concluído: {ok} processados, {fail} falhas.")


if __name__ == "__main__":
    run_transform()
