"""
Ingestão DATASUS 100% em Python: download (FTP), descompressão DBC→DBF,
processamento em chunks e gravação em data/raw/. Não depende do R.

Regra do pipeline (fonte de verdade = raw):
  - Tem no DATASUS e não tem em raw → baixar (este módulo).
  - Tem em raw e não tem em processed → transformar (transform.py).

Fluxo:
  1. Lista alvos por diff: grade desejada (UF × ano × mês × sistema) menos o que já existe em data/raw/.
  2. Opcionalmente inclui entradas de logs/erros.log para retentar falhas (ex.: fora da grade padrão).
  3. Para cada alvo: baixa DBC do FTP, descomprime, filtra em chunks e grava Parquet.
  4. Progresso em tela; erros em logs/erros.log (registro apenas; a lista de “o que processar” vem do diff).
"""

from __future__ import annotations

import errno
import ftplib
import random
import re
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Literal

import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq

from .log_util import log

# --- Raiz e diretórios ---
def _root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


RAW_BASE = _root() / "data" / "raw"
LOG_FILE = _root() / "logs" / "erros.log"

QUEM = "Python"
ONDE = "ingestion"

# Servidor DATASUS: uso de FTP nativo (ftplib) evita timeout do HTTP no gateway.
# Fallback: espelho S3 (acesso mundial; FTP oficial pode restringir fora do Brasil).
FTP_HOST = "ftp.datasus.gov.br"
FTP_REMOTE_DIR = "/dissemin/publicos"
# SIH-RD: SIHSUS/200801_/Dados/RD{UF}{YY}{MM}.dbc
# SIA-PA: SIASUS/200801_/Dados/PA{UF}{YY}{MM}.dbc
S3_MIRROR_BASE = "https://datasus-ftp-mirror.nyc3.cdn.digitaloceanspaces.com"
# URL HTTP legada (gateway do FTP; costuma dar timeout)
FTP_BASE_HTTP = "https://ftp.datasus.gov.br/dissemin/publicos"

# Padrões no log
LOG_ERRO_PATTERN = re.compile(
    r"(?:ERRO PROCESSAMENTO|FALHA DEFINITIVA DOWNLOAD):\s*(SIH-RD|SIA-PA)\s+([A-Z]{2})\s+(\d{4})\s+(\d{1,2})",
    re.IGNORECASE,
)

# Regex CID (alinhado ao R)
CID_REGEX = re.compile(
    r"^(E1[0-4]|I70|I73|I74|L97|M86|S78|S88|S98|T13\.6|T87|Z89|S72)",
    re.IGNORECASE,
)

STATES = (
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
)
SYSTEMS = ("SIH-RD", "SIA-PA")
YEARS_DEFAULT = (2021, 2025)
MONTHS = range(1, 13)
CHUNK_SIZE = 80_000
DOWNLOAD_TIMEOUT = 600
MAX_ATTEMPTS = 3
CONNECT_TIMEOUT = 30  # timeout só para o teste de conexão inicial
# Script R: timeout por atividade (renova enquanto houver saída ou arquivo crescendo)
R_NO_PROGRESS_TIMEOUT = 600   # 10 min sem nenhuma atividade → interrompe
R_HARD_TIMEOUT = 7200         # 2 h tempo total máximo
R_POLL_INTERVAL = 15           # verificar a cada 15 s
# Contagem de falhas no log: se o mesmo arquivo falhar >= este valor, não tenta de novo nesta execução
MAX_FAILURES_BEFORE_SKIP = 3
# Circuit breaker (literatura): após N falhas por timeout, pausa X s antes de continuar
CIRCUIT_BREAKER_THRESHOLD = 3
CIRCUIT_BREAKER_COOLDOWN = 300  # 5 min


def _clean_name(s: str) -> str:
    """Simula janitor::clean_names: minúsculo, espaços → underscore."""
    s = str(s).strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    return s or "unknown"


def _coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coalesce de colunas com mesmo nome após normalização.
    Ex.: VAL_TOT/val_tot -> val_tot.
    Mantém a primeira não-nula por linha.
    """
    if df.empty:
        return df
    cols = list(df.columns)
    if len(cols) == len(set(cols)):
        return df
    out: dict[str, pd.Series] = {}
    for col in dict.fromkeys(cols):  # preserva ordem de aparição
        idx = [i for i, c in enumerate(cols) if c == col]
        if len(idx) == 1:
            out[col] = df.iloc[:, idx[0]]
        else:
            block = df.iloc[:, idx]
            out[col] = block.bfill(axis=1).iloc[:, 0]
    return pd.DataFrame(out)


def _classify_cid(cid: str) -> str:
    if not cid or (isinstance(cid, float) and pd.isna(cid)):
        return "Sem CID"
    cid = str(cid).strip().upper()
    if re.match(r"^E1[0-4]", cid):
        return "Diabetes"
    if re.match(r"^(I70|I73|I74|L97)", cid):
        return "Vascular"
    if re.match(r"^(S78|S88|S98|T13\.6|S72)", cid):
        return "Trauma"
    if re.match(r"^(Z89|T87|M86)", cid):
        return "Pos-Amputacao"
    return "Outro"


def _dest_path(sistema: Literal["SIH", "SIA"], uf: str, year: int, month: int) -> Path:
    prefix = "sih" if sistema == "SIH" else "sia"
    name = f"{prefix}_{uf}_{year}_{month:02d}.parquet"
    dir_path = RAW_BASE / f"ano={year}" / f"uf={uf}" / f"sistema={sistema}"
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path / name


def _download_cache_path(sistema: Literal["SIH", "SIA"], uf: str, year: int, month: int) -> Path:
    """Arquivo de cache do fallback R (somente etapa de download)."""
    dest = _dest_path(sistema, uf, year, month)
    return dest.parent / f".download_{dest.name}"


def _system_to_label(system: str) -> Literal["SIH", "SIA"]:
    return "SIH" if system == "SIH-RD" else "SIA"


def _ftp_remote_path(system: str, uf: str, year: int, month: int) -> str:
    """Caminho do arquivo no FTP (e no espelho S3). Ex.: /dissemin/publicos/SIHSUS/200801_/Dados/RDAC2512.dbc"""
    yy = year % 100
    mm = f"{month:02d}"
    uf = uf.upper()
    if system == "SIH-RD":
        name = f"RD{uf}{yy}{mm}.dbc"
        return f"{FTP_REMOTE_DIR}/SIHSUS/200801_/Dados/{name}"
    else:
        name = f"PA{uf}{yy}{mm}.dbc"
        return f"{FTP_REMOTE_DIR}/SIASUS/200801_/Dados/{name}"


def _s3_mirror_url(system: str, uf: str, year: int, month: int) -> str:
    """URL no espelho S3 (mesma estrutura do FTP)."""
    rel = _ftp_remote_path(system, uf, year, month).strip("/")
    return f"{S3_MIRROR_BASE}/{rel}"


def _test_connection_datasus() -> tuple[bool, str]:
    """
    Testa conectividade: primeiro FTP nativo (ftplib); se falhar, tenta espelho S3 (HTTP).
    Retorna (True, msg_ok) ou (False, msg_erro). Não levanta exceção.
    """
    try:
        ftp = ftplib.FTP(timeout=CONNECT_TIMEOUT)
        ftp.connect(FTP_HOST, port=21)
        ftp.login()
        ftp.cwd(FTP_REMOTE_DIR)
        ftp.quit()
        return True, "Conectado ao FTP DATASUS (ftp.datasus.gov.br)."
    except Exception as e:
        err_ftp = _classify_error(e)
        try:
            req = urllib.request.Request(
                f"{S3_MIRROR_BASE}/dissemin/publicos",
                headers={"User-Agent": "datas-rag-sus/1.0"},
            )
            with urllib.request.urlopen(req, timeout=CONNECT_TIMEOUT) as r:
                r.read(1)
            return True, "FTP DATASUS indisponível; usando espelho S3 (conexão OK)."
        except Exception as e2:
            return False, f"FTP: {err_ftp} | Espelho S3: {_classify_error(e2)}"


def _classify_error(e: BaseException) -> str:
    """
    Traduz exceção em mensagem clara para o log:
    - Arquivo inexistente (404/550)
    - Timeout (rede/servidor lento)
    - Memória
    - Outros (com detalhe quando útil).
    """
    if isinstance(e, urllib.error.HTTPError):
        if e.code in (404, 410) or "550" in str(e):
            return "Arquivo inexistente no servidor (404/550)."
        return f"Erro HTTP {e.code}: {e.reason or e}"
    if isinstance(e, urllib.error.URLError):
        reason = getattr(e, "reason", None)
        if reason is not None and (
            isinstance(reason, (TimeoutError, socket.timeout))
            or (isinstance(reason, OSError) and getattr(reason, "errno", None) == errno.ETIMEDOUT)
            or "timed out" in str(reason).lower()
            or "errno 60" in str(reason).lower()
        ):
            return "Tempo excedido (timeout): servidor ou rede lenta; tente novamente mais tarde."
        return f"Erro de rede/URL: {reason or e}"
    if isinstance(e, (TimeoutError, socket.timeout)):
        return "Tempo excedido (timeout): servidor ou rede lenta; tente novamente mais tarde."
    if isinstance(e, OSError) and getattr(e, "errno", None) == errno.ETIMEDOUT:
        return "Tempo excedido (timeout): servidor ou rede lenta; tente novamente mais tarde."
    if isinstance(e, MemoryError):
        return "Erro de memória: arquivo ou conjunto de dados muito grande."
    s = str(e).strip()
    if "timed out" in s.lower() or "errno 60" in s.lower() or "timeout" in s.lower():
        return "Tempo excedido (timeout): servidor ou rede lenta; tente novamente mais tarde."
    return s or type(e).__name__


def _download_dbc_ftp(system: str, uf: str, year: int, month: int, path: Path) -> tuple[bool, str]:
    """Baixa DBC via FTP (ftplib). Lista o diretório antes (inventário) para evitar 550. Retorna (sucesso, mensagem_erro)."""
    remote = _ftp_remote_path(system, uf, year, month)
    remote_dir = remote.rsplit("/", 1)[0]  # ex.: /dissemin/publicos/SIHSUS/200801_/Dados
    filename = remote.rsplit("/", 1)[-1]   # ex.: RDAC2512.dbc
    try:
        ftp = ftplib.FTP(timeout=DOWNLOAD_TIMEOUT)
        ftp.connect(FTP_HOST, port=21)
        ftp.login()
        ftp.cwd(remote_dir)
        # Pré-checagem: LIST/NLST do diretório (literatura: evita 550, confirma existência)
        try:
            listing = [f.upper() for f in ftp.nlst()]
            if filename.upper() not in listing:
                ftp.quit()
                return False, "Arquivo inexistente no servidor (não está na listagem do diretório)."
        except (OSError, ftplib.error_perm):
            pass  # NLST pode falhar em alguns servidores; segue tentativa de RETR
        with path.open("wb") as f:
            ftp.retrbinary(f"RETR {filename}", f.write)
        ftp.quit()
        # Validação mínima: arquivo não vazio (evita processar download truncado)
        if path.stat().st_size == 0:
            return False, "Download retornou arquivo vazio."
        return True, ""
    except Exception as e:
        return False, _classify_error(e)


def _download_dbc_http(url: str, path: Path) -> tuple[bool, str]:
    """Baixa DBC via HTTP (espelho S3 ou gateway). Retorna (sucesso, mensagem_erro)."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "datas-rag-sus/1.0"})
        with urllib.request.urlopen(req, timeout=DOWNLOAD_TIMEOUT) as r:
            path.write_bytes(r.read())
        if path.stat().st_size == 0:
            return False, "Download retornou arquivo vazio."
        return True, ""
    except urllib.error.HTTPError as e:
        if e.code in (404, 410) or "550" in str(e):
            return False, "Arquivo inexistente no servidor (404/550)."
        return False, _classify_error(e)
    except Exception as e:
        return False, _classify_error(e)


def _download_dbc(system: str, uf: str, year: int, month: int, path: Path) -> tuple[bool, str]:
    """
    Baixa DBC: tenta FTP nativo (ftplib) com retry e backoff+jitter; se falhar, tenta espelho S3 (HTTP).
    Retorna (sucesso, mensagem_erro).
    """
    err = ""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        if attempt > 1:
            # Backoff exponencial + jitter (literatura: evita thundering herd, falhas em cascata)
            delay = min(2 ** attempt + random.uniform(0, 1), 60.0)
            time.sleep(delay)
        ok, err = _download_dbc_ftp(system, uf, year, month, path)
        if ok:
            return True, ""
        if "inexistente" in err.lower() or "550" in err or "404" in err:
            break  # Não retentar: arquivo não existe
        if attempt == MAX_ATTEMPTS:
            break
    err_ftp = err
    s3_url = _s3_mirror_url(system, uf, year, month)
    ok, err = _download_dbc_http(s3_url, path)
    if ok:
        return True, ""
    return False, f"FTP: {err_ftp} | S3: {err}"


def _records_to_clean_df(records: list[dict]) -> pd.DataFrame:
    """Lista de dict (DBF record) → DataFrame com colunas em snake_case."""
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    df.columns = [_clean_name(c) for c in df.columns]
    return _coalesce_duplicate_columns(df)


def _filter_sih(df: pd.DataFrame) -> pd.DataFrame:
    """Filtro SIH: diag_princ no CID regex ou proc_rea começa com 0415."""
    if df.empty:
        return df
    diag = df.get("diag_princ")
    proc = df.get("proc_rea")
    if diag is None:
        return pd.DataFrame()
    diag = diag.astype(str)
    mask_cid = diag.str.upper().str.strip().str.match(CID_REGEX.pattern, na=False)
    mask_proc = False
    if proc is not None:
        mask_proc = proc.astype(str).str.strip().str.startswith("0415")
    return df.loc[mask_cid | mask_proc].copy()


def _filter_sia(df: pd.DataFrame) -> pd.DataFrame:
    """Filtro SIA: (pa_grupo 03, pa_subgru 02, CID) ou (pa_grupo 07, pa_subgru 01 ou 02)."""
    if df.empty:
        return df
    proc = df.get("pa_proc_id")
    if proc is None:
        return pd.DataFrame()
    proc = proc.astype(str).str.zfill(10)
    pa_grupo = proc.str[:2]
    pa_subgru = proc.str[2:4]
    cid = df.get("pa_cidpri")
    if cid is not None:
        cid = cid.astype(str).str.upper().str.strip()
    else:
        cid = pd.Series([""] * len(df))
    mask_cid = cid.str.match(CID_REGEX.pattern, na=False)
    mask1 = (pa_grupo == "03") & (pa_subgru == "02") & mask_cid
    mask2 = (pa_grupo == "07") & (pa_subgru.isin(["01", "02"]))
    return df.loc[mask1 | mask2].copy()


def _add_meta_sih(df: pd.DataFrame, uf: str, year: int, month: int) -> pd.DataFrame:
    df = df.copy()
    df["uf_origem"] = uf
    df["ano_cmpt"] = year
    df["mes_cmpt"] = month
    df["sistema"] = "SIH"
    df["main_icd"] = df.get("diag_princ", pd.Series(dtype=object))
    df["icd_group"] = df["main_icd"].astype(str).map(_classify_cid)
    df["opm_flag"] = False
    df["fisio_flag"] = False
    return df


def _add_meta_sia(df: pd.DataFrame, uf: str, year: int, month: int) -> pd.DataFrame:
    df = df.copy()
    proc = df.get("pa_proc_id", pd.Series(dtype=object)).astype(str).str.zfill(10)
    df["pa_grupo"] = proc.str[:2]
    df["pa_subgru"] = proc.str[2:4]
    df["uf_origem"] = uf
    df["ano_cmpt"] = year
    df["mes_cmpt"] = month
    df["sistema"] = "SIA"
    df["main_icd"] = df.get("pa_cidpri", pd.Series(dtype=object))
    df["icd_group"] = df["main_icd"].astype(str).map(_classify_cid)
    df["opm_flag"] = df["pa_grupo"] == "07"
    df["fisio_flag"] = (df["pa_grupo"] == "03") & (df["pa_subgru"] == "02")
    return df


def _process_dbc_python(
    system: str, uf: str, year: int, month: int
) -> tuple[bool, str]:
    """Baixa DBC, descomprime, processa em chunks e grava Parquet. 100% Python."""
    import datasus_dbc  # optional
    from dbfread import DBF

    dest = _dest_path(_system_to_label(system), uf, year, month)
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        dbc_path = tmp / "file.dbc"
        dbf_path = tmp / "file.dbf"
        ok, err = _download_dbc(system, uf, year, month, dbc_path)
        if not ok:
            return False, err
        try:
            datasus_dbc.decompress(str(dbc_path), str(dbf_path))
        except Exception as e:
            return False, f"Descompressão DBC: {e}"
        if not dbf_path.is_file():
            return False, "Arquivo DBF não gerado"
        filter_fn = _filter_sih if system == "SIH-RD" else _filter_sia
        meta_fn = _add_meta_sih if system == "SIH-RD" else _add_meta_sia
        writer = None
        schema = None
        chunk = []
        n_written = 0
        try:
            for record in DBF(str(dbf_path), encoding="latin-1"):
                chunk.append(dict(record))
                if len(chunk) >= CHUNK_SIZE:
                    df = _records_to_clean_df(chunk)
                    chunk = []
                    df = filter_fn(df)
                    if df.empty:
                        continue
                    df = meta_fn(df, uf, year, month)
                    table = pa.Table.from_pandas(df, preserve_index=False)
                    if schema is None:
                        schema = table.schema
                        writer = pq.ParquetWriter(str(dest), schema)
                    writer.write_table(table)
                    n_written += len(df)
            if chunk:
                df = _records_to_clean_df(chunk)
                df = filter_fn(df)
                if not df.empty:
                    df = meta_fn(df, uf, year, month)
                    table = pa.Table.from_pandas(df, preserve_index=False)
                    if schema is None:
                        schema = table.schema
                        writer = pq.ParquetWriter(str(dest), schema)
                    writer.write_table(table)
                    n_written += len(df)
        finally:
            if writer is not None:
                writer.close()
        if schema is None:
            return False, "Nenhum registro passou no filtro"
        if not dest.exists():
            return False, "Parquet não foi gravado"
    return True, ""


def _targets_from_log() -> set[tuple[str, str, int, int]]:
    targets = set()
    if not LOG_FILE.is_file():
        return targets
    text = LOG_FILE.read_text(encoding="utf-8", errors="replace")
    for m in LOG_ERRO_PATTERN.finditer(text):
        system, uf, year, month = m.group(1), m.group(2).upper(), int(m.group(3)), int(m.group(4))
        targets.add((system, uf, year, month))
    return targets


def _count_failures_in_log(system: str, uf: str, year: int, month: int) -> int:
    """
    Conta quantas vezes este arquivo (system, uf, year, month) aparece no log
    como falha (ERRO ingestão, ERRO PROCESSAMENTO ou FALHA DEFINITIVA DOWNLOAD).
    """
    if not LOG_FILE.is_file():
        return 0
    text = LOG_FILE.read_text(encoding="utf-8", errors="replace")
    label = f"{system} {uf} {year} {month:02d}"
    label_alt = f"{system} {uf} {year} {month}" if month < 10 else None
    count = 0
    for line in text.splitlines():
        if "ERRO ingestão" in line and (label in line or (label_alt and label_alt in line)):
            count += 1
        else:
            m = LOG_ERRO_PATTERN.search(line)
            if m and m.group(1) == system and m.group(2).upper() == uf.upper() and int(m.group(3)) == year and int(m.group(4)) == month:
                count += 1
    return count


def _targets_missing(
    years: tuple[int, int] = YEARS_DEFAULT,
    skip_future: bool = True,
) -> set[tuple[str, str, int, int]]:
    from datetime import date
    targets = set()
    today = date.today()
    y_min, y_max = years
    for system in SYSTEMS:
        for uf in STATES:
            for year in range(y_min, y_max + 1):
                for month in MONTHS:
                    if skip_future and (year, month) > (today.year, today.month):
                        continue
                    if not _dest_path(_system_to_label(system), uf, year, month).exists():
                        targets.add((system, uf, year, month))
    return targets


def get_targets(
    from_log: bool = False,
    include_missing: bool = True,
    years: tuple[int, int] = YEARS_DEFAULT,
) -> list[tuple[str, str, int, int]]:
    """Alvos = diff (grade desejada menos data/raw/). Opcional: unir com entradas do log de erros."""
    targets = set()
    if include_missing:
        targets |= _targets_missing(years=years)
    if from_log:
        targets |= _targets_from_log()
    out = []
    for system, uf, year, month in targets:
        if not _dest_path(_system_to_label(system), uf, year, month).exists():
            out.append((system, uf, year, month))
    out.sort(key=lambda x: (x[1], x[2], x[3], x[0]))
    return out


def _is_file_not_found_error(err: str) -> bool:
    """True se a mensagem indica que o arquivo não existe no servidor (404/550)."""
    if not err:
        return False
    e = err.upper()
    return (
        "404" in e or "550" in e
        or "ARQUIVO INEXISTENTE" in e
        or "FILE NOT FOUND" in e
        or "CANNOT FIND THE FILE" in e
    )


# Padrões do stderr do R que são só informativos (não gravar no log para manter rastreabilidade)
_R_STDERR_SKIP = (
    "your local internet", "datasus ftp server", "seems to be ok", "seems to be up",
    "[etapa]", "[chunk]", "baixando", "modo único arquivo", "tentativa ",
    "ℹ", "connection seems", "server seems",
)


def _r_stderr_to_log_message(returncode: int, stderr_chunks: list[str]) -> str:
    """Reduz o stderr do R a uma mensagem curta para o log, sem linhas puramente informativas."""
    base = f"R concluiu com status {returncode}"
    if not stderr_chunks:
        return base
    lines = [ln.strip() for ln in stderr_chunks if ln.strip()]
    for ln in lines:
        low = ln.lower()
        if any(skip in low for skip in _R_STDERR_SKIP):
            continue
        if len(ln) > 120:
            ln = ln[:117] + "..."
        return f"{base}; {ln}"
    return base


def _run_r_fallback(system: str, uf: str, year: int, month: int) -> tuple[bool, str]:
    """
    Compatibilidade: fallback R agora é somente download.
    Mantido como wrapper para evitar quebra de chamadas legadas.
    """
    return _run_r_download_only(system, uf, year, month)


def _run_single(system: str, uf: str, year: int, month: int) -> tuple[bool, str]:
    """Processa um arquivo em Python; se ausente no FTP/S3, usa R só para download e finaliza no Python."""
    try:
        success, err = _process_dbc_python(system, uf, year, month)
        if success:
            return True, ""
        if _is_file_not_found_error(err):
            print("       Arquivo não no FTP/S3; tentando fallback R (somente download).", flush=True)
            log(QUEM, ONDE, f"Arquivo não encontrado (FTP/S3); fallback R download-only: {system} {uf} {year} {month:02d}")
            ok, err_r = _run_r_download_only(system, uf, year, month)
            if not ok:
                return False, err_r
            return _process_r_download_cache_python(system, uf, year, month)
        return False, err
    except ImportError as e:
        return False, f"Dependências: pip install datasus-dbc dbfread — {e}"
    except Exception as e:
        return False, _classify_error(e)


def _run_r_download_only(system: str, uf: str, year: int, month: int) -> tuple[bool, str]:
    """
    Executa R em modo download-only: gera .download_<arquivo>.parquet.
    Não processa filtros/transform no R.
    """
    root = _root()
    script = root / "scripts" / "r" / "fallback_download_only.R"
    if not script.is_file():
        return False, "Script R não encontrado (scripts/r/fallback_download_only.R)."
    dest = _dest_path(_system_to_label(system), uf, year, month)
    cache = _download_cache_path(_system_to_label(system), uf, year, month)
    cmd = ["Rscript", str(script), uf.upper(), str(year), str(month), system]
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(root),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        last_activity = [time.time()]
        stderr_chunks: list[str] = []

        def read_stream(stream, is_stderr: bool):
            try:
                for line in iter(stream.readline, ""):
                    last_activity[0] = time.time()
                    stripped = line.strip()
                    if is_stderr:
                        stderr_chunks.append(line)
                    if stripped and ("[ETAPA]" in stripped or "Concluído" in stripped or "BAIXANDO" in stripped):
                        print(f"\n       {stripped}", flush=True)
                    else:
                        print(".", end="", flush=True)
            except (ValueError, OSError):
                pass
            finally:
                try:
                    stream.close()
                except OSError:
                    pass

        t_stdout = threading.Thread(target=read_stream, args=(proc.stdout, False), daemon=True)
        t_stderr = threading.Thread(target=read_stream, args=(proc.stderr, True), daemon=True)
        t_stdout.start()
        t_stderr.start()

        start = time.time()
        last_printed_min = 0
        while proc.poll() is None:
            time.sleep(R_POLL_INTERVAL)
            now = time.time()
            elapsed_min = int((now - start) / 60)
            if elapsed_min > 0 and elapsed_min > last_printed_min:
                last_printed_min = elapsed_min
                print(f"\n       (tempo decorrido: {elapsed_min} min)", flush=True)

            if cache.exists():
                try:
                    st = cache.stat()
                    last_activity[0] = now
                    if st.st_size > 0:
                        print(".", end="", flush=True)
                except OSError:
                    pass

            if now - last_activity[0] > R_NO_PROGRESS_TIMEOUT:
                proc.kill()
                proc.wait()
                return False, (
                    f"R download-only interrompido: sem atividade por {R_NO_PROGRESS_TIMEOUT // 60} min "
                    "(nenhuma saída nem progresso de download)."
                )
            if now - start > R_HARD_TIMEOUT:
                proc.kill()
                proc.wait()
                return False, f"R download-only excedeu tempo máximo de {R_HARD_TIMEOUT // 3600} h."

        t_stdout.join(timeout=1.0)
        t_stderr.join(timeout=1.0)
        print(flush=True)

        if proc.returncode != 0:
            msg = _r_stderr_to_log_message(proc.returncode, stderr_chunks)
            return False, msg
        if not cache.exists():
            return False, f"R download-only concluiu sem gerar cache: {cache.name}"
        return True, ""
    except FileNotFoundError:
        return False, "Rscript não encontrado (instale R e coloque no PATH)."
    except Exception as e:
        return False, str(e)


def _process_r_download_cache_python(system: str, uf: str, year: int, month: int) -> tuple[bool, str]:
    """
    Processa no Python um cache parquet gerado pelo R download-only.
    Mantém os mesmos filtros e metadados da ingestão Python nativa.
    """
    sistema = _system_to_label(system)
    dest = _dest_path(sistema, uf, year, month)
    cache = _download_cache_path(sistema, uf, year, month)
    if not cache.exists():
        return False, f"Cache de download não encontrado: {cache}"

    filter_fn = _filter_sih if system == "SIH-RD" else _filter_sia
    meta_fn = _add_meta_sih if system == "SIH-RD" else _add_meta_sia

    writer = None
    schema = None
    n_written = 0
    try:
        pf = pq.ParquetFile(str(cache))
        for batch in pf.iter_batches(batch_size=CHUNK_SIZE):
            df = batch.to_pandas()
            df.columns = [_clean_name(c) for c in df.columns]
            df = _coalesce_duplicate_columns(df)
            df = filter_fn(df)
            if df.empty:
                continue
            df = meta_fn(df, uf, year, month)
            table = pa.Table.from_pandas(df, preserve_index=False)
            if schema is None:
                schema = table.schema
                writer = pq.ParquetWriter(str(dest), schema)
            writer.write_table(table)
            n_written += len(df)
    except Exception as e:
        return False, f"Processamento Python do cache R: {e}"
    finally:
        if writer is not None:
            writer.close()
        try:
            if cache.exists():
                cache.unlink()
        except Exception:
            pass

    if schema is None or n_written == 0:
        return False, "Nenhum registro passou no filtro (cache R)."
    if not dest.exists():
        return False, "Parquet final não foi gravado após processamento do cache R."
    return True, ""


def run_ingestion(
    from_log: bool = False,
    include_missing: bool = True,
    years: tuple[int, int] = YEARS_DEFAULT,
) -> None:
    """
    Busca alvos por diff (ausentes em data/raw/); opcionalmente une com entradas do log.
    Baixa e processa em Python (FTP → DBC → DBF → chunks → Parquet). Progresso em tela; erros no log.
    """
    targets = get_targets(from_log=from_log, include_missing=include_missing, years=years)
    if not targets:
        print("Nenhum alvo para processar.", flush=True)
        log(QUEM, ONDE, "Execução sem alvos.")
        return
    total = len(targets)
    print(f"Iniciando ingestão (100% Python): {total} arquivo(s).", flush=True)

    # Teste de conexão com o servidor DATASUS; log e tela
    conn_ok, conn_msg = _test_connection_datasus()
    if conn_ok:
        log(QUEM, ONDE, f"CONEXÃO DATASUS: OK — {conn_msg}")
        print(f"  Conexão DATASUS: OK.", flush=True)
    else:
        log(QUEM, ONDE, f"CONEXÃO DATASUS: FALHA — {conn_msg}")
        print(f"  Conexão DATASUS: FALHA — {conn_msg}", flush=True)
        print("  Timeouts nos downloads podem ser causados por falha de conexão/rede.", flush=True)

    ok = 0
    fail = 0
    skipped = 0
    consecutive_timeouts = 0
    for i, (system, uf, year, month) in enumerate(targets, start=1):
        if consecutive_timeouts >= CIRCUIT_BREAKER_THRESHOLD:
            print(f"\n  Circuit breaker: {consecutive_timeouts} falhas por timeout; aguardando {CIRCUIT_BREAKER_COOLDOWN // 60} min...", flush=True)
            log(QUEM, ONDE, f"Circuit breaker: aguardando {CIRCUIT_BREAKER_COOLDOWN}s após {consecutive_timeouts} timeouts.")
            time.sleep(CIRCUIT_BREAKER_COOLDOWN)
            consecutive_timeouts = 0
        label = f"{system} {uf} {year} {month:02d}"
        n_failures = _count_failures_in_log(system, uf, year, month)
        if n_failures >= MAX_FAILURES_BEFORE_SKIP:
            skipped += 1
            log(QUEM, ONDE, f"IGNORADO (≥{MAX_FAILURES_BEFORE_SKIP} falhas no log): {label}")
            print(f"  [{i}/{total}] ⏭️ {label} — ignorado ({n_failures} falhas no log).", flush=True)
            continue
        print(f"  [{i}/{total}] ⬇️ {label}...", flush=True)
        success, err = _run_single(system, uf, year, month)
        dest = _dest_path(_system_to_label(system), uf, year, month)
        if success and dest.exists():
            ok += 1
            consecutive_timeouts = 0
            log(QUEM, ONDE, f"SUCESSO ingestão: {label}")
            print(f"       ✅ {label}", flush=True)
        else:
            if success and not dest.exists():
                err = "Arquivo não gravado"
            fail += 1
            if "timeout" in (err or "").lower() or "tempo excedido" in (err or "").lower():
                consecutive_timeouts += 1
            else:
                consecutive_timeouts = 0
            log(QUEM, ONDE, f"ERRO ingestão {label}: {err[:200]}")
            print(f"       ❌ {label} — ver logs/erros.log", flush=True)
    print(f"Concluído: {ok} processados, {fail} falhas, {skipped} ignorados (≥{MAX_FAILURES_BEFORE_SKIP} falhas no log).", flush=True)
    log(QUEM, ONDE, f"Concluído: {ok} processados, {fail} falhas, {skipped} ignorados.")


if __name__ == "__main__":
    run_ingestion()
