"""
Ingestão: Parquets em data/downloaded/ (gravados pelo R) → movidos para
data/raw/ particionado por ano, UF e sistema (SIH, SIA).

Não baixa dados do DATASUS; apenas reorganiza os arquivos já baixados pelo R.
Um arquivo por vez, com controle de memória (apenas operações de I/O).
"""

import re
import shutil
from pathlib import Path

from .log_util import log

# Diretórios (relativos à raiz do projeto)
def _root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


DOWNLOADED_DIR = _root() / "data" / "downloaded"
RAW_BASE = _root() / "data" / "raw"

# Padrão: sih_UF_YYYY_MM.parquet ou sia_UF_YYYY_MM.parquet
FILE_PATTERN = re.compile(r"^(sih|sia)_([A-Z]{2})_(\d{4})_(\d{2})\.parquet$", re.IGNORECASE)

# Mapeamento prefixo → sistema (partição)
PREFIX_TO_SISTEMA = {"sih": "SIH", "sia": "SIA"}


def _parse_filename(name: str) -> dict | None:
    """Retorna dict com ano, uf, sistema e mes se o nome seguir o padrão."""
    m = FILE_PATTERN.match(name)
    if not m:
        return None
    prefix, uf, ano, mes = m.group(1).lower(), m.group(2).upper(), m.group(3), m.group(4)
    return {
        "ano": ano,
        "uf": uf,
        "sistema": PREFIX_TO_SISTEMA[prefix],
        "mes": mes,
    }


def run_ingest() -> None:
    """
    Lista Parquets em data/downloaded/, cria data/raw/ano=X/uf=Y/sistema=Z/
    e move cada arquivo para o diretório correspondente.
    """
    quem = "Python"
    onde_base = "ingest"

    if not DOWNLOADED_DIR.is_dir():
        log(quem, onde_base, f"Diretório inexistente: {DOWNLOADED_DIR}")
        return

    raw_files = sorted(DOWNLOADED_DIR.glob("*.parquet"))
    if not raw_files:
        log(quem, onde_base, f"Nenhum .parquet em {DOWNLOADED_DIR}")
        return

    moved = 0
    skipped = 0
    for path in raw_files:
        name = path.name
        info = _parse_filename(name)
        if not info:
            log(quem, onde_base, f"Arquivo ignorado (nome fora do padrão): {name}")
            skipped += 1
            continue

        dest_dir = RAW_BASE / f"ano={info['ano']}" / f"uf={info['uf']}" / f"sistema={info['sistema']}"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / name

        try:
            if dest_path.exists():
                log(quem, str(dest_path), f"Já existe; mantendo original em {path}")
                skipped += 1
                continue
            shutil.move(str(path), str(dest_path))
            log(quem, str(dest_path), f"Movido de {path}")
            moved += 1
        except OSError as e:
            log(quem, str(path), f"Erro ao mover: {e}")

    log(quem, onde_base, f"Concluído: {moved} movidos, {skipped} ignorados/pulados.")


def list_bases_by_partition() -> list[dict]:
    """
    Lista bases em data/downloaded/ por ano, UF e sistema.
    Retorna lista de dicts com ano, uf, sistema, mes e path (após parse do nome).
    Útil para saber o que está pendente de processamento sem mover ainda.
    """
    if not DOWNLOADED_DIR.is_dir():
        return []

    out = []
    for path in DOWNLOADED_DIR.glob("*.parquet"):
        info = _parse_filename(path.name)
        if info:
            info["path"] = str(path)
            out.append(info)
    return out


if __name__ == "__main__":
    run_ingest()
