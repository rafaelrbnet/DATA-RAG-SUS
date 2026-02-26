# Formato de log: Quem | Quando | Onde | O que
# Rastreabilidade: script (R ou Python), data do sistema, componente/caminho, mensagem.

import os
from datetime import datetime
from pathlib import Path

# Diretório de logs (raiz do projeto)
def _project_root() -> Path:
    p = Path(__file__).resolve().parent.parent.parent
    return p


def _log_dir() -> Path:
    d = _project_root() / "logs"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _log_file() -> Path:
    return _log_dir() / "erros.log"


def log(quem: str, onde: str, mensagem: str) -> None:
    """Registra uma linha no logs/erros.log.
    Formato: quando (ISO) | quem | onde | mensagem
    """
    quando = datetime.now().isoformat()
    linha = f"{quando} | {quem} | {onde} | {mensagem}"
    with open(_log_file(), "a", encoding="utf-8") as f:
        f.write(linha + "\n")
