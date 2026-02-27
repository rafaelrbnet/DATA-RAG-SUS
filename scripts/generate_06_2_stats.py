#!/usr/bin/env python3
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import re

import duckdb
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "data" / "processed"
DOC_OUT = ROOT / "docs" / "06.2-estatisticas-base-processada.md"


PART_RE = re.compile(r"ano=(\d{4})/uf=([A-Z]{2})/sistema=(SIA|SIH)/", re.IGNORECASE)


def fmt_int(v: int) -> str:
    return f"{v:,}".replace(",", ".")


def fmt_float(v: float | None, nd: int = 2) -> str:
    if v is None:
        return "-"
    return f"{v:,.{nd}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def collect_file_stats() -> dict:
    files = sorted(PROCESSED.rglob("*.parquet"))
    total_size = 0
    total_rows = 0
    by_system = defaultdict(lambda: {"files": 0, "rows": 0, "bytes": 0})
    by_year = defaultdict(lambda: {"files": 0, "rows": 0, "bytes": 0})
    by_uf = defaultdict(lambda: {"files": 0, "rows": 0, "bytes": 0})

    for f in files:
        st = f.stat()
        total_size += st.st_size
        m = PART_RE.search(f.as_posix())
        if not m:
            continue
        ano, uf, sistema = m.group(1), m.group(2), m.group(3).upper()
        try:
            rows = pq.ParquetFile(f).metadata.num_rows
        except Exception:
            rows = 0
        total_rows += rows
        by_system[sistema]["files"] += 1
        by_system[sistema]["rows"] += rows
        by_system[sistema]["bytes"] += st.st_size
        by_year[ano]["files"] += 1
        by_year[ano]["rows"] += rows
        by_year[ano]["bytes"] += st.st_size
        by_uf[uf]["files"] += 1
        by_uf[uf]["rows"] += rows
        by_uf[uf]["bytes"] += st.st_size

    return {
        "files": files,
        "total_files": len(files),
        "total_rows": total_rows,
        "total_size": total_size,
        "by_system": by_system,
        "by_year": by_year,
        "by_uf": by_uf,
    }


def run_sql():
    con = duckdb.connect(database=":memory:")
    glob = str(PROCESSED / "**" / "*.parquet").replace("'", "''")
    con.execute(
        f"""
        CREATE VIEW ds AS
        SELECT * FROM read_parquet('{glob}', hive_partitioning=true, union_by_name=true);
        """
    )

    nulls = con.execute(
        """
        SELECT
          COUNT(*) AS n_rows,
          SUM(CASE WHEN main_icd IS NULL OR trim(CAST(main_icd AS VARCHAR)) = '' THEN 1 ELSE 0 END) AS n_main_icd_null,
          SUM(CASE WHEN custo_total IS NULL THEN 1 ELSE 0 END) AS n_custo_total_null,
          SUM(CASE WHEN idade_grupo IS NULL OR trim(CAST(idade_grupo AS VARCHAR)) = '' THEN 1 ELSE 0 END) AS n_idade_grupo_null,
          SUM(CASE WHEN ano_mes IS NULL THEN 1 ELSE 0 END) AS n_ano_mes_null
        FROM ds
        """
    ).fetchone()

    custo = con.execute(
        """
        SELECT
          MIN(custo_total) AS min_v,
          QUANTILE_CONT(custo_total, 0.5) AS p50_v,
          QUANTILE_CONT(custo_total, 0.95) AS p95_v,
          AVG(custo_total) AS avg_v,
          MAX(custo_total) AS max_v
        FROM ds
        WHERE custo_total IS NOT NULL
        """
    ).fetchone()

    top_icd = con.execute(
        """
        SELECT CAST(icd_group AS VARCHAR) AS icd_group, COUNT(*) AS n
        FROM ds
        WHERE icd_group IS NOT NULL AND trim(CAST(icd_group AS VARCHAR)) <> ''
        GROUP BY 1
        ORDER BY n DESC
        LIMIT 10
        """
    ).fetchall()

    top_proc = con.execute(
        """
        SELECT proc_id, COUNT(*) AS n
        FROM (
          SELECT COALESCE(NULLIF(trim(CAST(pa_proc_id AS VARCHAR)), ''), NULLIF(trim(CAST(proc_rea AS VARCHAR)), '')) AS proc_id
          FROM ds
        )
        WHERE proc_id IS NOT NULL
        GROUP BY 1
        ORDER BY n DESC
        LIMIT 10
        """
    ).fetchall()

    months_cov = con.execute(
        """
        SELECT
          sistema,
          CAST(TRY_CAST(ano_cmpt AS DOUBLE) AS BIGINT) AS ano_cmpt_norm,
          COUNT(DISTINCT CAST(TRY_CAST(mes_cmpt AS DOUBLE) AS BIGINT)) AS meses_distintos
        FROM ds
        WHERE TRY_CAST(ano_cmpt AS DOUBLE) IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
    ).fetchall()

    return {
        "nulls": nulls,
        "custo": custo,
        "top_icd": top_icd,
        "top_proc": top_proc,
        "months_cov": months_cov,
    }


def build_markdown(fs: dict, sql: dict) -> str:
    ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    n_rows, n_main_icd_null, n_custo_total_null, n_idade_grupo_null, n_ano_mes_null = sql["nulls"]

    lines: list[str] = []
    lines.append("# 06.2 - Estatísticas da Base Processada")
    lines.append("")
    lines.append(f"Atualizado em: `{ts}`")
    lines.append("")
    lines.append("## 1. Volume Geral")
    lines.append("")
    lines.append(f"- Arquivos parquet: `{fmt_int(fs['total_files'])}`")
    lines.append(f"- Linhas totais: `{fmt_int(fs['total_rows'])}`")
    lines.append(f"- Tamanho total em disco: `{fmt_float(fs['total_size'] / (1024**3), 2)} GB`")
    lines.append("")
    lines.append("## 2. Volume por Sistema")
    lines.append("")
    lines.append("| Sistema | Arquivos | Linhas | Tamanho (GB) |")
    lines.append("|---|---:|---:|---:|")
    for sistema in sorted(fs["by_system"].keys()):
        d = fs["by_system"][sistema]
        lines.append(
            f"| `{sistema}` | {fmt_int(d['files'])} | {fmt_int(d['rows'])} | {fmt_float(d['bytes']/(1024**3),2)} |"
        )
    lines.append("")
    lines.append("## 3. Cobertura Temporal")
    lines.append("")
    lines.append("| Sistema | Ano | Meses distintos |")
    lines.append("|---|---:|---:|")
    for sistema, ano, meses in sql["months_cov"]:
        lines.append(f"| `{sistema}` | {ano} | {meses} |")
    lines.append("")
    lines.append("## 4. Qualidade de Dados (Nulos em colunas-chave)")
    lines.append("")
    lines.append("| Coluna | Nulos | % Nulos |")
    lines.append("|---|---:|---:|")
    for name, val in [
        ("main_icd", n_main_icd_null),
        ("custo_total", n_custo_total_null),
        ("idade_grupo", n_idade_grupo_null),
        ("ano_mes", n_ano_mes_null),
    ]:
        pct = (val / n_rows * 100) if n_rows else 0.0
        lines.append(f"| `{name}` | {fmt_int(int(val))} | {fmt_float(pct,2)}% |")
    lines.append("")
    lines.append("## 5. Estatísticas Financeiras (`custo_total`)")
    lines.append("")
    min_v, p50_v, p95_v, avg_v, max_v = sql["custo"]
    lines.append("| Métrica | Valor |")
    lines.append("|---|---:|")
    lines.append(f"| Mínimo | {fmt_float(min_v,2)} |")
    lines.append(f"| P50 | {fmt_float(p50_v,2)} |")
    lines.append(f"| P95 | {fmt_float(p95_v,2)} |")
    lines.append(f"| Média | {fmt_float(avg_v,2)} |")
    lines.append(f"| Máximo | {fmt_float(max_v,2)} |")
    lines.append("")
    lines.append("## 6. Top `icd_group`")
    lines.append("")
    lines.append("| # | Grupo | Linhas |")
    lines.append("|---:|---|---:|")
    for i, (grp, n) in enumerate(sql["top_icd"], start=1):
        lines.append(f"| {i} | `{grp}` | {fmt_int(int(n))} |")
    lines.append("")
    lines.append("## 7. Top Procedimentos (`pa_proc_id`/`proc_rea`)")
    lines.append("")
    lines.append("| # | Procedimento | Linhas |")
    lines.append("|---:|---|---:|")
    for i, (proc, n) in enumerate(sql["top_proc"], start=1):
        lines.append(f"| {i} | `{proc}` | {fmt_int(int(n))} |")
    lines.append("")
    lines.append("## 8. Método")
    lines.append("")
    lines.append("- Contagem de linhas por arquivo via metadado Parquet (`pyarrow`).")
    lines.append("- Agregações analíticas e percentis via DuckDB sobre `data/processed/**/*.parquet`.")
    lines.append("- Documento voltado a observabilidade da base e monitoramento de qualidade.")
    lines.append("")
    return "\n".join(lines) + "\n"


def main() -> None:
    if not PROCESSED.exists():
        raise SystemExit(f"Diretório não encontrado: {PROCESSED}")
    fs = collect_file_stats()
    sql = run_sql()
    md = build_markdown(fs, sql)
    DOC_OUT.write_text(md, encoding="utf-8")
    print(f"Gerado: {DOC_OUT}")


if __name__ == "__main__":
    main()
