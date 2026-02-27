# Scripts R — Fallback de Download

Diretório reservado ao fallback R do pipeline Python.

Atualmente existe apenas:

- `fallback_download_only.R`: baixa o alvo DATASUS e grava cache bruto em
  `.download_<arquivo>.parquet` dentro de `data/raw/ano=X/uf=Y/sistema=SIH|SIA/`.

Uso (modo único):

```bash
Rscript scripts/r/fallback_download_only.R UF ANO MES SISTEMA
# Exemplo:
Rscript scripts/r/fallback_download_only.R SP 2024 08 SIA-PA
```

Observações:

- Esse script não aplica filtros nem transformações de negócio.
- O processamento (filtros/chunks/parquet final) continua no `src/data/ingestion.py`.
