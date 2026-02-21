# Scripts R — Pipeline de dados

Scripts em **R** que fazem parte do pipeline de dados do SUS Data RAG.

Coloque aqui, por exemplo:

- Download de dados do DATASUS (ex.: via `microdatasus`, `datasus` ou APIs)
- Tratamento e limpeza em R antes de exportar para CSV/Parquet
- Geração de dicionários ou metadados usados pelo pipeline Python

**Onde os dados são salvos (estrutura do projeto):**

| Saída | Pasta | Exemplo |
|-------|--------|---------|
| Parquet processados (SIH/SIA) | `data/processed/` | `sih_SP_2024_01.parquet`, `sia_PA_2023_06.parquet` |
| Log de erros | `logs/` | `logs/erros.log` |

O script `analise_ortopedia.R` detecta a raiz do projeto automaticamente (rodando da raiz ou de `scripts/r/`).

**Dependências R:** o script `analise_ortopedia.R` instala automaticamente, na primeira execução, os pacotes que faltam: `microdatasus`, `tidyverse`, `arrow`, `fs`, `janitor`. Opcional: use `renv` para ambiente reproduzível.
