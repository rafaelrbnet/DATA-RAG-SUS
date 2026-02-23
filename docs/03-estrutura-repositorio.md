# 3. Estrutura do Repositório

```
datas-rag-sus/
│
├── README.md
├── pyproject.toml
├── .env.example
├── .gitignore
│
├── data/
│   ├── raw/          # Parquet bruto do DATASUS
│   ├── processed/    # Parquet tratado
│   └── schemas/      # Dicionários de dados
│
├── scripts/
│   └── r/            # Scripts R do pipeline (download, tratamento DATASUS)
│
├── docs/             # Documentação modular
│   ├── README.md     # Índice da documentação
│   ├── 01-objetivo.md
│   ├── 02-arquitetura.md
│   ├── 03-estrutura-repositorio.md
│   ├── 04-stack-tecnologica.md
│   ├── 05-fluxo-funcionamento.md
│   ├── 06-etapas-implementacao.md
│   ├── 07-prompt-agente.md
│   ├── 08-criterios-qualidade.md
│   ├── 09-roadmap.md
│   └── 10-licenca.md
│
├── src/
│   ├── rag/
│   │   ├── agent.py        # Orquestração LLM
│   │   ├── sql_generator.py # Geração de SQL
│   │   ├── executor.py     # Execução DuckDB
│   │   └── prompts.py      # Prompts do sistema
│   │
│   ├── data/
│   │   ├── transform.py    # Pipeline de transformação (raw → processed)
│   │   └── dictionary.py   # CID, SIGTAP etc.
│   │
│   └── api/
│       └── main.py         # API FastAPI
│
├── notebooks/
│   └── exploration.ipynb
│
└── tests/
    ├── test_sql_generation.py
    └── test_queries.py
```

[← Voltar ao índice](README.md)
