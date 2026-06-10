# Diagramas de Arquitetura — SUS Data RAG

Visão visual do pipeline, da camada de dados e da relação entre as views.

---

## 1. Pipeline end-to-end (Code-Interpreter RAG)

```mermaid
flowchart TD
    U([Usuário])
    U -->|pergunta em português| API

    subgraph API ["FastAPI  /query"]
        direction TB
        GEN["LLM — geração de SQL\n(GPT-4o / Ollama)"]
        VAL{"validação\nSELECT only?"}
        EXEC["DuckDB\nexecuta SQL"]
        EXP["LLM — explicação\ndo resultado"]
        GEN --> VAL
        VAL -->|ok| EXEC
        VAL -->|bloqueado| ERR[("erro retornado")]
        EXEC --> EXP
    end

    DB[("data/processed/**/*.parquet\ndata/enriched/**/*.parquet")]
    EXEC <-->|leitura analítica| DB

    EXP -->|sql + resultado + explicação| U
```

> **Princípio central:** o LLM nunca lê os dados brutos — gera SQL, recebe o agregado e explica o resultado.

---

## 2. Pipeline de ingestão e transformação de dados

```mermaid
flowchart LR
    FTP[("DATASUS\nFTP público")]
    FTP -->|arquivos .dbc| ING["ingestion.py\nDBC → DBF → Parquet"]
    ING --> RAW[("data/raw/\nParquet bruto\nSIA + SIH")]

    RAW --> TRF["transform.py\nnormalização + schema canônico"]
    TRF --> PROC[("data/processed/\n~21 M linhas · 110 colunas\nview: processed")]

    PROC --> AHEN["enriquecimento AHEN\nnarrativa clínica por registro"]
    AHEN --> ENR[("data/enriched/\nview: enriched\nclinical_* colunas")]
```

---

## 3. Relação entre as views `processed` e `enriched`

```mermaid
erDiagram
    processed {
        string  row_id              "chave de ligação"
        string  sistema             "SIA | SIH"
        string  uf_origem           "sigla da UF"
        int     ano_cmpt            "ano AAAA"
        string  cid_principal       "CID-10 sem ponto"
        string  icd_group           "grupo CID"
        float   custo_total         "valor R$"
        string  n_aih               "ID internação (SIH)"
        int     morte               "óbito: 1 | 0"
    }

    enriched {
        string  row_id                          "FK → processed"
        string  clinical_interpretacao_clinica  "categoria clínica AHEN"
        string  clinical_tipo_atendimento       "ambulatorial | internação"
        string  clinical_deslocamento_territorial "sem | intermunicipal | interestadual"
        string  clinical_event_narrative        "narrativa textual completa"
    }

    processed ||--|| enriched : "JOIN ON row_id"
```

> `enriched` é opcional: use apenas quando o filtro for por `clinical_interpretacao_clinica` ou `clinical_deslocamento_territorial`. Para filtros por `icd_group` ou `cid_principal`, consulte somente `processed`.

---

## 4. Decisão de design: por que Code-Interpreter RAG e não Vector RAG

```mermaid
flowchart LR
    subgraph VEC ["Vector RAG (descartado)"]
        direction TB
        V1["vetorizar linhas\n21 M embeddings"] --> V2["busca semântica\n(top-k)"]
        V2 --> V3["LLM lê texto\nconstruído"]
        V3 --> V4(["resposta estimada\nsem cálculo real"])
    end

    subgraph CODE ["Code-Interpreter RAG (adotado)"]
        direction TB
        C1["dados em Parquet\ncolunar"] --> C2["LLM gera SQL\nexecutável"]
        C2 --> C3["DuckDB executa\nno dado real"]
        C3 --> C4(["resultado exato\nauditável"])
    end

    VEC -. "❌ impreciso para\nagregações numéricas" .-> CODE
```

[← Voltar ao índice](README.md)
