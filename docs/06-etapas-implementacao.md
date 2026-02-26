# 6. Etapas de Implementação

## ETAPA 1 — Setup do ambiente

**Status:** concluída.

- Criar projeto Python com `pyproject.toml`
- Instalar: `duckdb`, `pandas`, `pyarrow`, `fastapi`, `uvicorn`, `langchain` (ou `llamaindex`), `python-dotenv`

## ETAPA 2 — Pipeline de dados

**Status:** concluída.

- **ingestion.py** (`python -m src.data.ingestion`): ingestão principal 100% Python. Faz diff da grade esperada com `data/raw/`, baixa DBC do DATASUS (FTP nativo com fallback S3), descompacta DBC->DBF, filtra em chunks e grava Parquet particionado em `data/raw/ano=X/uf=Y/sistema=SIH|SIA/`.
- **Retentativa e resiliência:** pode incluir alvos vindos de `logs/erros.log`; aplica retries, backoff/jitter, circuit breaker para timeout e ignora arquivos com muitas falhas recorrentes no log.
- **Fallback R (pontual):** quando o arquivo não existe no FTP/S3, o `ingestion.py` pode acionar `scripts/r/analise_ortopedia.R` para tentativa alternativa de obtenção do mesmo alvo.
- **transform.py** (`python -m src.data.transform`): lê `data/raw/` (fonte da verdade), aplica transformações e grava em `data/processed/` (camada derivada). Os arquivos em raw permanecem; reprocessamentos partem sempre de raw. Um arquivo por vez para controle de memória. Para estender, adicionar nova função `(df) -> df` em `TRANSFORM_STEPS`.

**Colunas derivadas e padronizações:**
- **custo_total:** normalização (não é soma). SIH e SIA têm colunas de valor com nomes diferentes; escolhemos a coluna apropriada por sistema e expomos como `custo_total` para visibilidade.
- **idade_grupo:** faixas 0-17, 18-59, 60+ a partir de pa_idade/idade. Pendente: artigo de referência para classificação dos grupos etários.
- **cid_capitulo:** primeiro caractere do CID (capítulo CID-10). Os grupos clínicos (`icd_group`) são derivados no pipeline de ingestão/transformação e devem ser referenciados no dicionário de dados/documentação.
- **ano_mes:** YYYYMM para competência. **Tipos:** colunas de valor/quantidade convertidas para numérico quando string. **UF:** padronizadas para 2 letras maiúsculas. **Strings:** trim e "nan"/"None" → vazio.

**Demais informações relevantes (4.3):** criar primeiro uma visualização bruta dos dados para entender como estão; em seguida fazer perguntas e, com base nelas, escolher o tipo de análise. Como os dados alimentam o RAG, é o próprio RAG que decidirá qual cálculo estatístico é mais adequado para cada pergunta.

**Logs:** R e Python usam o mesmo arquivo `logs/erros.log`. Formato de cada linha: `quando (ISO) | quem (Script R ou Python) | onde (componente/caminho) | o que aconteceu`.

### Domínio de dados identificado nos arquivos `data/raw/*.parquet`

Levantamento realizado sobre `data/raw/**/*.parquet` (3.224 arquivos; anos `2021` a `2025`; sistemas `SIA` e `SIH`).

- **Variáveis únicas no conjunto:** `335` (considerando nomes distintos, case-sensitive).
- **Variáveis comuns entre SIA e SIH (`16`):** `ano_cmpt`, `mes_cmpt`, `sistema`, `uf_origem`, `main_icd`, `icd_group`, `opm_flag`, `fisio_flag`, `mun_res_status`, `mun_res_tipo`, `mun_res_nome`, `mun_res_uf`, `mun_res_lat`, `mun_res_lon`, `mun_res_alt`, `mun_res_area`.
- **Variáveis presentes em SIA:** `92` colunas (produção ambulatorial, procedimento, perfil do atendimento e valores brutos).
- **Variáveis presentes em SIH:** `259` colunas (internação hospitalar, AIH, UTI, diagnósticos, permanência, desfecho, valores e auditoria/gestão).

**Variáveis SIA (amostra representativa do domínio):**
- Identificação e gestão: `pa_coduni`, `pa_gestao`, `pa_ufmun`, `pa_regct`, `pa_mvm`, `pa_cmp`.
- Procedimento e produção: `pa_proc_id`, `pa_qtdpro`, `pa_qtdapr`, `pa_tpfin`, `pa_subfin`, `pa_grupo`, `pa_subgru`.
- Profissional e estabelecimento: `pa_cnsmed`, `pa_cbocod`, `pa_cnpjcpf`, `pa_cnpjmnt`, `pa_nat_jur`.
- Clínica e perfil: `pa_cidpri`, `pa_cidsec`, `pa_cidcas`, `pa_idade`, `pa_sexo`, `pa_racacor`, `pa_etnia`.
- Valores: `pa_valpro`, `pa_valapr`, `pa_vl_cf`, `pa_vl_cl`, `pa_vl_inc`, `nu_vpa_tot`, `nu_pa_tot`, `pa_dif_val`.

**Variáveis SIH (amostra representativa do domínio):**
- Identificação da internação: `N_AIH`, `IDENT`, `CNES`, `CGC_HOSP`, `MUNIC_RES`, `MUNIC_MOV`.
- Datas e permanência: `DT_INTER`, `DT_SAIDA`, `DIAS_PERM`, `QT_DIARIAS`, `UTI_MES_TO`, `UTI_INT_TO`.
- Clínica e diagnóstico: `DIAG_PRINC`, `DIAG_SECUN`, `CID_PRINC`, `CID_ASSO`, `CID_MORTE`, `DIAGSEC1` ... `DIAGSEC9`.
- Perfil do paciente: `IDADE`, `SEXO`, `RACA_COR`, `ETNIA`, `NASC`, `MORTE`.
- Valores e faturamento: `VAL_SH`, `VAL_SP`, `VAL_SADT`, `VAL_UTI`, `VAL_TOT`, `VAL_SH_FED`, `VAL_SP_FED`, `VAL_SH_GES`, `VAL_SP_GES`.
- Gestão e auditoria: `GESTAO`, `GESTOR_COD`, `GESTOR_TP`, `AUD_JUST`, `SIS_JUST`, `REMESSA`, `SEQUENCIA`.

> Observação: no SIH existem variantes de nomenclatura (ex.: maiúsculas/minúsculas e `munRes*` vs `mun_res_*`) já nos arquivos brutos, por isso o total de colunas únicas no sistema é maior.

Para a lista integral das colunas e exemplo de linha preenchida por sistema, ver [06.1-dominio-colunas-completas.md](06.1-dominio-colunas-completas.md).


**Ao final do processamento — data/processed/**/*.parquet - o que você deve ver:**
| Estágio | Diretório | Conteúdo |
|---------|-----------|----------|
| **Fonte da verdade** | `data/raw/` | A ingestão Python grava direto em `ano=X/uf=Y/sistema=SIH|SIA/`. Arquivos não são removidos pelo transform. Todo reprocessamento parte daqui. |
| **Camada derivada** | `data/processed/` | Arquivos já agregados e transformados do pipeline (colunas padronizadas e derivadas). Sempre reconstruível a partir de raw. |

Fluxo: **ingestion (Python) -> data/raw/** (já particionado) **-> transform -> data/processed/**.

## ETAPA 3 — Camada DuckDB

- Função `query(sql: str) -> pd.DataFrame`
- Ler diretamente `data/processed/*.parquet`
- Sem banco externo; resposta em milissegundos

## ETAPA 4 — Agente LLM

- Recebe pergunta em português
- Analisa schema disponível
- Gera SQL seguro
- Executa no DuckDB
- Retorna resposta explicada
- Restrições: nunca inventar colunas; nunca estimar valores; sempre SQL executável

## ETAPA 5 — API

- Endpoint `POST /query`
- Entrada: `{ "question": "..." }`
- Saída: `{ "sql": "...", "result": "...", "explanation": "..." }`

[← Voltar ao índice](README.md)
