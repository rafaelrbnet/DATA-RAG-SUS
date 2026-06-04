# 11. Enriquecimento com ORPHAcodes (Orphanet)

> **Status:** planejado — não implementado.  
> Este documento descreve a análise de viabilidade e o plano de implementação para enriquecer a base com códigos de doenças raras do Orphanet.

---

## 11.1 O que é o Orphanet / ORPHAcode

O [Orphanet](https://www.orphacode.org/) é o repositório de referência global para doenças raras, mantido pelo INSERM (França) com financiamento europeu. Cada doença rara recebe um identificador numérico único chamado **ORPHAcode** (ex.: `ORPHA:558` = Síndrome de Marfan).

O Orphanet publica cruzamentos entre ORPHAcodes e os principais sistemas de codificação clínica, incluindo ICD-10 — o mesmo sistema usado nos dados administrativos do SUS (campo `cid_principal`).

**Licença:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — uso livre com atribuição, sem necessidade de registro ou chave de API para os arquivos de download.

---

## 11.2 Sistemas de codificação disponíveis

O arquivo `pt_product1.json` (português, ~2 MB comprimido) disponibilizado no GitHub da Orphanet contém mapeamentos para **8 sistemas de codificação**:

| Sistema | Mapeamentos | Descrição |
|---------|------------|-----------|
| **UMLS** | 9.634 | Unified Medical Language System — interoperabilidade global |
| **OMIM** | 8.525 | Online Mendelian Inheritance in Man — doenças genéticas |
| **MONDO** | 8.355 | Mondo Disease Ontology — ontologia unificada de doenças |
| **ICD-10** | 8.340 | Sistema usado pelo SUS hoje |
| **ICD-11** | 6.420 | Versão nova — Brasil em processo de adoção |
| **GARD** | 3.833 | NIH Genetic and Rare Diseases (EUA) |
| **MeSH** | 3.216 | Indexação bibliográfica PubMed |
| **MedDRA** | 1.805 | Farmacovigilância e ensaios clínicos |

**Fonte:** `Orphadata_aggregated/Rare diseases and classifications/Cross-referencing of rare diseases/JSON/pt_product1.json.tar.gz`  
**GitHub:** https://github.com/Orphanet/Orphadata_aggregated  
**Atualização:** 2× ao ano (julho e dezembro)

---

## 11.3 Cobertura na base SUS

Análise realizada em 04/06/2026 cruzando `pt_product1.json` com `data/processed/**/*.parquet`:

| Métrica | Valor |
|---------|-------|
| Doenças raras catalogadas no Orphanet | 11.456 |
| Doenças com pelo menos 1 CID-10 presente na base SUS | **5.952** |
| CIDs distintos com correspondência Orphanet | **1.580 de 7.862** |
| Registros SUS que teriam enriquecimento (match direto) | **4.235.876** |
| Cobertura percentual da base | **19,7%** |

### Cobertura por capítulo CID

| Capítulo | Mapeamentos | Área clínica |
|----------|------------|--------------|
| Q | 2.536 | Malformações congênitas — maior cobertura |
| G | 986 | Doenças neurológicas raras |
| C | 673 | Neoplasias raras |
| E | 557 | Doenças endócrinas/metabólicas raras |
| D | 493 | Doenças do sangue e órgãos hematopoéticos |
| H | 198 | Doenças do olho e ouvido |
| M | 186 | Doenças osteomusculares raras |

### Tipos de relação ICD-10 ↔ ORPHAcode

| Relação | Mapeamentos | Significado |
|---------|------------|-------------|
| **NTBT** | 6.888 (82,6%) | ORPHAcode **mais específico** que o CID-10 |
| **BTNT** | 825 (9,9%) | ORPHAcode **mais abrangente** que o CID-10 |
| **Exata** | 614 (7,4%) | Equivalência direta entre os conceitos |
| **ND** | 13 (0,2%) | Relação indeterminada |

---

## 11.4 Limitação estrutural crítica

> **Atenção:** 82,6% dos mapeamentos são do tipo NTBT — o ORPHAcode é um **subtipo específico** de uma condição mais ampla codificada pelo CID-10. O dado administrativo do SUS registra o CID, não a doença rara específica.

**Exemplo:**

```
CID-10: Q77.3 (Condrodisplasia punctata)
  └─ ORPHA:166024  Síndrome de displasia epifisária múltipla-macrocefalia  (NTBT)
  └─ ORPHA:166032  Síndrome de displasia epifisária múltipla-miniepifises  (NTBT)
  └─ ORPHA:166029  Síndrome de displasia epifisária múltipla-displasia femoral  (NTBT)
```

Um registro com `cid_principal = 'Q773'` pode ser qualquer um dos três — ou nenhum deles (o médico pode ter usado Q77.3 para uma variante não catalogada). **Não é possível distinguir** com dado administrativo.

**O que o enriquecimento garante:**
- O CID registrado **pertence ao mesmo grupo clínico** de uma ou mais doenças raras conhecidas
- O registro é **candidato a investigação** de doença rara

**O que o enriquecimento NÃO garante:**
- Que o paciente tem a doença rara
- Qual das doenças raras mapeadas é a correta

Esta limitação deve ser explicitada em todas as consultas e explicações geradas pelo agente.

---

## 11.5 Estrutura do dado a ser gerado

**Arquivo de referência:** `data/schemas/orphacode_icd10.parquet`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `orpha_code` | string | ORPHAcode numérico (ex.: `'558'`) |
| `nome_pt` | string | Nome da doença em português |
| `tipo_doenca` | string | `'Doença'`, `'Síndrome malformativa'`, etc. |
| `icd10` | string | Código ICD-10 com ponto (ex.: `'Q87.4'`) |
| `icd10_sem_ponto` | string | Código sem ponto para JOIN com `cid_principal` (ex.: `'Q874'`) |
| `icd11` | string | Código ICD-11 equivalente (quando disponível) |
| `omim` | string | Código OMIM (quando disponível) |
| `relacao` | string | `'NTBT'`, `'BTNT'`, `'Exata'`, `'ND'` |
| `validacao` | string | `'Validado'` ou `'Ainda não validado'` |
| `fonte_versao` | string | Versão do arquivo fonte (ex.: `'1.3.42 / 4.1.8 [2025-03-03]'`) |
| `fonte_data` | string | Data de extração do arquivo |

**Chave de JOIN com `processed`:**
```sql
processed.cid_principal = orphacode.icd10_sem_ponto
```

---

## 11.6 Plano de implementação

### Etapa 1 — Script de download e geração do Parquet

**Arquivo:** `src/data/orphacode_sync.py`

```bash
python -m src.data.orphacode_sync
# Baixa pt_product1.json.tar.gz do GitHub
# Parseia todos os sistemas de codificação
# Grava data/schemas/orphacode_icd10.parquet
```

**Lógica de atualização:** verificar hash do arquivo remoto antes de baixar; reaplicar apenas se houve mudança.

### Etapa 2 — View no executor

**Arquivo:** `src/rag/executor.py`

```python
orphacode_glob = str((schemas_root / "orphacode_icd10.parquet").as_posix())
if Path(orphacode_glob).exists():
    con.execute(
        f"CREATE OR REPLACE VIEW orphacode AS "
        f"SELECT * FROM read_parquet('{orphacode_glob}')"
    )
```

### Etapa 3 — Schema no prompt do agente

**Arquivo:** `src/rag/prompts.py` — adicionar ao `SCHEMA_CONTEXT`:

```
VIEW: orphacode (referência Orphanet, JOIN via icd10_sem_ponto)

  orpha_code     string  ORPHAcode numérico
  nome_pt        string  Nome da doença rara em português
  icd10          string  Código ICD-10 com ponto
  icd10_sem_ponto string Código ICD-10 sem ponto (use para JOIN com processed.cid_principal)
  icd11          string  Código ICD-11 (quando disponível)
  relacao        string  'NTBT' | 'BTNT' | 'Exata' | 'ND'
  validacao      string  'Validado' | 'Ainda não validado'

JOIN:
  JOIN orphacode o ON processed.cid_principal = o.icd10_sem_ponto

ATENÇÃO: A maioria dos mapeamentos é NTBT — o CID-10 é mais genérico que a doença rara.
Um registro com match não confirma o diagnóstico da doença rara específica.
```

### Etapa 4 — Regra no SYSTEM_PROMPT

```
17. Para perguntas sobre doenças raras: use JOIN com a view `orphacode` via
    processed.cid_principal = orphacode.icd10_sem_ponto. Sempre inclua no
    comentário SQL a ressalva de que o match é por grupo clínico (relação NTBT),
    não confirmação diagnóstica individual.
```

### Etapa 5 — Testes

- Teste unitário: `tests/test_orphacode_sync.py` — parsing do JSON, geração do Parquet
- Teste de integração: query de doença rara conhecida retorna resultados coerentes
- Verificação de cobertura: script de auditoria mensal (% de CIDs com match)

---

## 11.7 Exemplos de consultas que se tornam possíveis

```sql
-- Quantos registros envolvem CIDs associados a doenças raras em 2023?
SELECT COUNT(*) AS candidatos_doenca_rara
FROM processed p
JOIN orphacode o ON p.cid_principal = o.icd10_sem_ponto
WHERE p.ano_cmpt = 2023;
-- Resultado esperado: ~800k (19,7% × 4,27M de 2023)

-- Quais doenças raras têm mais registros em SP?
SELECT o.nome_pt, o.orpha_code, COUNT(*) AS registros
FROM processed p
JOIN orphacode o ON p.cid_principal = o.icd10_sem_ponto
WHERE p.uf_origem = 'SP'
  AND o.relacao = 'Exata'  -- apenas mapeamentos exatos, mais confiáveis
GROUP BY o.nome_pt, o.orpha_code
ORDER BY registros DESC
LIMIT 20;

-- Custo total de internações com CIDs de doenças neuromusculares raras
SELECT SUM(p.custo_total) AS custo_total
FROM processed p
JOIN orphacode o ON p.cid_principal = o.icd10_sem_ponto
WHERE p.sistema = 'SIH'
  AND o.icd10 LIKE 'G%'
  AND o.validacao = 'Validado';

-- Cruzamento com código OMIM (para pesquisa genética)
SELECT o.nome_pt, o.omim, COUNT(*) AS registros
FROM processed p
JOIN orphacode o ON p.cid_principal = o.icd10_sem_ponto
WHERE o.omim IS NOT NULL
  AND p.ano_cmpt = 2023
GROUP BY o.nome_pt, o.omim
ORDER BY registros DESC;
```

---

## 11.8 Atribuição obrigatória (CC BY 4.0)

Todo uso dos dados Orphanet deve incluir a atribuição:

> Orphanet. *Cross-referencing of rare diseases — Portuguese edition*.  
> Disponível em: https://github.com/Orphanet/Orphadata_aggregated  
> Licença: CC BY 4.0. Acesso em: 2026-06-04.

Incluir esta atribuição:
- No `README.md` do projeto (seção Créditos/Licença)
- No cabeçalho do arquivo `data/schemas/orphacode_icd10.parquet` (metadado Parquet)
- Em publicações científicas que utilizem análises com ORPHAcodes

---

[← Voltar ao índice](README.md)
