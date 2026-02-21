suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(lubridate)
  library(stringr)
})

#' Normaliza valores de sexo para "M", "F" ou "Indefinido".
#'
#' Objetivo:
#' - Padronizar códigos heterogêneos vindos de SIH/SIA.
#' - Reduzir cardinalidade para análises e gráficos.
#'
#' Regras:
#' - Masculino: 1, M, MASC, MASCULINO, H, HOMEM, MALE
#' - Feminino: 2, 3, F, FEM, FEM., FEMININO, MULHER, FEMALE
#' - Demais valores: Indefinido
#'
#' @param x Vetor de entrada (character/numeric/factor).
#' @return Vetor character com valores "M", "F" ou "Indefinido".
normalizar_sexo <- function(x) {
  x_clean <- x %>%
    as.character() %>%
    str_trim() %>%
    str_to_upper()

  case_when(
    x_clean %in% c("1", "M", "MASC", "MASC.", "MASCULINO", "H", "HOMEM", "MALE") ~ "M",
    x_clean %in% c("2", "3", "F", "FEM", "FEM.", "FEMININO", "MULHER", "FEMALE") ~ "F",
    x_clean %in% c("", "NA", "N/A", "NULL", "IGN", "IGNORADO", "I", "9", "0") ~ "Indefinido",
    TRUE ~ "Indefinido"
  )
}

#' Constrói a base lazy no Arrow (sem carregar tudo na RAM).
#'
#' Objetivo:
#' - Selecionar apenas colunas necessárias.
#' - Padronizar tipos e nomes entre SIH/SIA.
#' - Permitir filtros antes do collect para melhor performance.
#'
#' @param input_dir Caminho da pasta com dados parquet.
#' @param ano_min Ano mínimo para filtro opcional (NULL = sem filtro).
#' @param ufs Vetor de UFs para filtro opcional (NULL = sem filtro).
#' @return Arrow Dataset query (lazy).
construir_base_lazy <- function(input_dir = "dados_output", ano_min = NULL, ufs = NULL) {
  ds <- open_dataset(input_dir)

  base_lazy <- ds %>%
    transmute(
      sistema,
      uf = uf_origem,
      ano_cmpt = as.integer(ano_cmpt),
      mes_cmpt = as.integer(mes_cmpt),
      grupo_doenca = icd_group,

      valor = case_when(
        sistema == "SIH" ~ as.numeric(val_tot),
        sistema == "SIA" ~ as.numeric(pa_valpro),
        TRUE ~ 0
      ),

      idade = case_when(
        sistema == "SIH" ~ as.numeric(idade),
        sistema == "SIA" ~ as.numeric(pa_idade),
        TRUE ~ NA_real_
      ),

      sexo_raw = case_when(
        sistema == "SIH" ~ as.character(sexo),
        sistema == "SIA" ~ as.character(pa_sexo),
        TRUE ~ NA_character_
      ),

      morte,
      opm_flag,
      fisio_flag
    )

  if (!is.null(ano_min)) {
    base_lazy <- base_lazy %>% filter(ano_cmpt >= ano_min)
  }

  if (!is.null(ufs) && length(ufs) > 0) {
    base_lazy <- base_lazy %>% filter(uf %in% ufs)
  }

  base_lazy
}

#' Materializa a base final já normalizada em memória.
#'
#' Objetivo:
#' - Executar collect uma única vez.
#' - Criar coluna de data mensal.
#' - Aplicar normalização de sexo após materialização.
#'
#' @param base_lazy Objeto retornado por construir_base_lazy().
#' @return Tibble final normalizado para análise.
materializar_dados_vis <- function(base_lazy) {
  base_lazy %>%
    collect() %>%
    mutate(
      data = make_date(ano_cmpt, mes_cmpt, 1L),
      sexo = normalizar_sexo(sexo_raw)
    ) %>%
    select(
      sistema, uf, ano_cmpt, mes_cmpt, data, grupo_doenca,
      valor, idade, sexo, morte, opm_flag, fisio_flag
    )
}

#' Gera indicadores de qualidade da normalização de sexo.
#'
#' Objetivo:
#' - Medir taxa de "Indefinido" por sistema e UF.
#' - Listar valores brutos mais frequentes que caíram em "Indefinido".
#'
#' @param dados_vis Tibble final produzido por materializar_dados_vis().
#' @param base_lazy Base lazy (para diagnóstico detalhado de sexo_raw).
#' @return Lista com dois data.frames: resumo e diagnostico.
validar_normalizacao_sexo <- function(dados_vis, base_lazy) {
  resumo <- dados_vis %>%
    group_by(sistema, uf) %>%
    summarise(
      total = n(),
      indefinido = sum(sexo == "Indefinido", na.rm = TRUE),
      pct_indefinido = round(100 * indefinido / total, 2),
      .groups = "drop"
    ) %>%
    arrange(desc(pct_indefinido), desc(indefinido))

  diagnostico <- base_lazy %>%
    collect() %>%
    mutate(
      sexo = normalizar_sexo(sexo_raw),
      sexo_clean = str_to_upper(str_trim(as.character(sexo_raw)))
    ) %>%
    filter(sexo == "Indefinido") %>%
    count(sistema, uf, sexo_raw, sexo_clean, sort = TRUE)

  list(resumo = resumo, diagnostico = diagnostico)
}

#' Salva dataset final em parquet particionado.
#'
#' Objetivo:
#' - Melhorar desempenho de leitura futura por filtros.
#' - Organizar por sistema/ano/UF.
#'
#' @param dados_vis Tibble final normalizado.
#' @param output_dir Pasta de saída.
#' @return Invisível, com efeito colateral de escrita em disco.
salvar_parquet_particionado <- function(dados_vis, output_dir = "dados_normalizados") {
  write_dataset(
    dataset = dados_vis,
    path = output_dir,
    format = "parquet",
    partitioning = c("sistema", "ano_cmpt", "uf"),
    existing_data_behavior = "overwrite_or_ignore"
  )
  invisible(NULL)
}

# ------------------------------
# Exemplo de execução ponta a ponta
# ------------------------------
base_lazy <- construir_base_lazy(
  input_dir = "dados_output",
  ano_min = 2022,
  ufs = NULL
)

dados_vis <- materializar_dados_vis(base_lazy)

qualidade <- validar_normalizacao_sexo(dados_vis, base_lazy)
print(qualidade$resumo, n = 50)
print(qualidade$diagnostico, n = 100)

salvar_parquet_particionado(dados_vis, output_dir = "dados_normalizados")
