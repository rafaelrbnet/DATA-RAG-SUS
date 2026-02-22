# ==============================================================================
# ETL DATASUS - SIH (RD) & SIA (PA) - VERSÃO FINAL 
# ==============================================================================

# --- Dependências: instala se faltar (primeira execução) ---
required_packages <- c("microdatasus", "tidyverse", "arrow", "fs", "janitor")
missing <- required_packages[!(required_packages %in% installed.packages()[, "Package"])]
if (length(missing) > 0) {
  message("Instalando pacotes necessários: ", paste(missing, collapse = ", "))
  install.packages(missing, repos = "https://cloud.r-project.org/", quiet = TRUE)
}

suppressPackageStartupMessages({
  library(microdatasus)
  library(tidyverse)
  library(arrow)
  library(fs)
  library(janitor)
})

# --- 1. CONFIGURAÇÕES GLOBAIS ---
options(timeout = 600) # Timeout aumentado para arquivos grandes do SIA
options(warn = -1)     # Suprime warnings não críticos

# Diretório raiz do projeto (funciona rodando da raiz ou de scripts/r/)
if (dir.exists("data")) {
  project_root <- "."
} else if (dir.exists("../data")) {
  project_root <- ".."
} else {
  project_root <- "../.."
}

# Saída alinhada à estrutura do projeto: data/processed/
output_dir <- path(project_root, "data", "processed")
if (!dir_exists(output_dir)) dir_create(output_dir)

# Log em logs/ na raiz do projeto
logs_dir <- path(project_root, "logs")
if (!dir_exists(logs_dir)) dir_create(logs_dir)
log_file <- path(logs_dir, "erros.log")

# Regex CIDs (Filtro Clínico)
cid_regex <- paste0(
  "^(",
  "E1[0-4]",   # Diabetes (Causa Base)
  "|I70",      # Aterosclerose (Má circulação)
  "|I73",      # Outras doenças vasculares
  "|I74",      # Trombose/Embolia
  "|L97",      # Úlcera/Ferida no pé (Pé diabético)
  "|M86",      # Osteomielite (Infecção no osso)
  "|S78",      # Amputação de Coxa (Alta)
  "|S88",      # Amputação de Perna (Média)
  "|S98",      # Amputação de Pé (Baixa)
  "|T13\\.6",  # Amputação de nível não especificado
  "|T87",      # Complicação do Coto (Infecção pós-op)
  "|Z89",      # Ausência de membro (Paciente em reabilitação)
  "|S72",      # Fratura de Fêmur (Trauma grave comparativo)
  ")"
)

# --- 2. FUNÇÕES AUXILIARES ---

append_log <- function(msg) {
  write(paste(Sys.time(), msg), file = log_file, append = TRUE)
}

classify_cid <- function(cid) {
  case_when(
    is.na(cid) ~ "Sem CID",
    str_detect(cid, "^E1[0-4]") ~ "Diabetes",
    str_detect(cid, "^(I70|I73|I74|L97)") ~ "Vascular",
    str_detect(cid, "^(S78|S88|S98|T13\\.6|S72)") ~ "Trauma",
    str_detect(cid, "^(Z89|T87|M86)") ~ "Pos-Amputacao",
    TRUE ~ "Outro"
  )
}

# --- 3. FUNÇÃO PRINCIPAL: DOWNLOAD E PROCESSAMENTO ---
process_datasus_file <- function(system, state, year, month) {
  
  # Define prefixo e nome do arquivo
  prefix <- ifelse(system == "SIH-RD", "sih", "sia")
  file_name <- paste0(prefix, "_", state, "_", year, "_", sprintf("%02d", month), ".parquet")
  file_path <- path(output_dir, file_name)
  
  # --- CHECKPOINT: Se já existe, não faz nada ---
  if (file_exists(file_path)) {
    return(NULL) 
  }
  
  max_attempts <- 3
  attempt <- 1
  success <- FALSE
  raw_data <- NULL
  
  # --- LOOP DE TENTATIVAS (RETRY) ---
  while(attempt <= max_attempts && !success) {
    tryCatch({
      message(paste("⬇️ [BAIXANDO]", system, state, year, month, "- Tentativa", attempt))
      
      # Baixa os dados
      raw_data <- fetch_datasus(
        year_start = year, year_end = year,
        month_start = month, month_end = month,
        uf = state, information_system = system
      )
      
      if (is.null(raw_data)) stop("Dados vieram NULL (Erro de conexão ou arquivo vazio)")
      
      success <- TRUE 
      
    }, error = function(e) {
      msg_erro <- conditionMessage(e)
      
      # Se for erro 550 (arquivo não existe no servidor), aborta as tentativas
      if (grepl("550", msg_erro)) {
        message(paste("⚠️ Arquivo inexistente no servidor (Erro 550):", state, year, month))
        attempt <<- max_attempts + 1 # Força saída do while
      } else {
        message(paste("⚠️ Erro tentativa", attempt, ":", msg_erro))
        Sys.sleep(2) # Espera 2s antes de tentar de novo
      }
    })
    
    if (!success) attempt <- attempt + 1
  }
  
  # --- SE FALHOU APÓS 3 TENTATIVAS, SAI DA FUNÇÃO ---
  if (!success || is.null(raw_data)) {
    append_log(paste("FALHA DEFINITIVA DOWNLOAD:", system, state, year, month))
    return(NULL) 
  }
  
  # --- PROCESSAMENTO (Só executa se baixou com sucesso) ---
  tryCatch({
    
    if (system == "SIH-RD") {
      # === LÓGICA SIH ===
      proc_data <- process_sih(raw_data) %>%
        janitor::clean_names() %>% 
        mutate(diag_princ = as.character(diag_princ)) %>%
        filter(str_detect(diag_princ, cid_regex) | str_detect(proc_rea, "^0415")) %>%
        mutate(
          opm_flag = FALSE, 
          fisio_flag = FALSE, 
          morte = NA_integer_, 
          sistema = "SIH", 
          main_icd = diag_princ
        )
      
    } else {
      # === LÓGICA SIA ===
      # SIA requer cuidado extra com nomes de colunas e tipos
      proc_data <- process_sia(raw_data) %>%
        janitor::clean_names()
      
      # Verifica se as colunas essenciais existem antes de processar
      if (!"pa_proc_id" %in% names(proc_data)) {
        stop("Coluna PA_PROC_ID não encontrada no arquivo SIA.")
      }
      
      proc_data <- proc_data %>%
        mutate(
          pa_proc_id = str_pad(as.character(pa_proc_id), 10, pad = "0", side = "left"),
          pa_cidpri  = as.character(pa_cidpri),
          pa_grupo   = substr(pa_proc_id, 1, 2),
          pa_subgru  = substr(pa_proc_id, 3, 4)
        ) %>%
        filter(
          (pa_grupo == "03" & pa_subgru == "02" & str_detect(pa_cidpri, cid_regex)) |
            (pa_grupo == "07" & pa_subgru %in% c("01", "02"))
        ) %>%
        mutate(
          opm_flag = pa_grupo == "07", 
          fisio_flag = pa_grupo == "03" & pa_subgru == "02", 
          sistema = "SIA", 
          main_icd = pa_cidpri
        )
    }
    
    # Libera raw_data logo após obter proc_data (reduz pico de RAM)
    rm(raw_data)
    gc(verbose = FALSE)
    
    # Salva o resultado em Parquet
    final_data <- proc_data %>%
      mutate(
        uf_origem = state, 
        ano_cmpt = year, 
        mes_cmpt = month, 
        icd_group = classify_cid(main_icd)
      )
    
    write_parquet(final_data, file_path)
    message(paste("💾 [SALVO]", system, file_name, "| Linhas:", nrow(final_data)))
    
    # Libera proc_data e final_data após gravar (reduz RAM para próximo arquivo)
    rm(proc_data, final_data)
    gc(verbose = FALSE)
    
  }, error = function(e) {
    # Erro durante o processamento (ex: coluna faltando, erro de memória)
    append_log(paste("ERRO PROCESSAMENTO:", system, state, year, month, "-", conditionMessage(e)))
    message(paste("❌ Erro ao processar dados baixados:", conditionMessage(e)))
  })
  
  # Limpeza de memória forçada (caso tenha sobrado algo por erro ou early return)
  if (exists("raw_data")) rm(raw_data)
  if (exists("proc_data")) rm(proc_data)
  if (exists("final_data")) rm(final_data)
  gc(verbose = FALSE)
  return(TRUE)
}

# --- 4. EXECUÇÃO ---

states <- c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", 
            "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", 
            "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO")

# Defina o período desejado
years <- 2021:2025 
months <- 1:12

message("Iniciando ETL DATASUS (SIH + SIA)...")

for (state in states) {
  message(paste("\n--- 🌍 Estado:", state, "---")) 
  
  for (year in years) {
    for (month in months) {
      
      # Pula datas futuras (loop_date serve para checar se já passamos da data atual)
      loop_date <- as.Date(paste(year, month, "01", sep="-"))
      if (!is.na(loop_date) && loop_date > Sys.Date()) next
      
      # 1. Processa SIH (Internações)
      process_datasus_file("SIH-RD", state, year, month)
      
      # 2. Processa SIA (Ambulatorial) - AGORA ATIVO
      process_datasus_file("SIA-PA", state, year, month)
      
      # Libera memória após cada par SIH+SIA (mês)
      gc(verbose = FALSE)
    }
  }
  # Limpeza pesada de memória ao trocar de estado
  gc(verbose = FALSE)
}

message("\n✅ Processo Finalizado!")