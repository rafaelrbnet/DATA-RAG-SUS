#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  if (!requireNamespace("microdatasus", quietly = TRUE)) {
    install.packages("microdatasus", repos = "https://cloud.r-project.org/", quiet = TRUE)
  }
  if (!requireNamespace("arrow", quietly = TRUE)) {
    install.packages("arrow", repos = "https://cloud.r-project.org/", quiet = TRUE)
  }
  library(microdatasus)
  library(arrow)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 4) {
  message("Uso: Rscript fallback_download_only.R UF ANO MES SISTEMA")
  quit(save = "no", status = 1)
}

uf <- toupper(args[1])
year <- as.integer(args[2])
month <- as.integer(args[3])
system <- args[4] # SIH-RD ou SIA-PA

if (!system %in% c("SIH-RD", "SIA-PA")) {
  message("Sistema inválido: use SIH-RD ou SIA-PA")
  quit(save = "no", status = 1)
}

if (dir.exists("data")) {
  project_root <- "."
} else if (dir.exists("../data")) {
  project_root <- ".."
} else {
  project_root <- "../.."
}

sistema_label <- ifelse(system == "SIH-RD", "SIH", "SIA")
prefix <- ifelse(system == "SIH-RD", "sih", "sia")
file_name <- paste0(prefix, "_", uf, "_", year, "_", sprintf("%02d", month), ".parquet")
out_dir <- file.path(project_root, "data", "raw", paste0("ano=", year), paste0("uf=", uf), paste0("sistema=", sistema_label))
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
cache_path <- file.path(out_dir, paste0(".download_", file_name))

if (file.exists(cache_path)) {
  message(paste("[ETAPA] 1/2 Download — cache já existe:", basename(cache_path)))
  message("[ETAPA] 2/2 Concluído — reutilizando cache local")
  quit(save = "no", status = 0)
}

max_attempts <- 3
attempt <- 1
success <- FALSE
raw_data <- NULL

while (attempt <= max_attempts && !success) {
  tryCatch({
    message(paste("[ETAPA] 1/2 Download — baixando do DATASUS |", system, uf, year, month, "| Tentativa", attempt))
    raw_data <- fetch_datasus(
      year_start = year, year_end = year,
      month_start = month, month_end = month,
      uf = uf, information_system = system
    )
    if (is.null(raw_data)) stop("Dados vieram NULL")
    success <- TRUE
  }, error = function(e) {
    msg <- conditionMessage(e)
    if (grepl("550", msg)) {
      attempt <<- max_attempts + 1
    } else {
      message(paste("⚠️ Erro tentativa", attempt, ":", msg))
      Sys.sleep(2)
    }
  })
  if (!success) attempt <- attempt + 1
}

if (!success || is.null(raw_data)) {
  message("[ETAPA] Falha — não foi possível baixar os dados.")
  quit(save = "no", status = 1)
}

message(paste("[ETAPA] 2/2 Persistência — gravando cache:", basename(cache_path)))
write_parquet(raw_data, cache_path)
message("[ETAPA] Concluído — download persistido com sucesso.")
quit(save = "no", status = 0)
