## =========================
## 1. Environment Setup
## =========================
library(DBI)
library(RPostgres)

## =========================
## 2. Database Connection (modify if needed)
## =========================
con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = "thesis",
  host     = "localhost",
  port     = 5432,
  user     = "postgres",
  password = ""
)

## =========================
## 3. Import with Specified Object Name
## =========================
import_named_df_to_pg <- function(
    rdata_path,
    object_name,
    table_name,
    con,
    schema = "public"
) {
  
  load(rdata_path)
  
  if (!exists(object_name, envir = .GlobalEnv)) {
    stop(paste("Object does not exist:", object_name))
  }
  
  df <- get(object_name, envir = .GlobalEnv)
  
  if (!is.data.frame(df)) {
    stop(paste("Object is not a data.frame:", object_name))
  }
  
  df[] <- lapply(df, function(x) {
    if (is.factor(x)) as.character(x) else x
  })
  
  dbWriteTable(
    con,
    Id(schema = schema, table = table_name),
    df,
    overwrite = TRUE
  )
  
  message(sprintf(
    "Successfully imported %s::%s → %s.%s (%d rows)",
    rdata_path, object_name, schema, table_name, nrow(df)
  ))
}

## =========================
## 4. Import dataset_trimmed_2y
## =========================
import_named_df_to_pg(
  rdata_path = "data/new_2y/dataset_trimmed_2y.RData",
  object_name = "dataset_trimmed_2y",
  table_name  = "dataset_trimmed_2y",
  con = con
)

## =========================
## 5. Import dataset_trimmed_6m
## =========================
import_named_df_to_pg(
  rdata_path = "data/new_6m/dataset_trimmed_6m.RData",
  object_name = "dataset_trimmed_6m",
  table_name  = "dataset_trimmed_6m",
  con = con
)

## =========================
## 6. Disconnect
## =========================
dbDisconnect(con)
