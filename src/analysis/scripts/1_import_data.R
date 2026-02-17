library(RPostgres)
library(DBI)

db_config <- list(
  host = "localhost",         
  port = 5432,                  
  dbname = "thesis",     
  user = "postgres",       
  password = ""    
)

con <- tryCatch({
  dbConnect(
    RPostgres::Postgres(),
    host = db_config$host,
    port = db_config$port,
    dbname = db_config$dbname,
    user = db_config$user,
    password = db_config$password
  )
}, error = function(e) {
  cat("connection failed")
  print(e)
  return(NULL)
})

if (is.null(con)) {
  stop("connection failed")
} else {
  cat("connection succeed")
  
}

dataset_new_6m <- tryCatch({
  dbReadTable(con, "dataset_new_6m")
}, error = function(e) {
  cat("import dataset_new_6m failed")
  print(e)
  return(NULL)
})

if (!is.null(dataset_new_6m)) {
  cat("dataset_new_6m ", nrow(dataset_new_6m), "rows", ncol(dataset_new_6m), "columns\n\n")
} else {
  cat("import dataset_new_6m failed\n")
}

dataset_new_2y <- tryCatch({
  dbReadTable(con, "dataset_new_2y")
}, error = function(e) {
  cat("dataset_new_2y import failed\n")
  print(e)
  return(NULL)
})

if (!is.null(dataset_new_2y)) {
  cat("dataset_new_2y import successful:", nrow(dataset_new_2y), "rows,", ncol(dataset_new_2y), "columns\n\n")
} else {
  cat("dataset_new_2y import failed\n\n")
}

dbDisconnect(con)
cat("Database connection closed\n\n")

cat("========================================\n")
cat("Data Import Summary\n")
cat("========================================\n\n")

if (!is.null(dataset_new_6m)) {
  cat("---------- dataset_new_6m ----------\n")
  cat("Data dimensions:", nrow(dataset_new_6m), "rows ×", ncol(dataset_new_6m), "columns\n")
  cat("Variable names:\n")
  print(names(dataset_new_6m))
  cat("\nCollaboration distribution (6 months):\n")
  print(table(dataset_new_6m$collaboration))
  cat("\nFirst 3 rows preview:\n")
  print(head(dataset_new_6m, 3))
  cat("\n")
}

if (!is.null(dataset_new_2y)) {
  cat("---------- dataset_new_2y ----------\n")
  cat("Data dimensions:", nrow(dataset_new_2y), "rows ×", ncol(dataset_new_2y), "columns\n")
  cat("Variable names:\n")
  print(names(dataset_new_2y))
  cat("\nCollaboration distribution (2 years):\n")
  print(table(dataset_new_2y$collaboration))
  cat("\nFirst 3 rows preview:\n")
  print(head(dataset_new_2y, 3))
  cat("\n")
}

cat("========================================\n")
cat("Data Quality Check\n")
cat("========================================\n\n")

if (!is.null(dataset_new_6m)) {
  cat("dataset_new_6m missing value summary:\n")
  missing_new_6m <- colSums(is.na(dataset_new_6m))
  if (sum(missing_new_6m) == 0) {
    cat("No missing values\n\n")
  } else {
    print(missing_new_6m[missing_new_6m > 0])
    cat("\n")
  }
}

if (!is.null(dataset_new_2y)) {
  cat("dataset_new_2y missing value summary:\n")
  missing_new_2y <- colSums(is.na(dataset_new_2y))
  if (sum(missing_new_2y) == 0) {
    cat("No missing values\n\n")
  } else {
    print(missing_new_2y[missing_new_2y > 0])
    cat("\n")
  }
}

save(dataset_new_6m, file = "data/new_6m/dataset_new_6m.RData")
save(dataset_new_2y, file = "data/new_2y/dataset_new_2y.RData")
cat("Data saved as RData format\n")

