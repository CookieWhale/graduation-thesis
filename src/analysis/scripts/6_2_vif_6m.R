# ============================================
# VIF Diagnostics - Multicollinearity Check
# Based on Full Sample Models
# ============================================

library(tidyverse)
library(lme4)
library(performance)

load("results/new_6m/models_2level/models_full.RData")
load("data/new_6m/preprocessed_full.RData")

cat("========================================\n")
cat("VIF Diagnostics - Multicollinearity Check\n")
cat("========================================\n\n")

dir.create("results/new_6m/models_2level/vif", showWarnings = FALSE, recursive = TRUE)

diagnose_vif <- function(model_obj, model_name) {
  
  cat("\n", paste(rep("=", 60), collapse=""), "\n")
  cat("VIF Diagnostics:", model_name, "\n")
  cat(paste(rep("=", 60), collapse=""), "\n\n")
  
  model <- model_obj$model
  
  cat("--- Multicollinearity Check (VIF) ---\n\n")
  
  vif_values <- check_collinearity(model)
  
  cat("Variance Inflation Factors:\n")
  print(vif_values)
  cat("\n")
  
  high_vif <- vif_values[vif_values$VIF > 5, ]
  if(nrow(high_vif) > 0) {
    cat("High VIF variables (VIF > 5):\n")
    print(high_vif)
    cat("\nRecommendation: Consider removing or combining these variables\n\n")
  } else {
    cat("No severe multicollinearity detected (all VIF < 5)\n\n")
  }
  
  write.csv(as.data.frame(vif_values), 
            paste0("results/new_6m/models_2level/vif/", model_name, "_VIF.csv"),
            row.names = FALSE)
  
  cat("VIF results saved to: results/new_6m/models_2level/vif/", model_name, "_VIF.csv\n\n")
  
  vif_summary <- data.frame(
    Model = model_name,
    Max_VIF = round(max(vif_values$VIF), 2),
    N_high_VIF = nrow(high_vif),
    High_VIF_vars = ifelse(nrow(high_vif) > 0, 
                           paste(high_vif$Parameter, collapse = ", "), 
                           "None")
  )
  
  return(list(
    vif = vif_values,
    summary = vif_summary
  ))
}

cat("========================================\n")
cat("Model 1: Triggered vs Temporary\n")
cat("========================================\n")

vif_m1 <- diagnose_vif(
  model_obj = m1_main_full,
  model_name = "Model1_Triggered_vs_Temporary"
)

cat("========================================\n")
cat("Model 2: Sustained vs Terminated\n")
cat("========================================\n")

vif_m2 <- diagnose_vif(
  model_obj = m2_main_full,
  model_name = "Model2_Sustained_vs_Terminated"
)

cat("\n\n")
cat("========================================\n")
cat("VIF Diagnostic Summary Comparison\n")
cat("========================================\n\n")

vif_comparison <- bind_rows(
  vif_m1$summary,
  vif_m2$summary
)

print(vif_comparison)
cat("\n")

write.csv(vif_comparison,
          "results/new_6m/models_2level/vif/vif_summary.csv",
          row.names = FALSE)

save(vif_m1, vif_m2,
     file = "results/new_6m/models_2level/vif/vif_results.RData")

cat("\n========================================\n")
cat("VIF Diagnostics Completed\n")
cat("========================================\n\n")

cat("Generated files:\n")
cat("  results/new_6m/models_2level/vif/Model1_Triggered_vs_Temporary_VIF.csv\n")
cat("  results/new_6m/models_2level/vif/Model2_Sustained_vs_Terminated_VIF.csv\n")
cat("  results/new_6m/models_2level/vif/vif_summary.csv\n")
cat("  results/new_6m/models_2level/vif/vif_results.RData\n\n")

cat("Key findings:\n")
cat("  • Model1 Max VIF:", vif_m1$summary$Max_VIF, "\n")
cat("  • Model2 Max VIF:", vif_m2$summary$Max_VIF, "\n\n")

if(vif_m1$summary$N_high_VIF > 0 || vif_m2$summary$N_high_VIF > 0) {
  cat("High VIF variables detected. Please review detailed results\n")
} else {
  cat("No severe multicollinearity detected in either model\n")
}
