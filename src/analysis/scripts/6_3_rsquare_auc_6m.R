# ============================================
# Compute Key R² Metrics for Two-Level Mixed-Effects Logistic Models
# Model 1: triggered vs temporary
# Model 2: sustained vs terminated
# - Marginal R² (Full Data)
# - Conditional R² (Full Data)
# - AUC (Test Set)
# ============================================

library(tidyverse)
library(lme4)
library(performance)
library(pROC)

cat("========================================\n")
cat("Computing Key R² Metrics for Two-Level Models\n")
cat("========================================\n\n")

cat("Loading model objects and data...\n")
load("results/new_6m/models_2level/models_full.RData")
load("results/new_6m/models_2level/models_train.RData")
load("data/new_6m/preprocessed_test.RData")
load("data/new_6m/preprocessed_full.RData")

cat("Load successful\n\n")

calculate_metrics <- function(model_full, model_train, test_data, 
                              outcome_positive, outcome_negative, 
                              model_name) {
  
  cat("\n", paste(rep("=", 60), collapse=""), "\n")
  cat("Computing metrics:", model_name, "\n")
  cat(paste(rep("=", 60), collapse=""), "\n\n")
  
  cat("Computing R²GLMM for full data...\n")
  
  r2_perf <- r2_nakagawa(model_full$model)
  
  r2_marginal <- r2_perf$R2_marginal
  r2_conditional <- r2_perf$R2_conditional
  
  cat(sprintf("  Marginal R² (R²m): %.4f\n", r2_marginal))
  cat(sprintf("  Conditional R² (R²c): %.4f\n\n", r2_conditional))
  
  cat("Computing AUC for test set...\n")
  
  test_subset <- test_data %>%
    filter(collaboration %in% c(outcome_positive, outcome_negative)) %>%
    mutate(outcome_binary = ifelse(collaboration == outcome_positive, 1, 0))
  
  cat(sprintf("  Test sample size: %d\n", nrow(test_subset)))
  cat(sprintf("  Positive class (%s): %d\n", outcome_positive, sum(test_subset$outcome_binary == 1)))
  cat(sprintf("  Negative class (%s): %d\n", outcome_negative, sum(test_subset$outcome_binary == 0)))
  
  test_subset$pred_prob <- predict(model_train$model, 
                                   newdata = test_subset, 
                                   type = "response", 
                                   allow.new.levels = TRUE)
  
  roc_obj <- roc(test_subset$outcome_binary, 
                 test_subset$pred_prob, 
                 quiet = TRUE)
  auc_value <- as.numeric(auc(roc_obj))
  
  cat(sprintf("  AUC (Test Set): %.4f\n\n", auc_value))
  
  return(data.frame(
    model = model_name,
    comparison = paste(outcome_positive, "vs", outcome_negative),
    marginal_r2 = round(r2_marginal, 4),
    conditional_r2 = round(r2_conditional, 4),
    auc_test = round(auc_value, 4),
    n_full = nobs(model_full$model),
    n_test = nrow(test_subset)
  ))
}

cat("\n>>> Model 1: Triggered vs Temporary <<<\n")
metrics_m1 <- calculate_metrics(
  model_full = m1_main_full,
  model_train = m1_main_train,
  test_data = df_test,
  outcome_positive = "triggered",
  outcome_negative = "temporary",
  model_name = "Model1_Triggered_vs_Temporary"
)

cat("\n>>> Model 2: Sustained vs Terminated <<<\n")
metrics_m2 <- calculate_metrics(
  model_full = m2_main_full,
  model_train = m2_main_train,
  test_data = df_test,
  outcome_positive = "sustained",
  outcome_negative = "terminated",
  model_name = "Model2_Sustained_vs_Terminated"
)

cat("\n", paste(rep("=", 60), collapse=""), "\n")
cat("Saving results\n")
cat(paste(rep("=", 60), collapse=""), "\n\n")

all_metrics <- bind_rows(metrics_m1, metrics_m2)

write.csv(all_metrics, 
          "results/new_6m/models_2level/key_metrics.csv", 
          row.names = FALSE)

cat("Results saved to: results/new_6m/models_2level/key_metrics.csv\n\n")

cat("========================================\n")
cat("Key Metrics Summary\n")
cat("========================================\n\n")

print(all_metrics, row.names = FALSE)

cat("\n\n")
cat("========================================\n")
cat("Metric Interpretation\n")
cat("========================================\n\n")

cat("[Marginal R²]\n")
cat("  - Proportion of variance explained by fixed effects only\n")
cat("  - Reflects explanatory power of predictors\n\n")

cat("[Conditional R²]\n")
cat("  - Proportion of variance explained by fixed + random effects\n")
cat("  - Reflects overall model explanatory power\n\n")

cat("[AUC (Test Set)]\n")
cat("  - Classification performance on test data\n")
cat("  - Range: 0.5 (random) to 1.0 (perfect)\n")
cat("  - >0.7 is generally considered acceptable performance\n\n")

cat("Completed\n")
