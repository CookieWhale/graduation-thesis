# ============================================
# Compute Key R² Metrics for Mixed-Effects Logistic Model
# - Marginal R² (Full Data)
# - Conditional R² (Full Data)
# - AUC (Test Set)
# ============================================

library(tidyverse)
library(lme4)
library(performance)
library(pROC)

cat("========================================\n")
cat("Computing Key R² Metrics\n")
cat("========================================\n\n")

cat("Loading model objects and data...\n")
load("results/new_2y/interaction_model/models.RData")
load("results/new_2y/interaction_model/prepared_data.RData")

cat("Load successful\n\n")

cat("Computing R²GLMM for full data...\n")

library(performance)
r2_perf <- r2_nakagawa(model_full)

r2_marginal <- r2_perf$R2_marginal
r2_conditional <- r2_perf$R2_conditional

cat(sprintf("  Marginal R² (R²m): %.4f\n", r2_marginal))
cat(sprintf("  Conditional R² (R²c): %.4f\n\n", r2_conditional))

cat("Computing AUC for test set...\n")

test_prepared$pred_prob <- predict(model_train, 
                                   newdata = test_prepared, 
                                   type = "response", 
                                   allow.new.levels = TRUE)

roc_obj <- roc(test_prepared$outcome_binary, 
               test_prepared$pred_prob, 
               quiet = TRUE)
auc_value <- as.numeric(auc(roc_obj))

cat(sprintf("  AUC (Test Set): %.4f\n\n", auc_value))

cat("Saving results...\n")

results <- data.frame(
  metric = c("Marginal R² (Full Data)", 
             "Conditional R² (Full Data)", 
             "AUC (Test Set)"),
  value = c(r2_marginal, r2_conditional, auc_value)
) %>%
  mutate(value = round(value, 4))

write.csv(results, 
          "results/new_2y/interaction_model/key_metrics.csv", 
          row.names = FALSE)

cat("Results saved to: results/new_2y/interaction_model/key_metrics.csv\n\n")

cat("========================================\n")
cat("Key Metrics Summary\n")
cat("========================================\n\n")

print(results, row.names = FALSE)

cat("\nCompleted\n")
