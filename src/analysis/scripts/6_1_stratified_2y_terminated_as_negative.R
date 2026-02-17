# ============================================
# Two-Level Mixed-Effects Logistic Regression Model
# Hackathon Level: (1 | hackathon_id)
# ============================================

library(tidyverse)
library(lme4)
library(pROC)
library(caret)
library(sjPlot)
library(performance)

load("data/new_2y/preprocessed_train.RData")
load("data/new_2y/preprocessed_test.RData")
load("data/new_2y/preprocessed_full.RData")

cat("========================================\n")
cat("Two-Level Mixed-Effects Logistic Regression Analysis\n")
cat("========================================\n\n")

formula_main <- as.formula(
  "outcome_binary ~ h_duration_log_std + hackathon_size_std + is_offline_event +
   avg_outside_repos_within_std + avg_outside_repos_hackathon_mean_std +
   team_contributor_size_std + common_event_num_std +
   (1 | hackathon_id)"
)

fit_model <- function(data, formula, model_name, outcome_positive, outcome_negative) {
  
  cat("\n", paste(rep("=", 60), collapse=""), "\n")
  cat("Fitting model:", model_name, "\n")
  cat(paste(rep("=", 60), collapse=""), "\n\n")
  
  data_subset <- data %>%
    filter(collaboration %in% c(outcome_positive, outcome_negative)) %>%
    mutate(outcome_binary = ifelse(collaboration == outcome_positive, 1, 0))
  
  cat("Sample size:", nrow(data_subset), "\n")
  cat("Positive class (", outcome_positive, "):", sum(data_subset$outcome_binary == 1), "\n")
  cat("Negative class (", paste(outcome_negative, collapse="+"), "):", 
      sum(data_subset$outcome_binary == 0), "\n")
  cat("Positive class proportion:", round(mean(data_subset$outcome_binary) * 100, 2), "%\n\n")
  
  cat("Starting model fitting...\n")
  start_time <- Sys.time()
  
  model <- glmer(formula, 
                 data = data_subset, 
                 family = binomial(link = "logit"),
                 control = glmerControl(optimizer = "bobyqa",
                                        optCtrl = list(maxfun = 100000)))
  
  end_time <- Sys.time()
  cat("Model fitting completed, time used:", round(difftime(end_time, start_time, units = "mins"), 2), "minutes\n\n")
  
  if(model@optinfo$conv$opt != 0) {
    warning("Model may not have converged")
  } else {
    cat("Model successfully converged\n\n")
  }
  
  return(list(model = model, data = data_subset))
}

extract_results <- function(model, model_name) {
  
  coef_summary <- summary(model)$coefficients
  coef_df <- data.frame(
    variable = rownames(coef_summary),
    estimate = coef_summary[, "Estimate"],
    se = coef_summary[, "Std. Error"],
    z_value = coef_summary[, "z value"],
    p_value = coef_summary[, "Pr(>|z|)"]
  ) %>%
    mutate(
      OR = exp(estimate),
      OR_lower = exp(estimate - 1.96 * se),
      OR_upper = exp(estimate + 1.96 * se),
      sig = case_when(
        p_value < 0.001 ~ "***", p_value < 0.01 ~ "**",
        p_value < 0.05 ~ "*", p_value < 0.1 ~ ".", TRUE ~ ""
      )
    )
  
  cat("\n--- Collinearity Check (VIF) ---\n")
  vif_res <- performance::check_collinearity(model)
  print(vif_res)
  
  vif_df <- as.data.frame(vif_res) %>%
    rename(variable = Term, vif = VIF) %>%
    select(variable, vif)
  
  coef_df <- left_join(coef_df, vif_df, by = "variable") %>%
    mutate(across(where(is.numeric), ~round(., 4)))
  
  random_effects <- VarCorr(model)
  hackathon_var <- as.numeric(random_effects$hackathon_id[1])
  icc <- hackathon_var / (hackathon_var + pi^2/3)
  
  return(list(
    coef_df = coef_df,
    vif_info = vif_df,
    random_effects = data.frame(component = "hackathon_id", variance = hackathon_var, sd = sqrt(hackathon_var), icc = icc),
    fit_stats = data.frame(model = model_name, AIC = AIC(model), BIC = BIC(model), logLik = as.numeric(logLik(model)), n_obs = nobs(model), n_groups = ngrps(model))
  ))
}

evaluate_model <- function(model, test_data, outcome_positive, outcome_negative, model_name) {
  
  cat("\n", paste(rep("=", 60), collapse=""), "\n")
  cat("Evaluating model:", model_name, "\n")
  cat(paste(rep("=", 60), collapse=""), "\n\n")
  
  test_subset <- test_data %>%
    filter(collaboration %in% c(outcome_positive, outcome_negative)) %>%
    mutate(outcome_binary = ifelse(collaboration == outcome_positive, 1, 0))
  
  test_subset$pred_prob <- predict(model, newdata = test_subset, 
                                   type = "response", allow.new.levels = TRUE)
  test_subset$pred_class <- ifelse(test_subset$pred_prob > 0.5, 1, 0)
  
  conf_matrix <- confusionMatrix(factor(test_subset$pred_class, levels = c(0, 1)),
                                 factor(test_subset$outcome_binary, levels = c(0, 1)),
                                 positive = "1")
  
  cat("Confusion matrix:\n")
  print(conf_matrix$table)
  cat("\n")
  
  accuracy <- conf_matrix$overall["Accuracy"]
  sensitivity <- conf_matrix$byClass["Sensitivity"]
  specificity <- conf_matrix$byClass["Specificity"]
  precision <- conf_matrix$byClass["Precision"]
  f1 <- conf_matrix$byClass["F1"]
  
  cat("Classification performance:\n")
  cat("  Accuracy:", round(accuracy, 4), "\n")
  cat("  Sensitivity (Recall):", round(sensitivity, 4), "\n")
  cat("  Specificity:", round(specificity, 4), "\n")
  cat("  Precision:", round(precision, 4), "\n")
  cat("  F1 Score:", round(f1, 4), "\n\n")
  
  roc_obj <- roc(test_subset$outcome_binary, test_subset$pred_prob, quiet = TRUE)
  auc_value <- auc(roc_obj)
  
  cat("AUC:", round(auc_value, 4), "\n\n")
  
  performance_metrics <- data.frame(
    model = model_name,
    n_test = nrow(test_subset),
    accuracy = as.numeric(accuracy),
    sensitivity = as.numeric(sensitivity),
    specificity = as.numeric(specificity),
    precision = as.numeric(precision),
    f1 = as.numeric(f1),
    auc = as.numeric(auc_value)
  ) %>%
    mutate(across(where(is.numeric) & !matches("n_test"), ~round(., 4)))
  
  return(list(
    performance = performance_metrics,
    roc = roc_obj,
    predictions = test_subset
  ))
}

cat("\n\n")
cat("####################################################\n")
cat("##                                                ##\n")
cat("##  Model 1: Triggered vs. Temporary              ##\n")
cat("##  (No Prior Collaboration → Post-Hackathon Outcome) ##\n")
cat("##                                                ##\n")
cat("####################################################\n\n")

cat("\n>>> 1.1 Main Model (All Variables) - Training Set <<<\n")
m1_main_train <- fit_model(
  data = df_train,
  formula = formula_main,
  model_name = "Model1_Main_Train",
  outcome_positive = "triggered",
  outcome_negative = "temporary"
)

results_m1_main <- extract_results(m1_main_train$model, "Model1_Main")

cat("\n>>> 1.2 Main Model - Test Set Evaluation <<<\n")
eval_m1_main_test <- evaluate_model(
  model = m1_main_train$model,
  test_data = df_test,
  outcome_positive = "triggered",
  outcome_negative = "temporary",
  model_name = "Model1_Main_Test"
)

cat("\n>>> 1.3 Main Model (All Variables) - Full Sample (Final Inference) <<<\n")
m1_main_full <- fit_model(
  data = df_preprocessed,
  formula = formula_main,
  model_name = "Model1_Main_Full",
  outcome_positive = "triggered",
  outcome_negative = "temporary"
)

results_m1_main_full <- extract_results(m1_main_full$model, "Model1_Main_Full")

cat("\n\n")
cat("####################################################\n")
cat("##                                                ##\n")
cat("##  Model 2: Sustained vs. Terminated             ##\n")
cat("##  (Prior Collaboration → Post-Hackathon Outcome) ##\n")
cat("##                                                ##\n")
cat("####################################################\n\n")

cat("\n>>> 2.1 Main Model (All Variables) - Training Set <<<\n")
m2_main_train <- fit_model(
  data = df_train,
  formula = formula_main,
  model_name = "Model2_Main_Train",
  outcome_positive = "sustained",
  outcome_negative = "terminated"
)

results_m2_main <- extract_results(m2_main_train$model, "Model2_Main")

cat("\n>>> 2.2 Main Model - Test Set Evaluation <<<\n")
eval_m2_main_test <- evaluate_model(
  model = m2_main_train$model,
  test_data = df_test,
  outcome_positive = "sustained",
  outcome_negative = "terminated",
  model_name = "Model2_Main_Test"
)

cat("\n>>> 2.3 Main Model (All Variables) - Full Sample (Final Inference) <<<\n")
m2_main_full <- fit_model(
  data = df_preprocessed,
  formula = formula_main,
  model_name = "Model2_Main_Full",
  outcome_positive = "sustained",
  outcome_negative = "terminated"
)

results_m2_main_full <- extract_results(m2_main_full$model, "Model2_Main_Full")

dir.create("results/new_2y/models_2level", showWarnings = FALSE, recursive = TRUE)

save(m1_main_full, m2_main_full, 
     file = "results/new_2y/models_2level/models_full.RData")

save(m1_main_train, m2_main_train,
     file = "results/new_2y/models_2level/models_train.RData")

cat("Model objects saved\n")

all_coefs <- bind_rows(
  results_m1_main_full$coef_df %>% mutate(model = "Model1_Main"),
  results_m2_main_full$coef_df %>% mutate(model = "Model2_Main")
)
write.csv(all_coefs, "results/new_2y/models_2level/coefficients_all.csv", row.names = FALSE)

all_random <- bind_rows(
  results_m1_main_full$random_effects %>% mutate(model = "Model1_Main"),
  results_m2_main_full$random_effects %>% mutate(model = "Model2_Main")
)
write.csv(all_random, "results/new_2y/models_2level/random_effects.csv", row.names = FALSE)

all_fit_stats <- bind_rows(
  results_m1_main_full$fit_stats,
  results_m2_main_full$fit_stats
)
write.csv(all_fit_stats, "results/new_2y/models_2level/model_fit_stats.csv", row.names = FALSE)
