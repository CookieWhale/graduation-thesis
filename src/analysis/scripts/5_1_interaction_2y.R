# ============================================
# Mixed-Effects Logistic Regression with Interaction Terms
# Positive class: triggered + sustained (precolab = 0 for triggered, 1 for sustained)
# Negative class: temporary + terminated (precolab = 0 for temporary, 1 for terminated)
# Model: P(Y=1) = Logit^{-1}(α + β₁ Pre + β₂ X + β₃ (Pre × X))
# Random effect at Hackathon level: (1 | hackathon_id)
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
cat("Mixed-Effects Logistic Regression with Precolab Interaction\n")
cat("========================================\n\n")

formula_interaction <- as.formula(
  "outcome_binary ~ precolab + 
   h_duration_log_std + hackathon_size_std + is_offline_event +
   avg_outside_repos_within_std + avg_outside_repos_hackathon_mean_std +
   team_contributor_size_std + common_event_num_std +
   precolab:h_duration_log_std + 
   precolab:hackathon_size_std + 
   precolab:is_offline_event +
   precolab:avg_outside_repos_within_std + 
   precolab:avg_outside_repos_hackathon_mean_std +
   precolab:team_contributor_size_std + 
   precolab:common_event_num_std +
   (1 | hackathon_id)"
)

prepare_data <- function(data) {
  
  cat("\n", paste(rep("=", 60), collapse=""), "\n")
  cat("Preparing data: creating outcome_binary and precolab variables\n")
  cat(paste(rep("=", 60), collapse=""), "\n\n")
  
  data_prepared <- data %>%
    mutate(
      outcome_binary = case_when(
        collaboration %in% c("triggered", "sustained") ~ 1,
        collaboration %in% c("temporary", "terminated") ~ 0,
        TRUE ~ NA_real_
      ),
      precolab = case_when(
        collaboration %in% c("triggered", "temporary") ~ 0,
        collaboration %in% c("sustained", "terminated") ~ 1,
        TRUE ~ NA_real_
      )
    ) %>%
    filter(!is.na(outcome_binary))
  
  cat("Total sample size:", nrow(data_prepared), "\n\n")
  
  cat("By collaboration category:\n")
  print(table(data_prepared$collaboration))
  cat("\n")
  
  cat("By outcome_binary:\n")
  cat("  Positive class (triggered + sustained):", sum(data_prepared$outcome_binary == 1), 
      paste0("(", round(mean(data_prepared$outcome_binary) * 100, 2), "%)"), "\n")
  cat("    - triggered (precolab=0):", sum(data_prepared$collaboration == "triggered"), "\n")
  cat("    - sustained (precolab=1):", sum(data_prepared$collaboration == "sustained"), "\n")
  cat("  Negative class (temporary + terminated):", sum(data_prepared$outcome_binary == 0),
      paste0("(", round((1-mean(data_prepared$outcome_binary)) * 100, 2), "%)"), "\n")
  cat("    - temporary (precolab=0):", sum(data_prepared$collaboration == "temporary"), "\n")
  cat("    - terminated (precolab=1):", sum(data_prepared$collaboration == "terminated"), "\n\n")
  
  cat("By precolab:\n")
  cat("  precolab=0 (triggered + temporary):", sum(data_prepared$precolab == 0), "\n")
  cat("  precolab=1 (sustained + terminated):", sum(data_prepared$precolab == 1), "\n\n")
  
  return(data_prepared)
}

fit_interaction_model <- function(data, formula, model_name) {
  
  cat("\n", paste(rep("=", 60), collapse=""), "\n")
  cat("Fitting model:", model_name, "\n")
  cat(paste(rep("=", 60), collapse=""), "\n\n")
  
  cat("Starting model fitting...\n")
  start_time <- Sys.time()
  
  model <- glmer(formula, 
                 data = data, 
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
  
  return(model)
}

extract_interaction_results <- function(model, model_name) {
  
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
        p_value < 0.001 ~ "***", 
        p_value < 0.01 ~ "**",
        p_value < 0.05 ~ "*", 
        p_value < 0.1 ~ ".", 
        TRUE ~ ""
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
  
  fit_stats <- data.frame(
    model = model_name,
    AIC = AIC(model),
    BIC = BIC(model),
    logLik = as.numeric(logLik(model)),
    n_obs = nobs(model),
    n_groups = ngrps(model)
  )
  
  return(list(
    coef_df = coef_df,
    vif_info = vif_df,
    random_effects = data.frame(
      component = "hackathon_id",
      variance = hackathon_var,
      sd = sqrt(hackathon_var),
      icc = icc
    ),
    fit_stats = fit_stats
  ))
}

evaluate_interaction_model <- function(model, test_data, model_name) {
  
  cat("\n", paste(rep("=", 60), collapse=""), "\n")
  cat("Evaluating model:", model_name, "\n")
  cat(paste(rep("=", 60), collapse=""), "\n\n")
  
  test_data$pred_prob <- predict(model, newdata = test_data, 
                                 type = "response", allow.new.levels = TRUE)
  test_data$pred_class <- ifelse(test_data$pred_prob > 0.5, 1, 0)
  
  conf_matrix <- confusionMatrix(
    factor(test_data$pred_class, levels = c(0, 1)),
    factor(test_data$outcome_binary, levels = c(0, 1)),
    positive = "1"
  )
  
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
  
  roc_obj <- roc(test_data$outcome_binary, test_data$pred_prob, quiet = TRUE)
  auc_value <- auc(roc_obj)
  
  cat("AUC:", round(auc_value, 4), "\n\n")
  
  performance_metrics <- data.frame(
    model = model_name,
    n_test = nrow(test_data),
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
    predictions = test_data
  ))
}


