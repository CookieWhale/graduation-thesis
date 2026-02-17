# ============================================
# Data Cleaning, Preprocessing, Correlation Analysis and Splitting
# ============================================

library(tidyverse)
library(lme4)
library(corrplot)
library(caret)

load("data/new_2y/dataset_trimmed_2y.RData")
df <- dataset_trimmed_2y

cat("Original data dimensions:", dim(df), "\n\n")

cat("========================================\n")
cat("Step 2: Variable Transformation\n")
cat("========================================\n\n")

df <- df %>%
  mutate(
    h_duration_log = log(h_duration + 1)
  )

cat("Log transformation completed:")
cat("  - h_duration_log = log(h_duration + 1)\n\n")

cat("Descriptive statistics of h_duration_log:\n")
summary_transformed <- df %>%
  select(h_duration, h_duration_log) %>%
  summary()
print(summary_transformed)
cat("\n")

cat("========================================\n")
cat("Step 3: Group-mean Centering\n")
cat("========================================\n\n")

df <- df %>%
  group_by(hackathon_id) %>%
  mutate(
    avg_outside_repos_hackathon_mean = mean(avg_outside_repos_before, na.rm = TRUE),
    avg_outside_repos_within = avg_outside_repos_before - avg_outside_repos_hackathon_mean
  ) %>%
  ungroup()

cat("Group-mean centering completed:\n")
cat("  - avg_outside_repos_within (within-hackathon deviation)\n")
cat("  - avg_outside_repos_hackathon_mean (hackathon-level mean)\n\n")

cat("Verification of centering:\n")
cat("  Mean of avg_outside_repos_within should be close to 0:", 
    round(mean(df$avg_outside_repos_within), 6), "\n\n")

cat("========================================\n")
cat("Step 4: Standardization\n")
cat("========================================\n\n")

vars_to_scale <- c(
  "h_duration_log",
  "hackathon_size",
  "avg_outside_repos_within",
  "avg_outside_repos_hackathon_mean",
  "team_contributor_size",
  "common_event_num"
)

scaling_params <- df %>%
  select(all_of(vars_to_scale)) %>%
  summarise(across(everything(), 
                   list(mean = ~mean(., na.rm = TRUE),
                        sd = ~sd(., na.rm = TRUE)))) %>%
  pivot_longer(everything(),
               names_to = c("variable", ".value"),
               names_pattern = "(.*)_(mean|sd)")

cat("Standardization parameters (based on full sample):\n")
print(scaling_params)
cat("\n")

df_preprocessed <- df %>%
  mutate(across(all_of(vars_to_scale), 
                ~scale(.)[,1],
                .names = "{.col}_std"))

cat("Standardization completed, new variables added with '_std' suffix\n\n")

cat("Verification of standardization (first 3 variables):\n")
verification <- df_preprocessed %>%
  select(ends_with("_std")) %>%
  select(1:3) %>%
  summarise(across(everything(), 
                   list(mean = ~mean(., na.rm = TRUE),
                        sd = ~sd(., na.rm = TRUE))))
print(verification)
cat("\n")

cat("========================================\n")
cat("Step 5: Prepare Data Subsets\n")
cat("========================================\n\n")

df_model1 <- df_preprocessed %>%
  filter(collaboration %in% c("triggered", "temporary"))

df_model2 <- df_preprocessed %>%
  filter(collaboration %in% c("sustained", "terminated"))

cat("Subset sample sizes:\n")
cat("  Model 1 (No Prior Collab):", nrow(df_model1), "\n")
cat("  Model 2 (Prior Collab):", nrow(df_model2), "\n\n")

cat("========================================\n")
cat("Step 6: Correlation Analysis\n")
cat("========================================\n\n")

cor_vars <- c(
  "h_duration_log_std",
  "hackathon_size_std",
  "avg_outside_repos_within_std",
  "avg_outside_repos_hackathon_mean_std",
  "team_contributor_size_std",
  "common_event_num_std",
  "is_offline_event"
)

compute_correlation <- function(data, vars, subset_name) {
  
  cor_matrix <- data %>%
    select(all_of(vars)) %>%
    cor(use = "pairwise.complete.obs")
  
  cat("\n", subset_name, "correlation matrix:\n")
  print(round(cor_matrix, 3))
  
  high_cor <- which(abs(cor_matrix) > 0.7 & abs(cor_matrix) < 1, arr.ind = TRUE)
  
  if(nrow(high_cor) > 0) {
    cat("\nHigh correlation pairs (|r| > 0.7):\n")
    for(i in 1:nrow(high_cor)) {
      row_idx <- high_cor[i, 1]
      col_idx <- high_cor[i, 2]
      if(row_idx < col_idx) {
        cat("  ", rownames(cor_matrix)[row_idx], "vs", 
            colnames(cor_matrix)[col_idx], ": r =", 
            round(cor_matrix[row_idx, col_idx], 3), "\n")
      }
    }
  } else {
    cat("\nNo severe multicollinearity detected\n")
  }
  
  pdf(paste0("results/new_2y/correlation_", gsub(" ", "_", tolower(subset_name)), ".pdf"), 
      width = 10, height = 10)
  corrplot(cor_matrix, 
           method = "color",
           type = "upper",
           tl.col = "black",
           tl.srt = 45,
           addCoef.col = "black",
           number.cex = 0.7,
           title = paste("Correlation Matrix -", subset_name),
           mar = c(0,0,2,0))
  dev.off()
  
  return(cor_matrix)
}

cor_model1 <- compute_correlation(df_model1, cor_vars, "Model 1")
cor_model2 <- compute_correlation(df_model2, cor_vars, "Model 2")

write.csv(cor_model1, "results/new_2y/correlation_model1.csv", row.names = TRUE)
write.csv(cor_model2, "results/new_2y/correlation_model2.csv", row.names = TRUE)

cat("\nCorrelation analysis completed, results saved\n\n")

cat("========================================\n")
cat("Step 7: Train-Test Split\n")
cat("========================================\n\n")

set.seed(42)

unique_hackathons <- unique(df_preprocessed$hackathon_id)
n_hackathons <- length(unique_hackathons)

cat("Total number of hackathons:", n_hackathons, "\n")

train_hackathons <- sample(unique_hackathons, 
                           size = floor(0.8 * n_hackathons),
                           replace = FALSE)

df_train <- df_preprocessed %>%
  filter(hackathon_id %in% train_hackathons)

df_test <- df_preprocessed %>%
  filter(!hackathon_id %in% train_hackathons)

cat("\nTraining set:\n")
cat("  Hackathons:", n_distinct(df_train$hackathon_id), "\n")
cat("  Observations:", nrow(df_train), "\n")
cat("  Proportion:", round(nrow(df_train) / nrow(df_preprocessed) * 100, 2), "%\n")

cat("\nTest set:\n")
cat("  Hackathons:", n_distinct(df_test$hackathon_id), "\n")
cat("  Observations:", nrow(df_test), "\n")
cat("  Proportion:", round(nrow(df_test) / nrow(df_preprocessed) * 100, 2), "%\n")

cat("\nTraining set outcome distribution:\n")
print(table(df_train$collaboration))
cat("\nTest set outcome distribution:\n")
print(table(df_test$collaboration))

cat("\n========================================\n")
cat("Step 8: Save Preprocessed Data\n")
cat("========================================\n\n")

save(df_preprocessed, file = "data/new_2y/preprocessed_full.RData")
cat("Saved: data/new_2y/preprocessed_full.RData\n")

save(df_train, file = "data/new_2y/preprocessed_train.RData")
cat("Saved: data/new_2y/preprocessed_train.RData\n")

save(df_test, file = "data/new_2y/preprocessed_test.RData")
cat("Saved: data/new_2y/preprocessed_test.RData\n")

save(scaling_params, file = "data/new_2y/scaling_params.RData")
cat("Saved: data/new_2y/scaling_params.RData\n")

save(train_hackathons, file = "data/new_2y/train_hackathons.RData")
cat("Saved: data/new_2y/train_hackathons.RData\n")

cat("\n========================================\n")
cat("Preprocessing Summary Report\n")
cat("========================================\n\n")

preprocessing_summary <- data.frame(
  Step = c(
    "1. Original Data",
    "2. Remove Missing Values",
    "3. Log Transformation",
    "4. Group-mean Centering",
    "5. Standardization",
    "6. Training Set",
    "7. Test Set"
  ),
  N_observations = c(
    nrow(df),
    nrow(df),
    nrow(df),
    nrow(df),
    nrow(df_preprocessed),
    nrow(df_train),
    nrow(df_test)
  ),
  N_hackathons = c(
    n_distinct(df$hackathon_id),
    n_distinct(df$hackathon_id),
    n_distinct(df$hackathon_id),
    n_distinct(df$hackathon_id),
    n_distinct(df_preprocessed$hackathon_id),
    n_distinct(df_train$hackathon_id),
    n_distinct(df_test$hackathon_id)
  ),
  Notes = c(
    "Originally loaded data",
    "Removed missing avg_outside_repos_before",
    "h_duration_log = log(h_duration + 1)",
    "avg_outside_repos_within & _hackathon_mean",
    "7 variables standardized (added _std suffix)",
    paste0(round(nrow(df_train)/nrow(df_preprocessed)*100, 1), "% of full data"),
    paste0(round(nrow(df_test)/nrow(df_preprocessed)*100, 1), "% of full data")
  )
)

print(preprocessing_summary)

write.csv(preprocessing_summary, 
          "results/new_2y/preprocessing_summary.csv",
          row.names = FALSE)

cat("\n\nFinal variable list:\n")
cat("========================================\n")

variable_list <- data.frame(
  Category = c(
    rep("Outcome", 1),
    rep("ID Variables", 3),
    rep("Original Predictors", 6),
    rep("Transformed Variables", 1),
    rep("Centered Variables", 2),
    rep("Standardized Variables", 6)
  ),
  Variable = c(
    "collaboration",
    "hackathon_id", "project_id", "user1_id/user2_id",
    "h_duration", "hackathon_size", "is_offline_event",
    "avg_outside_repos_before", "team_contributor_size",
    "common_event_num", 
    "h_duration_log", 
    "avg_outside_repos_within", "avg_outside_repos_hackathon_mean",
    "h_duration_log_std", "hackathon_size_std", 
    "avg_outside_repos_within_std", "avg_outside_repos_hackathon_mean_std",
    "team_contributor_size_std", "common_event_num_std"
  ),
  Description = c(
    "4 categories: triggered, temporary, sustained, terminated",
    "Hackathon identifier", "Project/team identifier", "User pair identifiers",
    "Duration in days", "Number of participants", "0=online, 1=offline",
    "Avg repos before (original)", "Team size",
    "Common events count", 
    "log(h_duration + 1)",
    "Within-hackathon deviation", "Hackathon-level mean",
    "Standardized (mean=0, sd=1)", "Standardized",
    "Standardized (within)", "Standardized (between)",
    "Standardized", "Standardized"
  )
)

print(variable_list)

write.csv(variable_list,
          "results/new_2y/variable_list.csv",
          row.names = FALSE)

cat("\n========================================\n")
cat("Data preprocessing completed\n")
cat("========================================\n\n")

cat("Generated files:\n")
cat("Data files:\n")
cat("  data/new_2y/preprocessed_full.RData\n")
cat("  data/new_2y/preprocessed_train.RData\n")
cat("  data/new_2y/preprocessed_test.RData\n")
cat("  data/new_2y/scaling_params.RData\n")
cat("  data/new_2y/train_hackathons.RData\n")
cat("\nResult files:\n")
cat("  results/new_2y/correlation_model1.csv\n")
cat("  results/new_2y/correlation_model1.pdf\n")
cat("  results/new_2y/correlation_model2.csv\n")
cat("  results/new_2y/correlation_model2.pdf\n")
cat("  results/new_2y/preprocessing_summary.csv\n")
cat("  results/new_2y/variable_list.csv\n")
