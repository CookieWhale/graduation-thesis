library(tidyverse)
library(lme4)
library(kableExtra)

load("data/new_6m/dataset_trimmed_6m.RData")

df <- dataset_trimmed_6m

cat("Data dimensions:", dim(df), "\n")
cat("Variable names:", names(df), "\n\n")

descriptive_stats <- function(data, title) {
  
  cat_stats <- data %>%
    dplyr::count(collaboration) %>%
    dplyr::mutate(percentage = n / sum(n) * 100) %>%
    dplyr::arrange(desc(n))
  
  continuous_vars <- c("h_duration", "hackathon_size", 
                       "avg_outside_repos_before", "team_contributor_size",
                       "common_event_num", "time")
  
  cont_stats <- data %>%
    dplyr::select(all_of(continuous_vars)) %>%
    dplyr::summarise(across(everything(), 
                            list(mean = ~mean(., na.rm = TRUE),
                                 sd = ~sd(., na.rm = TRUE),
                                 median = ~median(., na.rm = TRUE),
                                 min = ~min(., na.rm = TRUE),
                                 max = ~max(., na.rm = TRUE),
                                 n_missing = ~sum(is.na(.))),
                            .names = "{.col}_{.fn}")) %>%
    tidyr::pivot_longer(everything(),
                        names_to = c("variable", ".value"),
                        names_pattern = "(.*)_(.*)") %>%
    dplyr::mutate(across(where(is.numeric), ~round(., 2)))
  
  offline_stats <- data %>%
    dplyr::count(is_offline_event) %>%
    dplyr::mutate(percentage = n / sum(n) * 100)
  
  cat("\n", paste(rep("=", 60), collapse=""), "\n")
  cat(title, "\n")
  cat(paste(rep("=", 60), collapse=""), "\n\n")
  
  cat("Sample size: ", nrow(data), "\n")
  cat("Number of hackathons: ", dplyr::n_distinct(data$hackathon_id), "\n")
  if("project_id" %in% names(data)) {
    cat("Number of projects: ", dplyr::n_distinct(data$project_id), "\n")
  }
  
  cat("\n--- Outcome Variable (collaboration) ---\n")
  print(as.data.frame(cat_stats))
  
  cat("\n--- Continuous/Count Variables ---\n")
  print(as.data.frame(cont_stats))
  
  cat("\n--- Binary Variable (is_offline_event) ---\n")
  print(as.data.frame(offline_stats))
  
  return(list(
    cat_stats = cat_stats,
    cont_stats = cont_stats,
    offline_stats = offline_stats,
    n_obs = nrow(data),
    n_hackathons = dplyr::n_distinct(data$hackathon_id),
    n_projects = if("project_id" %in% names(data)) dplyr::n_distinct(data$project_id) else NA
  ))
}

stats_full <- descriptive_stats(df, "TABLE 1: Full Sample Descriptive Statistics")

df_model1 <- df %>%
  dplyr::filter(collaboration %in% c("triggered", "temporary"))

cat("\nModel 1 subset size:", nrow(df_model1), "\n")

stats_model1 <- descriptive_stats(df_model1, 
                                  "TABLE 2: Model 1 Subset (No Prior Collaboration)")

df_model2 <- df %>%
  dplyr::filter(collaboration %in% c("sustained", "terminated"))

cat("\nModel 2 subset size:", nrow(df_model2), "\n")

stats_model2 <- descriptive_stats(df_model2,
                                  "TABLE 3: Model 2 Subset (Prior Collaboration Exists)")

create_comparison_table <- function(stats_list, names_list) {
  
  cont_comparison <- purrr::map2_df(stats_list, names_list, function(stats, name) {
    stats$cont_stats %>%
      dplyr::mutate(sample = name) %>%
      dplyr::select(sample, variable, mean, sd, median, min, max)
  })
  
  cont_wide <- cont_comparison %>%
    tidyr::pivot_longer(cols = c(mean, sd, median, min, max),
                        names_to = "statistic") %>%
    tidyr::unite("var_stat", variable, statistic) %>%
    tidyr::pivot_wider(names_from = sample, values_from = value)
  
  return(cont_wide)
}

comparison_table <- create_comparison_table(
  list(stats_full, stats_model1, stats_model2),
  c("Full Sample", "Model 1", "Model 2")
)

cat("\n", paste(rep("=", 60), collapse=""), "\n")
cat("COMPARISON TABLE: All Three Samples\n")
cat(paste(rep("=", 60), collapse=""), "\n\n")
print(as.data.frame(comparison_table))

dir.create("results", showWarnings = FALSE)

write.csv(stats_full$cont_stats, 
          "results/new_6m/trimmed_descriptive_stats_full.csv", 
          row.names = FALSE)

write.csv(stats_model1$cont_stats, 
          "results/new_6m/trimmed_descriptive_stats_model1.csv", 
          row.names = FALSE)

write.csv(stats_model2$cont_stats, 
          "results/new_6m/trimmed_descriptive_stats_model2.csv", 
          row.names = FALSE)

write.csv(comparison_table,
          "results/new_6m/trimmed_descriptive_stats_comparison.csv",
          row.names = FALSE)

outcome_summary <- data.frame(
  Sample = c(rep("Full", nrow(stats_full$cat_stats)), 
             rep("Model1", nrow(stats_model1$cat_stats)), 
             rep("Model2", nrow(stats_model2$cat_stats))),
  rbind(
    as.data.frame(stats_full$cat_stats),
    as.data.frame(stats_model1$cat_stats),
    as.data.frame(stats_model2$cat_stats)
  )
)

write.csv(outcome_summary,
          "results/new_6m/trimmed_outcome_distribution.csv",
          row.names = FALSE)

sample_size_summary <- data.frame(
  Sample = c("Full Sample", "Model 1 (No Prior Collab)", "Model 2 (Prior Collab)"),
  N_observations = c(stats_full$n_obs, stats_model1$n_obs, stats_model2$n_obs),
  N_hackathons = c(stats_full$n_hackathons, stats_model1$n_hackathons, stats_model2$n_hackathons),
  N_projects = c(stats_full$n_projects, stats_model1$n_projects, stats_model2$n_projects)
)

write.csv(sample_size_summary,
          "results/new_6m/trimmed_sample_size_summary.csv",
          row.names = FALSE)

cat("\n", paste(rep("=", 60), collapse=""), "\n")
cat("Descriptive statistics completed\n")
cat(paste(rep("=", 60), collapse=""), "\n\n")
cat("Sample size summary:\n")
print(sample_size_summary)
cat("\nResults saved to results/new_6m/trimmed_ folder:\n")
cat("  descriptive_stats_full.csv\n")
cat("  descriptive_stats_model1.csv\n")
cat("  descriptive_stats_model2.csv\n")
cat("  descriptive_stats_comparison.csv\n")
cat("  outcome_distribution.csv\n")
cat("  sample_size_summary.csv\n")
