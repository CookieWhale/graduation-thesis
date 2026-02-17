# ============================================================
# Script Function: Differential Trimming Data Cleaning
#  > vars_to_check <- c("h_duration", "hackathon_size", "time")
#  > 
#  > # Check 90%, 95%, 97.5%, 99%, 99.5% quantiles
#  > quantile_check <- sapply(df[vars_to_check], quantile, 
#                             +                          probs = c(0.9, 0.95, 0.975, 0.99, 0.995, 1), 
#                             +                          na.rm = TRUE)
#  > print(quantile_check)
#  h_duration hackathon_size    time
#  90%           36            923   57.00
#  95%           61           2064  101.00
#  97.5%         72           6833  351.00
#  99%           85          19396  730.00
#  99.5%        104          19396 1015.93
#  100%         402          19396 2190.00
# ============================================================


library(tidyverse)

load("data/new_2y/dataset_new_2y.RData")

df <- dataset_new_2y

n_original <- nrow(df)
cat("Original sample size:", n_original, "\n")

q_duration <- quantile(df$h_duration, 0.97, na.rm = TRUE)

q_size     <- quantile(df$hackathon_size, 0.97, na.rm = TRUE)

cat("--- Trimming Thresholds ---\n")
cat("- h_duration (97%):", q_duration, "\n")
cat("- hackathon_size (97%):", q_size, "\n")

dataset_trimmed_2y <- df %>%
  filter(
    h_duration <= q_duration,
    hackathon_size <= q_size,
    !is.na(avg_outside_repos_before)
  )

n_final <- nrow(dataset_trimmed_2y)
n_removed <- n_original - n_final

cat("--- Cleaning Summary ---\n")
cat("Final sample size:", n_final, "\n")
cat("Total rows removed:", n_removed, "(approximately", round(n_removed/n_original*100, 2), "% of total sample)\n")

if(!dir.exists("data/new_2y")) dir.create("data/new_2y", recursive = TRUE)

save(dataset_trimmed_2y, file = "data/new_2y/dataset_trimmed_2y.RData")

cat("\nProcessed data saved to: data/new_2y/dataset_trimmed_2y.RData\n")

summary_check <- dataset_trimmed_2y %>%
  select(h_duration, hackathon_size,  avg_outside_repos_before) %>%
  summarise(across(everything(), list(mean = mean, sd = sd, max = max), na.rm = TRUE))

print(summary_check)
