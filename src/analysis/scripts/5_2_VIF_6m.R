# ============================================
# VIF Analysis Script
# Collinearity diagnostics using fitted interaction models
# ============================================

library(tidyverse)
library(lme4)
library(performance)
library(car)

cat("========================================\n")
cat("VIF Collinearity Diagnostics\n")
cat("========================================\n\n")

load("results/new_6m/interaction_model/models.RData")
load("results/new_6m/interaction_model/prepared_data.RData")

cat("Models and data loaded successfully\n\n")


compute_comprehensive_vif <- function(model, model_name, data) {
  
  cat("\n", paste(rep("=", 60), collapse=""), "\n")
  cat("VIF Analysis:", model_name, "\n")
  cat(paste(rep("=", 60), collapse=""), "\n\n")
  
  cat("Method 1: performance::check_collinearity()\n")
  cat(paste(rep("-", 60), collapse=""), "\n")
  
  vif_performance <- tryCatch({
    performance::check_collinearity(model)
  }, error = function(e) {
    cat("performance VIF calculation failed:", e$message, "\n")
    return(NULL)
  })
  
  if(!is.null(vif_performance)) {
    print(vif_performance)
    cat("\n")
    
    vif_df1 <- as.data.frame(vif_performance) %>%
      rename(variable = Term, vif = VIF) %>%
      mutate(
        method = "performance",
        model = model_name,
        severity = case_when(
          vif > 10 ~ "High (VIF > 10)",
          vif > 5 ~ "Moderate (5 < VIF ≤ 10)",
          TRUE ~ "Low (VIF ≤ 5)"
        )
      )
    
    if("VIF_CI_low" %in% names(vif_df1)) {
      vif_df1 <- vif_df1 %>%
        rename(vif_ci_low = VIF_CI_low, vif_ci_high = VIF_CI_high)
    }
    
    high_vif <- vif_df1 %>% filter(vif > 5)
    if(nrow(high_vif) > 0) {
      cat("High VIF variables detected:\n")
      print(high_vif %>% select(variable, vif, severity), row.names = FALSE)
      cat("\n")
    } else {
      cat("All variables have VIF < 5\n\n")
    }
  } else {
    vif_df1 <- NULL
  }
  
  cat("\nMethod 2: car::vif() - Generalized VIF (GVIF)\n")
  cat(paste(rep("-", 60), collapse=""), "\n")
  cat("Note: GVIF is more appropriate for models with interaction terms\n\n")
  
  vif_car <- tryCatch({
    fixed_formula <- update(formula(model), . ~ . - (1|hackathon_id))
    glm_model <- glm(fixed_formula, data = data, family = binomial(link = "logit"))
    car::vif(glm_model)
  }, error = function(e) {
    cat("car GVIF calculation failed:", e$message, "\n")
    return(NULL)
  })
  
  if(!is.null(vif_car)) {
    if(is.matrix(vif_car)) {
      cat("GVIF Results:\n")
      print(vif_car)
      cat("\n")
      
      vif_df2 <- data.frame(
        variable = rownames(vif_car),
        gvif = vif_car[, "GVIF"],
        df = vif_car[, "Df"],
        gvif_adjusted = vif_car[, "GVIF^(1/(2*Df))"],
        method = "car_GVIF",
        model = model_name
      ) %>%
        mutate(
          severity = case_when(
            gvif_adjusted > 10 ~ "High (GVIF^(1/2Df) > 10)",
            gvif_adjusted > 5 ~ "Moderate (5 < GVIF^(1/2Df) ≤ 10)",
            TRUE ~ "Low (GVIF^(1/2Df) ≤ 5)"
          )
        )
      
      high_gvif <- vif_df2 %>% filter(gvif_adjusted > 5)
      if(nrow(high_gvif) > 0) {
        cat("High GVIF variables detected:\n")
        print(high_gvif %>% select(variable, gvif_adjusted, severity), row.names = FALSE)
        cat("\n")
      } else {
        cat("All variables have GVIF^(1/2Df) < 5\n\n")
      }
      
    } else {
      cat("VIF Results:\n")
      print(vif_car)
      cat("\n")
      
      vif_df2 <- data.frame(
        variable = names(vif_car),
        vif = as.numeric(vif_car),
        method = "car_VIF",
        model = model_name
      ) %>%
        mutate(
          severity = case_when(
            vif > 10 ~ "High (VIF > 10)",
            vif > 5 ~ "Moderate (5 < VIF ≤ 10)",
            TRUE ~ "Low (VIF ≤ 5)"
          )
        )
    }
  } else {
    vif_df2 <- NULL
  }
  
  cat("\nMethod 3: Correlation Matrix of Predictors\n")
  cat(paste(rep("-", 60), collapse=""), "\n")
  
  predictor_vars <- c(
    "precolab",
    "h_duration_log_std", 
    "hackathon_size_std", 
    "is_offline_event",
    "avg_outside_repos_within_std", 
    "avg_outside_repos_hackathon_mean_std",
    "team_contributor_size_std", 
    "common_event_num_std"
  )
  
  available_vars <- predictor_vars[predictor_vars %in% names(data)]
  
  if(length(available_vars) > 1) {
    cor_matrix <- cor(data[, available_vars], use = "complete.obs")
    
    cat("Correlation matrix:\n")
    print(round(cor_matrix, 3))
    cat("\n")
    
    high_cor <- which(abs(cor_matrix) > 0.7 & upper.tri(cor_matrix), arr.ind = TRUE)
    
    if(nrow(high_cor) > 0) {
      cat("High correlation pairs (|r| > 0.7):\n")
      high_cor_df <- data.frame(
        var1 = rownames(cor_matrix)[high_cor[,1]],
        var2 = colnames(cor_matrix)[high_cor[,2]],
        correlation = cor_matrix[high_cor],
        model = model_name
      ) %>%
        arrange(desc(abs(correlation)))
      
      print(high_cor_df, row.names = FALSE)
      cat("\n")
    } else {
      cat("No high correlation pairs (|r| > 0.7)\n")
      high_cor_df <- data.frame()
    }
    
    moderate_cor <- which(abs(cor_matrix) > 0.5 & abs(cor_matrix) <= 0.7 & upper.tri(cor_matrix), arr.ind = TRUE)
    
    if(nrow(moderate_cor) > 0) {
      cat("\nModerate correlation pairs (0.5 < |r| ≤ 0.7):\n")
      moderate_cor_df <- data.frame(
        var1 = rownames(cor_matrix)[moderate_cor[,1]],
        var2 = colnames(cor_matrix)[moderate_cor[,2]],
        correlation = cor_matrix[moderate_cor],
        model = model_name
      ) %>%
        arrange(desc(abs(correlation)))
      
      print(moderate_cor_df, row.names = FALSE)
      cat("\n")
    } else {
      moderate_cor_df <- data.frame()
    }
    
  } else {
    cor_matrix <- NULL
    high_cor_df <- data.frame()
    moderate_cor_df <- data.frame()
  }
  
  cat("\n", paste(rep("=", 60), collapse=""), "\n")
  cat("VIF Diagnostic Summary\n")
  cat(paste(rep("=", 60), collapse=""), "\n\n")
  
  if(!is.null(vif_df1)) {
    max_vif <- max(vif_df1$vif, na.rm = TRUE)
    cat(sprintf("Maximum VIF: %.2f\n", max_vif))
    
    if(max_vif > 10) {
      cat("Severe multicollinearity detected (VIF > 10)\n")
    } else if(max_vif > 5) {
      cat("Moderate multicollinearity detected (5 < VIF ≤ 10)\n")
    } else {
      cat("Multicollinearity within acceptable range (VIF ≤ 5)\n")
    }
  }
  
  if(!is.null(vif_df2) && "gvif_adjusted" %in% names(vif_df2)) {
    max_gvif <- max(vif_df2$gvif_adjusted, na.rm = TRUE)
    cat(sprintf("\nMaximum GVIF^(1/2Df): %.2f\n", max_gvif))
    
    if(max_gvif > 10) {
      cat("Severe multicollinearity detected (GVIF^(1/2Df) > 10)\n")
    } else if(max_gvif > 5) {
      cat("Moderate multicollinearity detected (5 < GVIF^(1/2Df) ≤ 10)\n")
    } else {
      cat("Multicollinearity within acceptable range (GVIF^(1/2Df) ≤ 5)\n")
    }
  }
  
  cat("\n")
  
  return(list(
    vif_performance = vif_df1,
    vif_car = vif_df2,
    high_correlations = if(exists("high_cor_df")) high_cor_df else data.frame(),
    moderate_correlations = if(exists("moderate_cor_df")) moderate_cor_df else data.frame(),
    correlation_matrix = cor_matrix
  ))
}
