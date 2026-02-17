library(tidyverse)
library(patchwork)
library(scales)

load("data/new_6m/dataset_trimmed_6m.RData")
df <- dataset_trimmed_6m

dir.create("results/new_6m/plots", showWarnings = FALSE, recursive = TRUE)

create_line_area_distribution <- function(
    data,
    var_name,
    var_label,
    log_x = FALSE,
    color = "steelblue"
) {
  
  p <- ggplot(data, aes(x = !!sym(var_name))) +
    geom_density(
      aes(y = after_stat(density)),
      color = color,
      fill  = color,
      alpha = 0.35,
      linewidth = 1.3
    ) +
    labs(
      title = var_label,
      x = var_label,
      y = "Density"
    ) +
    theme_minimal(base_size = 13) +
    theme(
      plot.title = element_text(face = "bold", size = 14),
      panel.grid.minor = element_blank()
    )
  
  if (log_x) {
    p <- p + scale_x_log10(labels = comma)
  } else {
    p <- p + scale_x_continuous(labels = comma)
  }
  
  return(p)
}


p_size <- create_line_area_distribution(
  data      = df,
  var_name  = "hackathon_size",
  var_label = "Hackathon Size",
  log_x     = FALSE   
)

p_duration <- create_line_area_distribution(
  data      = df,
  var_name  = "h_duration",
  var_label = "Hackathon Duration (days)"
)

final_plot <- p_size | p_duration +
  plot_annotation(
    title = "Distribution of Hackathon Size and Duration",
    theme = theme(
      plot.title = element_text(
        size = 18,
        face = "bold",
        hjust = 0.5
      )
    )
  )

ggsave(
  "results/new_6m/plots/trimmed_line_area_distribution_hackathon_size_duration.png",
  final_plot,
  width = 16,
  height = 6,
  dpi = 300
)

cat("  results/new_6m/plots/trimmed_line_area_distribution_hackathon_size_duration.png\n")
