################################ Visualizations #####################################
# Invoking Pipebind Operator
Sys.setenv("_R_USE_PIPEBIND_" = "true")
library(ggplot2)

# Rainfall ####
# Bar Plot for available stations per Year
data[[grep("RR", names(data), value = TRUE)]] |> . =>
  split(., as.factor(.[ ,Year])) |>
  lapply(\(data) length(unique(data[ ,StationName_ID]))) |> . => 
  do.call("rbind", .) |> . => 
  data.frame(
    year = rownames(.),
    NumberofStation = .[ ,1]
  ) |> . =>
  ggplot(data = ., aes(year, NumberofStation)) +
  geom_bar(stat = "identity", fill = "darkblue") +
  labs(title = "", x = "Years", y = "Number of Stations") +
  scale_x_discrete(breaks = seq(1891, 2023, 12)) +
  theme(plot.title = element_text(size = 23, face = "bold", hjust = 0.5),
        axis.text = element_text(size = 18, face = "bold", angle = 90),
        axis.title = element_text(size = 20))

#plotly::ggplotly(plot) -> bars
# Saving to disk
dev.copy(
  png,
  filename = "For Windows Systems/outputs/RR Data Availability for Ghana.png",
  width = 1800,
  height = 900
)
dev.off()


# Maximum Temperature ###
# Bar Plot for available stations per Year
data[[grep("Tx", names(data), value = TRUE)]] |> . =>
  split(., as.factor(.[ ,Year])) |>
  lapply(\(data) length(unique(data[ ,StationName_ID]))) |> . => 
  do.call("rbind", .) |> . => 
  data.frame(
    year = rownames(.),
    NumberofStation = .[ ,1]
  ) |> . =>
  ggplot(data = ., aes(year, NumberofStation)) +
  geom_bar(stat = "identity", fill = "firebrick") +
  labs(title = "", x = "Years", y = "Number of Stations") +
  scale_x_discrete(breaks = seq(1891, 2023, 12)) +
  theme(plot.title = element_text(size = 23, face = "bold", hjust = 0.5),
        axis.text = element_text(size = 18, face = "bold", angle = 90),
        axis.title = element_text(size = 20))

# plotly::ggplotly(plot) -> bars
# Saving to disk
dev.copy(
  png,
  filename = "For Windows Systems/outputs/TMax Data Availability for Ghana.png",
  width = 1800,
  height = 900
)
dev.off()



# Minimum Temperature ###
# Bar Plot for available stations per Year
data[[grep("TN", names(data), value = TRUE)]] |> . =>
  split(., as.factor(.[ ,Year])) |> 
  lapply(\(data) length(unique(data[ ,StationName_ID]))) |> . => 
  do.call("rbind", .) |> . => 
  data.frame(
    year = rownames(.),
    NumberofStation = .[ ,1]
  ) |> . =>
  ggplot(data = ., aes(year, NumberofStation)) +
  geom_bar(stat = "identity", fill = "firebrick") +
  labs(title = "", x = "Years", y = "Number of Stations") +
  scale_x_discrete(breaks = seq(1891, 2023, 12)) +
  theme(plot.title = element_text(size = 23, face = "bold", hjust = 0.5),
        axis.text = element_text(size = 18, face = "bold", angle = 90),
        axis.title = element_text(size = 20))

#plotly::ggplotly(plot) -> bars
# Saving to disk
dev.copy(
  png,
  filename = "For Windows Systems/outputs/TMinData Availability for Ghana.png",
  width = 1800,
  height = 900
)
dev.off()


# Spatial Plots of Profiled Stations


  