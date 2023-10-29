#=====================================================================================================================
#                     GSSTI - GMET - 
# Digitizing Climatological Paper Archives in Ghana: 
# 
# Project: Digitizing Climatological Paper Archives in Ghana: Consultancy Service to support the Ghana Meteorololigcal Agency in Digitizing Climatological Paper Archives in Ghana
# Code: Profiling entire Rainfall Data of Ghana
# Variable: Minimum Temperature
# Written by: Stephen Aboagye-Ntow, Ghana Space Science and Technology Institute (GSSTI) 
# Edited by:  Stephen Aboagye-Ntow, Ghana Space Science and Technology Institute (GSSTI) 
# Objective: This code profiles minimum temperature data Store of Ghana as a function of their respective districts.
#           it returns the start and end dates, missing  days, days with no recorded min temperature, duplicated stations names, duplicated stations IDs amongst
#           other duplicates.               

 

################################ Visualizations #####################################
# sourcing Functions Script
source("Scripts/FunctionsScript.R")

# Invoking Pipebind Operator
Sys.setenv("_R_USE_PIPEBIND_" = "true")
library(ggplot2)

# Rainfall ####
# Bar Plot for available stations per Year
split(RR, as.factor(RR$Year)) |> 
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
  filename = "For Linux_Unix Systems/outputs/RR Data Availability for Ghana.png",
  width = 1800,
  height = 900
)
dev.off()


# Maximum Temperature ###
# Bar Plot for available stations per Year
split(TX, as.factor(TX$Year)) |> 
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
  filename = "For Linux_Unix Systems/outputs/TMax Data Availability for Ghana.png",
  width = 1800,
  height = 900
)
dev.off()



# Minimum Temperature ###
# Bar Plot for available stations per Year
split(TN, as.factor(TN$Year)) |> 
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
  filename = "For Linux_Unix Systems/outputs/TMinData Availability for Ghana.png",
  width = 1800,
  height = 900
)
dev.off()


##############################################################################################################################





# Spatial Plots of Profiled Stations


  