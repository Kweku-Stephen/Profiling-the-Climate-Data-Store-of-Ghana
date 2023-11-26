#=====================================================================================================================
#                      GSSTI - GMET - GNAPP 
# Digitizing Climatological Paper Archives in Ghana: 
# 
# Project: Digitizing Climatological Paper Archives in Ghana: Consultancy Service to support the Ghana Meteorololigcal Agency in Digitizing Climatological Paper Archives in Ghana
# Code: Profiling entire Rainfall Data of Ghana
# Variable: Rainfall and Temperature (TMax and TMin)
# Written by: Stephen Aboagye-Ntow, Ghana Space Science and Technology Institute (GSSTI) 
# Edited by:  Stephen Aboagye-Ntow, Ghana Space Science and Technology Institute (GSSTI) 
# Objective: This presents Stations available per year for each variable and 
#             visualizations of data Integrity of each Station. ie data avaiable and data missing



#============================== Visualizations ====================================
# source
source("For Windows Systems/Scripts/FunctionsScript.R")

# Output Directories
sapply(c("RR", "TMax", "TMin"), \(x) dir.create(file.path(path, x, "Plots")))

# Invoking Pipebind Operator
Sys.setenv("_R_USE_PIPEBIND_" = "true")

# Package
library(ggplot2)


#========================== Stations Available Per Year ===================================

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
	filename = file.path(path, "RR", "Plots", "Rainfall Stations per Year.png"),
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
	filename = file.path(path, "TMax", "Plots", "TMax Stations per Year.png"),
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
	filename = file.path(path, "TMin", "Plots", "TMin Stations per Year.png"),
	width = 1800,
	height = 900
)
dev.off()




#========================Data Integrity for each Station =====================================

# Rainfall ####
RRdataIntegVis(
	reshapedData = dataReshapedRRDone,
	StationName_ID = "KIAMO-Accra_23016ACC"
) 
# writing to disk
dev.copy(
	png, 
	file = file.path(path, "RR/Plots", "Rainfall Data Profile for KIAMO.png"),
	height = 650,
	width = 880
)
dev.off()


# Max Temperature ####
TMdataIntegVis(
	reshapedData = dataReshapedTXDone,
	StationName_ID = "KIAMO-Accra_23016ACC",
	var = "Max Temp"
)
# writing to disk
dev.copy(
	png, 
	file = file.path(path, "TMax/Plots", "TMax Data Profile for KIAMO.png"),
	height = 650,
	width = 880
)
dev.off()


# Min Temperature ####
TMdataIntegVis(
	reshapedData = dataReshapedTNDone,
	StationName_ID = "KIAMO-Accra_23016ACC",
	var = "Min Temp"
)
# writing to disk
dev.copy(
	png, 
	file = file.path(path, "TMin/Plots", "TMin Data Profile for KIAMO.png"),
	height = 650,
	width = 880
)
dev.off()





##############################################################################################################################





# Spatial Plots of Profiled Stations


