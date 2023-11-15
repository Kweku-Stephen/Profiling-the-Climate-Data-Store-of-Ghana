#=====================================================================================================================
#                      GSSTI - GMET -  
# Digitizing Climatological Paper Archives in Ghana: 
# 
# Project: Digitizing Climatological Paper Archives in Ghana: Consultancy Service to support the Ghana Meteorololigcal Agency in Digitizing Climatological Paper Archives in Ghana
# Code: Profiling entire Rainfall Data of Ghana
# Variable: Rainfall and Temperature (TMax and TMin)
# Written by: Stephen Aboagye-Ntow, Ghana Space Science and Technology Institute (GSSTI) 
# Edited by:  Stephen Aboagye-Ntow, Ghana Space Science and Technology Institute (GSSTI) 
# Objective: This presents illustrations of data Integrity of each Station. ie data avaiable and data missing


#=======================Data Integrity=====================================

source("For Windows Systems/Scripts/FunctionsScript_W.R")

# Rainfall ####
SOM_RR <- RRdataIntegVis(
	reshapedData = dataReshapedRRDone,
	StationName_ID = "Somanya_07053SOM"
)


# Max Temperature ####
SOM_TX <- TMdataIntegVis(
	reshapedData = dataReshapedTXDone,
	StationName_ID = "Somanya_07053SOM",
	var = "Max Temp"
)


# Min Temperature ####
SOM_TN <- TMdataIntegVis(
	reshapedData = dataReshapedTNDone,
	StationName_ID = "Somanya_07053SOM",
	var = "Min Temp"
)
