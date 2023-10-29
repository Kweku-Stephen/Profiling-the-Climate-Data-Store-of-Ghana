#=====================================================================================================================
#                    GSSTI - GMET - 
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



################################################################################

# Enabling the pipebind operator
Sys.setenv("_R_USE_PIPEBIND_" = "true")

# Sourcing Functions Script ####
source("For Windows Systems/Scripts/PriorityFunctionsScript.R")

# Creating an output directories for each Variable
sapply(
	c("RR", "TMax", "TMin"), 
	\(x) dir.create(file.path(getwd(), path, x, "Priority Districts"))
)

# Import of Priority Districts into a vector
pDistricts <- dir(
	"For Windows Systems/Data", 
	pattern = "priority", 
	full.names = TRUE
) |> 
	readLines() 

# creating a regex for extracting Priority Districts
ind <- 1 
res <- pDistricts[1]

for(i in 1:(length(pDistricts) - 1)) {
	res <- paste(res[1], pDistricts[ind], sep = "|")
	
	ind <- ind + 1
}


# Start and End Year ####
# Rainfall
rr_pr <- startEndYear(list = RR_split)|> . =>
	subset(., grepl(res, .[ ,Dist]))
# Writing out
write.csv(
	rr_pr, 
	file = paste0(path, "/RR/Priority Districts/RR_startEndYear.csv"), 
	row.names = FALSE
)

# Max Temp 
tx_pr <- startEndYear(list = TX_split)|> . =>
	subset(., grepl(res, .[ ,Dist]))
# Writing out
write.csv(
	tx_pr, 
	file = paste0(path, "/TMax/Priority Districts/TX_startEndYear.csv"), 
	row.names = FALSE
)

# Min Temp ####
tn_pr <- startEndYear(list = TN_split)|> . =>
	subset(., grepl(res, .[ ,Dist]))
# Writing out
write.csv(
	tn_pr, 
	file = paste0(path, "/TMin/Priority Districts/Tn_startEndYear.csv"), 
	row.names = FALSE
)




# # Profile ####
# # Computing data availability for each Station
# Rainfall
Profile_StationsRR_Prior <- Profile_StationsRR |> . => 
	subset(., grepl(res, .[ ,District]))
# Writing to disk
write.csv(
	Profile_StationsRR_Prior,
	file = paste0(path, "/RR/Priority Districts/Profiled Stations.csv"),
	row.names = FALSE
)

# Max Temp
Profile_StationsTX_Prior <- Profile_StationsTX |> . =>
	subset(., grepl(res, .[ ,District]))
# Writing to disk
write.csv(
	Profile_StationsTX_Prior,
	file = paste0(path, "/TMax/Priority Districts/Profiled StationsTX.csv"),
	row.names = FALSE
)

# Min Temp
Profile_StationsTN_Prior <- Profile_StationsTN |> . =>
	subset(., grepl(res, .[ ,District]))
# Writing to disk
write.csv(
	Profile_StationsTN_Prior,
	file = paste0(path, "/TMin/Priority Districts/Profiled StationsTN.csv"),
	row.names = FALSE
)



# Missing Stations Days ####
# Rainfall
MissingDays_prio(
	vec = regions, 
	list = profMssRegRR, 
	cores = , 
	rgex = res
)-> Priority_Rainfall

# Writing out
rio::export(
	Priority_Rainfall, 
	file = paste0(path, "/RR/Priority Districts/RRMissing.xlsx"), 
	rowNames = FALSE,
	sheetName = names(Priority_Rainfall)
)


# Maximum Temperature 
MissingDays_prio(
	vec = regions,
	list = profMssRegTX,
	cores = ,
	rgex = res
) -> Priority_TMax

# Writing out
rio::export(
	Priority_TMax, 
	file = paste0(path, "/TMax/Priority Districts/TXMissing.xlsx"), 
	rowNames = FALSE,
	sheetName = names(Priority_TMax)
)


# Minimum Temperature
MissingDays_prio(
	vec = regions,
	list = profMssRegTN,
	cores = ,
	rgex = res
) -> Priority_TMin

# Writing out
rio::export(
	Priority_TMin, 
	file = paste0(path, "/TMin/Priority Districts/TNMissing.xlsx"), 
	rowNames = FALSE,
	sheetName = names(Priority_TMin)
)




# Duplicates ####
# Rainfall
Duplicates_RR_prio <- Duplicates(Profile_StationsRR) |> 
	# Extracting Priority Stations
	lapply(\(datatable) subset(datatable, grepl(res, datatable[ ,District])))

# Writing to a workbook ####
rio::export(
	Duplicates_RR_prio, 
	file = paste0(path, "/RR/Priority Districts/Duplicates_RR.xlsx"), 
	rowNames = FALSE,
	sheetNames = c("Duplicated IDs", "oneTownDiffIDs", "oneTowndif/sameid_diffType")
)


# Max Temp
Duplicates_TX_prio <- Duplicates(Profile_StationsTX) |> 
	# Extracting Priority Stations
	lapply(\(datatable) subset(datatable, grepl(res, datatable[ ,District])))

# Writing to a workbook ####
rio::export(
	Duplicates_TX_prio, 
	file = paste0(path, "/TMax/Priority Districts/Duplicates_TX.xlsx"), 
	rowNames = FALSE,
	sheetNames = c("Duplicated IDs", "oneTownDiffIDs", "oneTowndif/sameid_diffType")
)


# Min Temp
Duplicates_TN_prio <- Duplicates(Profile_StationsTN) |> 
	# Extracting Priority Stations
	lapply(\(datatable) subset(datatable, grepl(res, datatable[ ,District])))

# Writing to a workbook ####
rio::export(
	Duplicates_TN_prio, 
	file = paste0(path, "/TMin/Priority Districts/Duplicates_TN.xlsx"), 
	rowNames = FALSE,
	sheetNames = c("Duplicated IDs", "oneTownDiffIDs", "oneTowndif/sameid_diffType")
)




