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
source("For Linux_Unix Systems/Scripts/PriorityFunctionsScript.R")

# Path to output
path <- "For Linux_Unix Systems/outputs"

# Creating an output directories for each Variable
sapply(
  c("RR", "TMax", "TMin"), 
  \(x) dir.create(file.path(getwd(), path, x, "Priority Districts"))
)

# Import of Priority Districts into a vector
pDistricts <- dir(
  "For Linux_Unix Systems/Data", 
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
startEndYear(list = RR_split)|> . =>
  subset(., grepl(res, .[ ,Dist])) |> 
  # Writing out
  write.csv(
    file = paste0(path, "/RR/Priority Districts/RR_startEndYear.csv"), 
    row.names = FALSE
  )


# Max Temp 
startEndYear(list = TX_split)|> . =>
  subset(., grepl(res, .[ ,Dist])) |> 
  # Writing out
  write.csv(
    file = paste0(path, "/TMax/Priority Districts/TX_startEndYear.csv"), 
    row.names = FALSE
  )


# Min Temp ####
startEndYear(list = TN_split)|> . =>
  subset(., grepl(res, .[ ,Dist])) |> 
  # Writing out
  write.csv(
    file = paste0(path, "/TMin/Priority Districts/Tn_startEndYear.csv"), 
    row.names = FALSE
  )





# # Profile ####
# # Computing data availability for each Station
# Rainfall
Profile_StationsRR |> . => 
  subset(., grepl(res, .[ ,District])) |> 
  # Writing to disk
  write.csv(
    file = paste0(path, "/RR/Priority Districts/Profiled Stations.csv"),
    row.names = FALSE
  )
  


# Max Temp
Profile_StationsTX |> . =>
  subset(., grepl(res, .[ ,District])) |> 
  # Writing to disk
  write.csv(
    file = paste0(path, "/TMax/Priority Districts/Profiled StationsTX.csv"),
    row.names = FALSE
  )


# Min Temp
Profile_StationsTN |> . =>
  subset(., grepl(res, .[ ,District])) |> 
  # Writing to disk
  write.csv(
    file = paste0(path, "/TMin/Priority Districts/Profiled StationsTN.csv"),
    row.names = FALSE
  )




# Missing Stations Days ####

# computing nodes
cores_Prior <- nCores()

# Rainfall
MissingDays_prio(
  vec = regions, 
  list = profMssRegRR, 
  cluster = cores_Prior, 
  rgex = res
) |> . =>
  # Writing out
  rio::export(
    .,
    file = paste0(path, "/RR/Priority Districts/RRMissing.xlsx"), 
    rowNames = FALSE,
    sheetName = names(.)
  )



# Maximum Temperature 
MissingDays_prio(
  vec = regions,
  list = profMssRegTX,
  cluster = cores_Prior,
  rgex = res
) |> . =>
  # Writing out
  rio::export(
    .,
    file = paste0(path, "/TMax/Priority Districts/TXMissing.xlsx"),
    rowNames = FALSE,
    sheetName = names(.)
  )



# Minimum Temperature
MissingDays_prio(
  vec = regions,
  list = profMssRegTN,
  cluster = cores_Prior,
  rgex = res
) |> . =>
  # Writing out
  rio::export(
    ., 
    file = paste0(path, "/TMin/Priority Districts/TNMissing.xlsx"), 
    rowNames = FALSE,
    sheetName = names(.)
  )




# Duplicates ####
# Rainfall
Duplicates(Profile_StationsRR) |> 
  # Extracting Priority Stations
  lapply(\(datatable) subset(datatable, grepl(res, datatable[ ,District]))) |> 
  # Writing to a workbook ####
  rio::export(
    file = paste0(path, "/RR/Priority Districts/Duplicates_RR.xlsx"), 
    rowNames = FALSE,
    sheetNames = c("Duplicated IDs", "oneTownDiffIDs", "oneTowndif/sameid_diffType")
  )


# Max Temp
Duplicates(Profile_StationsTX) |> 
  # Extracting Priority Stations
  lapply(\(datatable) subset(datatable, grepl(res, datatable[ ,District]))) |> 
  # Writing to a workbook ####
  rio::export(
    file = paste0(path, "/TMax/Priority Districts/Duplicates_TX.xlsx"), 
    rowNames = FALSE,
    sheetNames = c("Duplicated IDs", "oneTownDiffIDs", "oneTowndif/sameid_diffType")
  )


# Min Temp
Duplicates(Profile_StationsTN) |> 
  # Extracting Priority Stations
  lapply(\(datatable) subset(datatable, grepl(res, datatable[ ,District]))) |> 
  # Writing to a workbook ####
  rio::export(
    file = paste0(path, "/TMin/Priority Districts/Duplicates_TN.xlsx"), 
    rowNames = FALSE,
    sheetNames = c("Duplicated IDs", "oneTownDiffIDs", "oneTowndif/sameid_diffType")
  )




