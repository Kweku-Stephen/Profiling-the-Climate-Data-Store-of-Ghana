#=====================================================================================================================
#                   GSSTI - GMET - 
# Digitizing Climatological Paper Archives in Ghana: 
# 
# Project: Digitizing Climatological Paper Archives in Ghana: Consultancy Service to support the Ghana Meteorololigcal Agency in Digitizing Climatological Paper Archives in Ghana
# Code: Profiling entire Rainfall Data of Ghana
# Variable: Maximum Temperature
# Written by: Stephen Aboagye-Ntow, Ghana Space Science and Technology Institute (GSSTI) 
# Edited by:  Stephen Aboagye-Ntow, Ghana Space Science and Technology Institute (GSSTI) 
# Objective: This code profiles maximum temperature data Store of Ghana as a function of their respective districts.
#           it returns the start and end dates, missing  days, days with no recorded max temperature, duplicated stations names, duplicated stations IDs amongst
#           other duplicates.               

 

########################################## MAXIMUM TEMPERATURE ########################################
# Invoking the pipebind operator ####
Sys.setenv("_R_USE_PIPEBIND_" = "true")

# sourcing Functions Script
source("For Linux_Unix Systems/Scripts/FunctionsScript.R")

# Output Directory 
dir.create("For Linux_Unix Systems/outputs/TMax")

# Computing nodes
ncoresTemp <- parallelly::availableCores() - 25

# Maximum Temperature ####
# Extracting the Tmax data.table from the list data(containing all 3 datatables of rr, tx and tn)
TX <- data[[grep("Tx", names(data), value = TRUE)]]

# Extracting split Tmax data from the datasplit list
TX_split <- dataSplit[[grep("Tx", names(dataSplit), value = TRUE)]]

# Start and End Year for Each Station
startEndYear(list = TX_split) -> startEndYears_TX

# Writing to Disk
write.csv(
  startEndYears_TX, 
  file = paste(path, "TMax/StartandYears of .csv",sep = "/"), 
  row.names = FALSE
)

# spliting Tmax Stations IDs "Eg Gh Id" into 15 elements, as a function of cluster size
idsTX <- parallel::splitIndices(length(unique(TX[ ,StationName_ID])), ncoresTemp) |> 
  lapply(\(vec) unique(TX[ ,StationName_ID])[vec])

# Data Reshaping for all dataTables of the list "TX_Split" from "dataSplit" ####
dataReshapedTX <- parallel::mclapply(
  idsTX,
  lD_list,
  mc.cores = ncoresTemp,
  list = TX_split
) |> . =>
  do.call("c", .)

# Populating missing dates for each Station
dataReshapedTXDone <- parallel::mclapply(
  idsTX,
  \(vec) {
    dataReshapedTX[vec] |>
      lapply(PopMisnDate, "Year")
  },
  mc.cores = ncoresTemp
)  |> . =>
  do.call("c", .)

# # Data Reshaping for all dataTables of the list "TX_Split" from "dataSplit" ####
# dataReshapedTX <- parallel::mclapply(
#   idsTX, 
#   lD_list, 
#   mc.cores = ncoresTemp,
#   list = TX_split
# ) |> . =>
#   do.call("c", .) |> . =>
#   # Populating missing dates for each Station
#   parallel::mclapply(
#     idsTX,
#     \(vec) {
#       .[vec] |> 
#         lapply(PopMisnDate, "Year")
#     },
#     mc.cores = ncoresTemp
#   )  |> . =>
#   do.call("c", .)



# Profiling Maximum Temperature Data ####
# Computing data availability for each Station
Profile_StationsTX <- parallel::mclapply(
  idsTX,
  # Anaymous Function which calls "Profile" for every node
  \(vec, list) {
    list[vec] |> 
      lapply(
        Profile
      )
  },
  list = dataReshapedTXDone,
  mc.cores = ncoresTemp
) |> . =>
  do.call("c", .) |> . =>
  do.call("rbind", .)

# Writing to disk
write.csv(
  Profile_StationsTX, 
  file = paste(path, "TMax/Profiled StationsTX.csv", sep = "/"), 
  row.names = FALSE
)



# Missing days for each Station ####
# Extracting missing days for each Station
parallel::mclapply(
  idsTX,
  \(vec, list){
    list[vec] |> 
      lapply(\(data) data[is.na(value), ])
  },
  list = dataReshapedTXDone,
  mc.cores = ncoresTemp
) |> . =>
  do.call("c", .) -> profileMissingTX

profileMissingTX <- (parallel::mclapply(
  idsTX,
  \(vec, list){
    list[vec] |> 
      lapply(missingDays)
  },
  list = profileMissingTX,
  mc.cores = ncores
) |> . =>
  do.call("c", .)) 

# Writing to disk as excel workbooks as a function of Regions and respective Districts ####
regions <- c(
  "Upper West", "Upper East", "Northern Region", "Savannah", "Ahafo", "Bono", "Bono East", 
  "Ashanti", "Eastern", "Central", "North East", "Oti", "Western North", "Volta", "Greater Accra", "Western"
)

profileMissingTX |> 
  lapply(as.data.frame) |> . => 
  do.call("rbind", .) |> . =>
  split(., as.factor(.[ ,"Reg"])) |> . =>
  lapply(
    c("[Uu]pper [Ww]est", "[Uu]pper [Ee]ast", "[Nn]orthern", "[Ss]avannah", "[Aa]hafo", 
      "[Bb]ono", "[Bb]ono [Ee]ast", "[Aa]shanti","[Ee]astern", "[Cc]entral", 
      "[Nn]orth [Ee]ast", "[Oo]ti", "[Ww]estern [Nn]orth", "[Vv]olta", "[Gg]reater [Aa]ccra", "[Ww]estern"),
    \(vec, list){
      list[grep(vec, names(list), value = TRUE)] |> . =>
        do.call("rbind", .)
    },
    list = .
  ) |> 
  setNames(regions) -> profMssRegTX

# Saving to workbooks
parallel::mclapply(
  regions,
  \(vec, list) {
    
    # reg <- list[[vec]] |> . =>
    #   split(., as.factor(.[ ,"Distr"]))
    
    reg <- list[[vec]] |> . =>
      split(., as.factor(.[ ,"Distr"])) |> 
      lapply(
        \(data) {
          dt <- within(
            data, 
            {
              StationName = rownames(data) |> 
                strsplit("\\.") |> 
                sapply(\(vec) vec[2]) #|> . =>
                #do.call("c", .) 
            }
          )
          return(dt)
        }
      )
    
    rio::export(
      reg, 
      file = file.path(getwd(), "For Linux_Unix Systems/outputs/TMax", paste(vec, "xlsx", sep = ".")), 
      sheetName = names(reg), 
      rowNames = FALSE
    )
    
  },
  list = profMssRegTX,
  mc.cores = 16
)



# Duplicates ####
Duplicates_TX <- Duplicates(data = Profile_StationsTX)

# Same Observation Duplicates
sameObservationTX <- subset(
  data$`Daily-Tx-All-Stations Sheet 1.txt`,
  duplicated(
    with(
      data$`Daily-Tx-All-Stations Sheet 1.txt`,
      paste(Name, `Eg Gh Id`, `Station Type`, Year, Month, sep = "-")
    )
  )
)

# Writing to a workbook ####
rio::export(
  Duplicates_TX, 
  file = paste(path, "TMax/Duplicates_TX.xlsx",sep = "/"), 
  sheetNames = c("Duplicated IDs", "oneTownDiffIDs", "oneTownSD-IDsSameType"),
)
