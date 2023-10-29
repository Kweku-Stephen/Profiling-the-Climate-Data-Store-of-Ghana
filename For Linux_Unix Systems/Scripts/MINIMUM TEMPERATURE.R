#=====================================================================================================================
#                 GSSTI - GMET - 
# Digitizing Climatological Paper Archives in Ghana: 
# 
# Project: Digitizing Climatological Paper Archives in Ghana: Consultancy Service to support the Ghana Meteorololigcal Agency in Digitizing Climatological Paper Archives in Ghana
# Code: Profiling entire Rainfall Data of Ghana
# Variable: Minimum Temperature
# Written by: Stephen Aboagye-Ntow, Ghana Space Science and Technology Institute (GSSTI) 
# Edited by:  Stephen Aboagye-Ntow, Ghana Space Science and Technology Institute (GSSTI) 
# Objective: This code profiles minimum temperature data Store of Ghana as a function of their respective districts.
#            it returns the start and end dates, missing  days, days with no recorded min temperature, duplicated stations names, duplicated stations IDs amongst
#            other duplicates.               
 


############################################### MINIMUM TEMPERATURE #########################################
# Invoking the pipebind operator ####
Sys.setenv("_R_USE_PIPEBIND_" = "true")

# Output Directory 
dir.create("For Linux_Unix Systems/outputs/TMin")

# sourcing Functions Script
source("For Linux_Unix Systems/Scripts/FunctionsScript.R")

# Computing nodes
ncoresTemp <- parallelly::availableCores() - 25

# Minimum Temperature
# Extracting the Tmin data.table from the list "data" (containing all 3 datatables of rr, tx and tn)
TN <- data[[grep("Tn", names(data), value = TRUE)]]

# Extracting split Tmin data from the "datasplit" list
TN_split <- dataSplit[[grep("Tn", names(dataSplit), value = TRUE)]]

# Start and End Year for each Station
startEndYear(list = TN_split) -> startEndYears_TN

# Writing to Disk
write.csv(
  startEndYears_TN,
  file = paste(path, "TMin/StartEndYears.csv", sep = "/"),
  row.names = FALSE
)

# spliting Stations IDs "Eg Gh Id" into 15 elements, as a function of cluster size
idsTN <- parallel::splitIndices(length(unique(TN[ ,StationName_ID])), ncoresTemp) |> 
  lapply(\(vec) unique(TN[ ,StationName_ID])[vec])

# Data Reshaping for all data.tables of the list "TN-split Split" ####
dataReshapedTN <- parallel::mclapply(
  idsTN,
  lD_list,
  mc.cores = ncoresTemp,
  list = TN_split
) |> . =>
  do.call("c", .)

# Populating missing dates in for each Station
dataReshapedTNDone <- parallel::mclapply(
  idsTN,
  \(vec) {
    dataReshapedTN[vec] |>
      lapply(PopMisnDate, "Year")
  },
  mc.cores = ncoresTemp
)  |> . =>
  do.call("c", .)


# Profiling Minimum Temperature Data ####
# Computing data availability for each Station
Profile_StationsTN <- parallel::mclapply(
  idsTN,
  # Anaymous Function which calls "Profile" for every node
  \(vec, list) {
    list[vec] |> 
      lapply(Profile)
  },
  list = dataReshapedTNDone,
  mc.cores = ncoresTemp
) |> . =>
  do.call("c", .) |> . =>
  do.call("rbind", .)

# Writing to disk
write.csv(
  Profile_StationsTN, 
  file = paste(path, "TMin/Profiled StationsTN.csv",sep = "/"), 
  row.names = FALSE
)


# Missing days for each Station ####
# Extracting missing days for each Station
parallel::mclapply(
  idsTN,
  \(vec, list){
    list[vec] |> 
      lapply(\(data) data[is.na(value), ])
  },
  list = dataReshapedTNDone,
  mc.cores = ncoresTemp
) |> . =>
  do.call("c", .) -> profileMissingTN

profileMissingTN <- (parallel::mclapply(
  idsTN,
  \(vec, list){
    list[vec] |> 
      lapply(missingDays)
  },
  list = profileMissingTN,
  mc.cores = ncores
) |> . =>
  do.call("c", .)) 

# Writing to disk as excel workbooks as a function of Regions and respective Districts ####
regions <- c(
  "Upper West", "Upper East", "Northern Region", "Savannah", "Ahafo", "Bono", "Bono East", 
  "Ashanti", "Eastern", "Central", "North East", "Oti", "Western North", "Volta", "Greater Accra", "Western"
)

profileMissingTN |> 
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
  setNames(regions) -> profMssRegTN

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
      file = file.path(getwd(), "For Linux_Unix Systems/outputs/TMin", paste(vec, "xlsx", sep = ".")), 
      sheetName = names(reg), 
      rowNames = FALSE
    )
    
  },
  list = profMssRegTN,
  mc.cores = 16
)



# Duplicates ####
Duplicates_TN <- Duplicates(data = Profile_StationsTN)

# Writing to a workbook ####
rio::export(
  Duplicates_TN, 
  file = "For Linux_Unix Systems/outputs/TMin/Duplicates_TN.xlsx", 
  sheetNames = c("Duplicated IDs", "oneTownDiffIDs", "oneTownSD-IDsSameType"),
)
